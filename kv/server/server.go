package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your Code Here (4B).
	var keys [][]byte
	keys = append(keys, req.Key)
	server.Latches.WaitForLatches(keys)
	defer server.Latches.ReleaseLatches(keys)

	resp := &kvrpcpb.GetResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return resp, err
	}
	txn := mvcc.NewMvccTxn(reader, req.Version)
	lock, err := txn.GetLock(req.Key)
	if err != nil {
		return resp, err
	}

	if lock != nil && lock.Ts < txn.StartTS {
		resp.Error = &kvrpcpb.KeyError{
			Locked: &kvrpcpb.LockInfo{
				PrimaryLock: lock.Primary,
				LockVersion: lock.Ts,
				Key:         req.Key,
				LockTtl:     lock.Ttl,
			},
		}
	}

	val, err := txn.GetValue(req.Key)
	if err != nil {
		return resp, err
	}
	if val == nil {
		resp.NotFound = true
		return resp, err
	}
	resp.Value = val

	return resp, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	var keys [][]byte
	mutations := req.Mutations
	for _, mutation := range mutations {
		keys = append(keys, mutation.Key)
	}
	server.Latches.WaitForLatches(keys)
	defer server.Latches.ReleaseLatches(keys)

	resp := &kvrpcpb.PrewriteResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return resp, err
	}
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)

	for _, key := range keys {
		write, commitTs, err := txn.MostRecentWrite(key) //最近写入的write
		if err != nil {
			return resp, err
		}

		if write != nil && commitTs > txn.StartTS {
			keyerror := &kvrpcpb.KeyError{
				Conflict: &kvrpcpb.WriteConflict{
					StartTs:    write.StartTS,
					ConflictTs: commitTs,
					Key:        key,
					Primary:    req.PrimaryLock,
				},
			}
			resp.Errors = append(resp.Errors, keyerror)
			return resp, nil
		}
	}

	for _, key := range keys {
		lock, err := txn.GetLock(key)
		if err != nil {
			return resp, err
		}
		if lock != nil {
			keyError := &kvrpcpb.KeyError{
				Locked: &kvrpcpb.LockInfo{
					PrimaryLock: lock.Primary,
					LockVersion: lock.Ts,
					Key:         key,
					LockTtl:     lock.Ttl,
				},
			}
			resp.Errors = append(resp.Errors, keyError)
			return resp, nil
		}
	}

	for _, mutation := range mutations {
		var kind mvcc.WriteKind
		switch mutation.Op {
		case kvrpcpb.Op_Put:
			txn.PutValue(mutation.Key, mutation.Value)
			kind = mvcc.WriteKindPut
		case kvrpcpb.Op_Del:
			txn.DeleteValue(mutation.Key)
			kind = mvcc.WriteKindDelete
		case kvrpcpb.Op_Rollback:
			kind = mvcc.WriteKindRollback
		case kvrpcpb.Op_Lock:
		}
		txn.PutLock(mutation.Key, &mvcc.Lock{
			Primary: req.PrimaryLock,
			Ts:      txn.StartTS,
			Ttl:     req.LockTtl,
			Kind:    kind,
		})
	}

	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		return resp, err
	}
	return resp, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	keys := req.Keys
	server.Latches.WaitForLatches(keys)
	defer server.Latches.ReleaseLatches(keys)

	resp := &kvrpcpb.CommitResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return resp, err
	}
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)

	for _, key := range keys {
		lock, err := txn.GetLock(key)
		if err != nil {
			return resp, err
		}
		if lock != nil && lock.Ts != txn.StartTS {
			resp.Error = &kvrpcpb.KeyError{
				Retryable: "true",
			}
			return resp, nil
		} else if lock == nil {
			write, _, err := txn.CurrentWrite(key)
			if err != nil {
				return resp, nil
			}
			if write == nil {
				continue
			}
			if write.StartTS == txn.StartTS && write.Kind == mvcc.WriteKindRollback {
				resp.Error = &kvrpcpb.KeyError{
					Retryable: "true",
				}
				return resp, nil
			}
		}
	}

	for _, key := range keys {
		lock, _ := txn.GetLock(key)
		if lock == nil {
			continue
		}
		txn.PutWrite(key, req.CommitVersion, &mvcc.Write{
			StartTS: txn.StartTS,
			Kind:    lock.Kind,
		})
		txn.DeleteLock(key)
	}

	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		return resp, err
	}
	return resp, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.ScanResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return resp, err
	}

	txn := mvcc.NewMvccTxn(reader, req.Version)
	scanner := mvcc.NewScanner(req.StartKey, txn)
	pairs := make([]*kvrpcpb.KvPair, 0)

	for i := 0; i < int(req.Limit); i++ {
		if !scanner.Iter.Valid() {
			break
		}

		key, val, err := scanner.Next()
		if err != nil {
			return resp, err
		}
		if key == nil {
			continue
		}
		lock, err := txn.GetLock(key)
		if err != nil {
			return resp, err
		}

		if lock != nil && lock.Ts < txn.StartTS {
			pair := &kvrpcpb.KvPair{
				Error: &kvrpcpb.KeyError{
					Locked: &kvrpcpb.LockInfo{
						PrimaryLock: lock.Primary,
						LockVersion: lock.Ts,
						Key:         key,
						LockTtl:     lock.Ttl,
					},
				},
			}
			pairs = append(pairs, pair)
			continue
		}
		if val != nil {
			pair := &kvrpcpb.KvPair{
				Key:   key,
				Value: val,
			}
			pairs = append(pairs, pair)
		}
	}
	resp.Pairs = pairs
	return resp, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.CheckTxnStatusResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return resp, nil
	}

	txn := mvcc.NewMvccTxn(reader, req.LockTs)
	write, commitTs, err := txn.CurrentWrite(req.PrimaryKey)
	if err != nil {
		return resp, err
	}
	lock, err := txn.GetLock(req.PrimaryKey)
	if err != nil {
		return resp, nil
	}
	if write != nil && write.Kind != mvcc.WriteKindRollback {
		resp.CommitVersion = commitTs
		return resp, nil
	}

	if lock == nil {
		if write != nil && write.Kind == mvcc.WriteKindRollback {
			resp.CommitVersion = 0
			resp.LockTtl = 0
			resp.Action = kvrpcpb.Action_NoAction
			return resp, nil
		} else {
			txn.PutWrite(req.PrimaryKey, req.LockTs, &mvcc.Write{
				StartTS: req.LockTs,
				Kind:    mvcc.WriteKindRollback,
			})
			err := server.storage.Write(req.Context, txn.Writes())
			if err != nil {
				return resp, err
			}
			resp.Action = kvrpcpb.Action_LockNotExistRollback
			return resp, nil
		}
	}

	curTs := mvcc.PhysicalTime(req.CurrentTs)
	lockTs := mvcc.PhysicalTime(lock.Ts)
	if curTs > lockTs && curTs-lockTs >= lock.Ttl {
		txn.DeleteLock(req.PrimaryKey)
		txn.DeleteValue(req.PrimaryKey)
		txn.PutWrite(req.PrimaryKey, req.LockTs, &mvcc.Write{
			StartTS: req.LockTs,
			Kind:    mvcc.WriteKindRollback,
		})

		err := server.storage.Write(req.Context, txn.Writes())
		if err != nil {
			return resp, err
		}
		resp.Action = kvrpcpb.Action_TTLExpireRollback
	} else {
		resp.Action = kvrpcpb.Action_NoAction
		return resp, nil
	}
	return resp, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.BatchRollbackResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return resp, err
	}
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	keys := req.Keys

	for _, key := range keys {
		write, _, err := txn.CurrentWrite(key)
		if err != nil {
			return resp, err
		}
		if write != nil && write.Kind != mvcc.WriteKindRollback {
			resp.Error = &kvrpcpb.KeyError{
				Abort: "true",
			}
			return resp, nil
		}
	}

	for _, key := range keys {
		write, _, err := txn.CurrentWrite(key)
		if err != nil {
			return resp, err
		}
		lock, err := txn.GetLock(key)
		if err != nil {
			return resp, err
		}

		if lock != nil && lock.Ts != txn.StartTS {
			txn.PutWrite(key, txn.StartTS, &mvcc.Write{
				StartTS: txn.StartTS,
				Kind:    mvcc.WriteKindRollback,
			})
			continue
		}
		if write != nil && write.Kind == mvcc.WriteKindRollback {
			continue
		}

		txn.DeleteLock(key)
		txn.DeleteValue(key)
		txn.PutWrite(key, txn.StartTS, &mvcc.Write{
			StartTS: txn.StartTS,
			Kind:    mvcc.WriteKindRollback,
		})
	}

	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		return resp, err
	}
	return resp, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.ResolveLockResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return resp, nil
	}
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	iter := reader.IterCF(engine_util.CfLock)
	var keys [][]byte

	for ; iter.Valid(); iter.Next() {
		item := iter.Item()
		key := item.KeyCopy(nil)
		val, err := item.ValueCopy(nil)
		if err != nil {
			return resp, err
		}

		lock, err := mvcc.ParseLock(val)
		if err != nil {
			return resp, err
		}
		if lock.Ts == txn.StartTS {
			keys = append(keys, key)
		}
	}

	if req.CommitVersion == 0 {
		rbReq := &kvrpcpb.BatchRollbackRequest{
			Keys:         keys,
			StartVersion: txn.StartTS,
			Context:      req.Context,
		}
		rbkResp, err := server.KvBatchRollback(nil, rbReq)
		if err != nil {
			return resp, err
		}
		resp.Error = rbkResp.Error
		resp.RegionError = rbkResp.RegionError
		return resp, nil
	} else if req.CommitVersion > 0 {
		cmtReq := &kvrpcpb.CommitRequest{
			Keys:          keys,
			StartVersion:  txn.StartTS,
			CommitVersion: req.CommitVersion,
			Context:       req.Context,
		}
		cmResp, err := server.KvCommit(nil, cmtReq)
		if err != nil {
			return resp, err
		}
		resp.Error = cmResp.Error
		resp.RegionError = cmResp.RegionError
		return resp, nil
	}

	return resp, nil
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}
