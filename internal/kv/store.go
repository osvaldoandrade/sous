package kv

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/osvaldoandrade/sous/internal/api"
	cserrors "github.com/osvaldoandrade/sous/internal/errors"
)

var terminalStatuses = map[string]struct{}{
	"success": {},
	"error":   {},
	"timeout": {},
}

type Store struct {
	client        *redis.Client
	casActivation *redis.Script
}

func NewStore(addr, password string) *Store {
	client := redis.NewClient(&redis.Options{
		Addr:         addr,
		Password:     password,
		DialTimeout:  3 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
		PoolSize:     64,
		MinIdleConns: 8,
	})
	casScript := redis.NewScript(`
local curr = redis.call('GET', KEYS[1])
if curr ~= ARGV[1] then
  return 0
end
redis.call('SET', KEYS[1], ARGV[2], 'EX', ARGV[4])
redis.call('SET', KEYS[2], ARGV[3], 'EX', ARGV[4])
return 1
`)
	return &Store{client: client, casActivation: casScript}
}

func (s *Store) Close() error {
	return s.client.Close()
}

func (s *Store) Ping(ctx context.Context) error {
	if err := s.client.Ping(ctx).Err(); err != nil {
		return cserrors.Wrap(cserrors.CSKVUnavailable, "kvrocks ping failed", err)
	}
	return nil
}

func (s *Store) CreateFunction(ctx context.Context, rec api.FunctionRecord) error {
	key := FunctionMetaKey(rec.Tenant, rec.Namespace, rec.Name)
	raw, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	ok, err := s.client.SetNX(ctx, key, raw, 0).Result()
	if err != nil {
		return cserrors.Wrap(cserrors.CSKVWriteFailed, "failed to create function record", err)
	}
	if !ok {
		return cserrors.New(cserrors.CSValidationFailed, "function already exists")
	}
	return nil
}

func (s *Store) GetFunction(ctx context.Context, tenant, namespace, function string) (api.FunctionRecord, error) {
	var rec api.FunctionRecord
	raw, err := s.client.Get(ctx, FunctionMetaKey(tenant, namespace, function)).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return rec, cserrors.New(cserrors.CSValidationFailed, "function not found")
		}
		return rec, cserrors.Wrap(cserrors.CSKVReadFailed, "failed to read function record", err)
	}
	if err := json.Unmarshal(raw, &rec); err != nil {
		return rec, cserrors.Wrap(cserrors.CSKVReadFailed, "failed to decode function record", err)
	}
	return rec, nil
}

func (s *Store) SoftDeleteFunction(ctx context.Context, tenant, namespace, function string, deletedAtMS int64) error {
	rec, err := s.GetFunction(ctx, tenant, namespace, function)
	if err != nil {
		return err
	}
	rec.DeletedAtMS = &deletedAtMS
	raw, _ := json.Marshal(rec)
	if err := s.client.Set(ctx, FunctionMetaKey(tenant, namespace, function), raw, 0).Err(); err != nil {
		return cserrors.Wrap(cserrors.CSKVWriteFailed, "failed to soft-delete function", err)
	}
	return nil
}

func (s *Store) PutDraft(ctx context.Context, tenant, namespace, function string, draft api.DraftRecord, ttl time.Duration) error {
	raw, err := json.Marshal(draft)
	if err != nil {
		return err
	}
	if err := s.client.Set(ctx, DraftKey(tenant, namespace, function, draft.DraftID), raw, ttl).Err(); err != nil {
		return cserrors.Wrap(cserrors.CSKVWriteFailed, "failed to write draft", err)
	}
	return nil
}

func (s *Store) GetDraft(ctx context.Context, tenant, namespace, function, draftID string) (api.DraftRecord, error) {
	var out api.DraftRecord
	raw, err := s.client.Get(ctx, DraftKey(tenant, namespace, function, draftID)).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return out, cserrors.New(cserrors.CSValidationFailed, "draft not found or expired")
		}
		return out, cserrors.Wrap(cserrors.CSKVReadFailed, "failed to read draft", err)
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return out, cserrors.Wrap(cserrors.CSKVReadFailed, "failed to decode draft", err)
	}
	return out, nil
}

func (s *Store) MarkDraftConsumed(ctx context.Context, tenant, namespace, function, draftID string, ttl time.Duration) error {
	draft, err := s.GetDraft(ctx, tenant, namespace, function, draftID)
	if err != nil {
		return err
	}
	draft.Consumed = true
	raw, _ := json.Marshal(draft)
	if err := s.client.Set(ctx, DraftKey(tenant, namespace, function, draftID), raw, ttl).Err(); err != nil {
		return cserrors.Wrap(cserrors.CSKVWriteFailed, "failed to mark draft consumed", err)
	}
	return nil
}

func (s *Store) PublishVersion(ctx context.Context, tenant, namespace, function string, versionMeta api.VersionRecord, bundle []byte, alias string) (int64, error) {
	version, err := s.client.Incr(ctx, VersionSeqKey(tenant, namespace, function)).Result()
	if err != nil {
		return 0, cserrors.Wrap(cserrors.CSKVWriteFailed, "failed to allocate version", err)
	}
	versionMeta.Version = version
	rawMeta, err := json.Marshal(versionMeta)
	if err != nil {
		return 0, err
	}

	pipe := s.client.TxPipeline()
	pipe.Set(ctx, VersionMetaKey(tenant, namespace, function, version), rawMeta, 0)
	pipe.Set(ctx, VersionBundleKey(tenant, namespace, function, version), bundle, 0)
	if alias != "" {
		aliasRec := api.AliasRecord{Alias: alias, Version: version, UpdatedAtMS: time.Now().UnixMilli()}
		rawAlias, _ := json.Marshal(aliasRec)
		pipe.Set(ctx, AliasKey(tenant, namespace, function, alias), rawAlias, 0)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return 0, cserrors.Wrap(cserrors.CSKVWriteFailed, "failed to publish version", err)
	}
	return version, nil
}

func (s *Store) GetVersion(ctx context.Context, tenant, namespace, function string, version int64) (api.VersionRecord, []byte, error) {
	var meta api.VersionRecord
	metaRaw, err := s.client.Get(ctx, VersionMetaKey(tenant, namespace, function, version)).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return meta, nil, cserrors.New(cserrors.CSValidationFailed, "version not found")
		}
		return meta, nil, cserrors.Wrap(cserrors.CSKVReadFailed, "failed to read version meta", err)
	}
	if err := json.Unmarshal(metaRaw, &meta); err != nil {
		return meta, nil, err
	}
	bundle, err := s.client.Get(ctx, VersionBundleKey(tenant, namespace, function, version)).Bytes()
	if err != nil {
		return meta, nil, cserrors.Wrap(cserrors.CSKVReadFailed, "failed to read version bundle", err)
	}
	return meta, bundle, nil
}

func (s *Store) GetLatestVersion(ctx context.Context, tenant, namespace, function string) (api.VersionRecord, error) {
	var meta api.VersionRecord
	latest, err := s.client.Get(ctx, VersionSeqKey(tenant, namespace, function)).Int64()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return meta, cserrors.New(cserrors.CSValidationFailed, "version not found")
		}
		return meta, cserrors.Wrap(cserrors.CSKVReadFailed, "failed to read latest version sequence", err)
	}
	if latest <= 0 {
		return meta, cserrors.New(cserrors.CSValidationFailed, "version not found")
	}
	meta, _, err = s.GetVersion(ctx, tenant, namespace, function, latest)
	if err != nil {
		return meta, err
	}
	return meta, nil
}

func (s *Store) SetAlias(ctx context.Context, tenant, namespace, function, alias string, version int64) error {
	raw, _ := json.Marshal(api.AliasRecord{Alias: alias, Version: version, UpdatedAtMS: time.Now().UnixMilli()})
	if err := s.client.Set(ctx, AliasKey(tenant, namespace, function, alias), raw, 0).Err(); err != nil {
		return cserrors.Wrap(cserrors.CSKVWriteFailed, "failed to set alias", err)
	}
	return nil
}

func (s *Store) GetAlias(ctx context.Context, tenant, namespace, function, alias string) (api.AliasRecord, error) {
	var out api.AliasRecord
	raw, err := s.client.Get(ctx, AliasKey(tenant, namespace, function, alias)).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return out, cserrors.New(cserrors.CSValidationFailed, "alias not found")
		}
		return out, cserrors.Wrap(cserrors.CSKVReadFailed, "failed to get alias", err)
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return out, err
	}
	return out, nil
}

func (s *Store) ListAliases(ctx context.Context, tenant, namespace, function string) ([]api.AliasRecord, error) {
	var (
		cursor uint64
		out    []api.AliasRecord
	)
	pattern := AliasPattern(tenant, namespace, function)
	for {
		keys, next, err := s.client.Scan(ctx, cursor, pattern, 100).Result()
		if err != nil {
			return nil, cserrors.Wrap(cserrors.CSKVReadFailed, "failed to list aliases", err)
		}
		for _, key := range keys {
			raw, err := s.client.Get(ctx, key).Bytes()
			if err != nil {
				continue
			}
			var rec api.AliasRecord
			if err := json.Unmarshal(raw, &rec); err == nil {
				out = append(out, rec)
			}
		}
		if next == 0 {
			break
		}
		cursor = next
	}
	return out, nil
}

func (s *Store) ResolveVersion(ctx context.Context, tenant, namespace, function, alias string, version int64) (int64, error) {
	if version > 0 {
		return version, nil
	}
	if alias == "" {
		return 0, cserrors.New(cserrors.CSValidationFailed, "either alias or version must be set")
	}
	a, err := s.GetAlias(ctx, tenant, namespace, function, alias)
	if err != nil {
		return 0, err
	}
	return a.Version, nil
}

func (s *Store) PutActivationRunning(ctx context.Context, rec api.ActivationRecord, ttl time.Duration) error {
	raw, _ := json.Marshal(rec)
	pipe := s.client.TxPipeline()
	pipe.Set(ctx, ActivationMetaKey(rec.Tenant, rec.ActivationID), raw, ttl)
	pipe.Set(ctx, ActivationStatusKey(rec.Tenant, rec.ActivationID), rec.Status, ttl)
	if _, err := pipe.Exec(ctx); err != nil {
		return cserrors.Wrap(cserrors.CSKVWriteFailed, "failed to write activation start", err)
	}
	return nil
}

func (s *Store) CompleteActivationCAS(ctx context.Context, rec api.ActivationRecord, ttl time.Duration) (bool, error) {
	raw, _ := json.Marshal(rec)
	ttlSeconds := int64(ttl / time.Second)
	if ttlSeconds <= 0 {
		ttlSeconds = 1
	}
	res, err := s.casActivation.Run(ctx, s.client,
		[]string{ActivationStatusKey(rec.Tenant, rec.ActivationID), ActivationMetaKey(rec.Tenant, rec.ActivationID)},
		"running", rec.Status, raw, strconv.FormatInt(ttlSeconds, 10),
	).Int64()
	if err != nil {
		return false, cserrors.Wrap(cserrors.CSKVCASFailed, "failed CAS update for activation terminal state", err)
	}
	return res == 1, nil
}

func (s *Store) GetActivation(ctx context.Context, tenant, activationID string) (api.ActivationRecord, error) {
	var out api.ActivationRecord
	raw, err := s.client.Get(ctx, ActivationMetaKey(tenant, activationID)).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return out, cserrors.New(cserrors.CSValidationFailed, "activation not found")
		}
		return out, cserrors.Wrap(cserrors.CSKVReadFailed, "failed to read activation", err)
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return out, err
	}
	return out, nil
}

func (s *Store) SaveResultByRequestID(ctx context.Context, tenant, requestID string, result api.InvocationResult, ttl time.Duration) error {
	raw, _ := json.Marshal(result)
	if err := s.client.Set(ctx, ActivationResultByRequestKey(tenant, requestID), raw, ttl).Err(); err != nil {
		return cserrors.Wrap(cserrors.CSKVWriteFailed, "failed to save request correlation result", err)
	}
	return nil
}

func (s *Store) GetResultByRequestID(ctx context.Context, tenant, requestID string) (api.InvocationResult, error) {
	var out api.InvocationResult
	raw, err := s.client.Get(ctx, ActivationResultByRequestKey(tenant, requestID)).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return out, cserrors.New(cserrors.CSCodeQTimeout, "result not found")
		}
		return out, cserrors.Wrap(cserrors.CSKVReadFailed, "failed to read request correlation result", err)
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return out, err
	}
	return out, nil
}

func (s *Store) AppendLogChunk(ctx context.Context, tenant, activationID string, chunk int64, payload []byte, ttl time.Duration) error {
	chunkKey := LogChunkKey(tenant, activationID, chunk)
	idxKey := LogChunkIndexKey(tenant, activationID)
	pipe := s.client.TxPipeline()
	pipe.Set(ctx, chunkKey, payload, ttl)
	pipe.ZAdd(ctx, idxKey, redis.Z{Score: float64(chunk), Member: chunk})
	pipe.Expire(ctx, idxKey, ttl)
	if _, err := pipe.Exec(ctx); err != nil {
		return cserrors.Wrap(cserrors.CSKVWriteFailed, "failed to append log chunk", err)
	}
	return nil
}

func (s *Store) ListLogChunks(ctx context.Context, tenant, activationID string, offset, limit int64) ([]string, int64, error) {
	if limit <= 0 {
		limit = 100
	}
	idxKey := LogChunkIndexKey(tenant, activationID)
	members, err := s.client.ZRange(ctx, idxKey, offset, offset+limit-1).Result()
	if err != nil {
		return nil, offset, cserrors.Wrap(cserrors.CSKVReadFailed, "failed to list log chunks", err)
	}
	out := make([]string, 0, len(members))
	for _, m := range members {
		chunkID, convErr := strconv.ParseInt(m, 10, 64)
		if convErr != nil {
			continue
		}
		chunkRaw, err := s.client.Get(ctx, LogChunkKey(tenant, activationID, chunkID)).Bytes()
		if err != nil {
			continue
		}
		out = append(out, string(chunkRaw))
	}
	next := offset + int64(len(members))
	return out, next, nil
}

func (s *Store) PutSchedule(ctx context.Context, rec api.ScheduleRecord) error {
	raw, _ := json.Marshal(rec)
	metaKey := ScheduleMetaKey(rec.Tenant, rec.Namespace, rec.Name)
	idxKey := ScheduleIndexKey(rec.Tenant, rec.Namespace)
	pipe := s.client.TxPipeline()
	pipe.Set(ctx, metaKey, raw, 0)
	pipe.SAdd(ctx, idxKey, rec.Name)
	if _, err := pipe.Exec(ctx); err != nil {
		return cserrors.Wrap(cserrors.CSKVWriteFailed, "failed to put schedule", err)
	}
	return nil
}

func (s *Store) ListSchedules(ctx context.Context, tenant, namespace string) ([]api.ScheduleRecord, error) {
	names, err := s.client.SMembers(ctx, ScheduleIndexKey(tenant, namespace)).Result()
	if err != nil {
		return nil, cserrors.Wrap(cserrors.CSKVReadFailed, "failed to list schedule names", err)
	}
	out := make([]api.ScheduleRecord, 0, len(names))
	for _, name := range names {
		raw, err := s.client.Get(ctx, ScheduleMetaKey(tenant, namespace, name)).Bytes()
		if err != nil {
			continue
		}
		var rec api.ScheduleRecord
		if err := json.Unmarshal(raw, &rec); err == nil {
			out = append(out, rec)
		}
	}
	return out, nil
}

func (s *Store) ListAllSchedules(ctx context.Context) ([]api.ScheduleRecord, error) {
	var (
		cursor uint64
		out    []api.ScheduleRecord
	)
	for {
		keys, next, err := s.client.Scan(ctx, cursor, "cs:schedule:*:*:index", 100).Result()
		if err != nil {
			return nil, cserrors.Wrap(cserrors.CSKVReadFailed, "failed to scan schedule indexes", err)
		}
		for _, key := range keys {
			names, err := s.client.SMembers(ctx, key).Result()
			if err != nil {
				continue
			}
			parts := strings.Split(key, ":")
			if len(parts) < 5 {
				continue
			}
			tenant := parts[2]
			namespace := parts[3]
			for _, name := range names {
				raw, err := s.client.Get(ctx, ScheduleMetaKey(tenant, namespace, name)).Bytes()
				if err != nil {
					continue
				}
				var rec api.ScheduleRecord
				if err := json.Unmarshal(raw, &rec); err == nil {
					out = append(out, rec)
				}
			}
		}
		if next == 0 {
			break
		}
		cursor = next
	}
	return out, nil
}

func (s *Store) GetSchedule(ctx context.Context, tenant, namespace, name string) (api.ScheduleRecord, error) {
	var out api.ScheduleRecord
	raw, err := s.client.Get(ctx, ScheduleMetaKey(tenant, namespace, name)).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return out, cserrors.New(cserrors.CSValidationFailed, "schedule not found")
		}
		return out, err
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return out, err
	}
	return out, nil
}

func (s *Store) DeleteSchedule(ctx context.Context, tenant, namespace, name string) error {
	pipe := s.client.TxPipeline()
	pipe.Del(ctx, ScheduleMetaKey(tenant, namespace, name))
	pipe.SRem(ctx, ScheduleIndexKey(tenant, namespace), name)
	pipe.Del(ctx, ScheduleStateKey(tenant, namespace, name), ScheduleInflightKey(tenant, namespace, name))
	_, err := pipe.Exec(ctx)
	if err != nil {
		return err
	}
	return nil
}

func (s *Store) GetScheduleState(ctx context.Context, tenant, namespace, name string) (api.ScheduleState, error) {
	var state api.ScheduleState
	raw, err := s.client.Get(ctx, ScheduleStateKey(tenant, namespace, name)).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return api.ScheduleState{}, nil
		}
		return state, err
	}
	if err := json.Unmarshal(raw, &state); err != nil {
		return state, err
	}
	return state, nil
}

func (s *Store) PutScheduleState(ctx context.Context, tenant, namespace, name string, state api.ScheduleState) error {
	raw, _ := json.Marshal(state)
	if err := s.client.Set(ctx, ScheduleStateKey(tenant, namespace, name), raw, 0).Err(); err != nil {
		return cserrors.Wrap(cserrors.CSSchedulerStateWrite, "failed to write schedule state", err)
	}
	return nil
}

func (s *Store) SetScheduleInflight(ctx context.Context, tenant, namespace, name, activationID string, ttl time.Duration) error {
	return s.client.Set(ctx, ScheduleInflightKey(tenant, namespace, name), activationID, ttl).Err()
}

func (s *Store) GetScheduleInflight(ctx context.Context, tenant, namespace, name string) (string, error) {
	v, err := s.client.Get(ctx, ScheduleInflightKey(tenant, namespace, name)).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return "", nil
		}
		return "", err
	}
	return v, nil
}

func (s *Store) ClearScheduleInflight(ctx context.Context, tenant, namespace, name string) error {
	return s.client.Del(ctx, ScheduleInflightKey(tenant, namespace, name)).Err()
}

func (s *Store) PutWorkerBinding(ctx context.Context, rec api.WorkerBinding) error {
	raw, _ := json.Marshal(rec)
	metaKey := WorkerBindingMetaKey(rec.Tenant, rec.Namespace, rec.Name)
	idxKey := WorkerBindingIndexKey(rec.Tenant, rec.Namespace)
	pipe := s.client.TxPipeline()
	pipe.Set(ctx, metaKey, raw, 0)
	pipe.SAdd(ctx, idxKey, rec.Name)
	_, err := pipe.Exec(ctx)
	if err != nil {
		return cserrors.Wrap(cserrors.CSKVWriteFailed, "failed to put worker binding", err)
	}
	return nil
}

func (s *Store) ListWorkerBindings(ctx context.Context, tenant, namespace string) ([]api.WorkerBinding, error) {
	names, err := s.client.SMembers(ctx, WorkerBindingIndexKey(tenant, namespace)).Result()
	if err != nil {
		return nil, err
	}
	out := make([]api.WorkerBinding, 0, len(names))
	for _, name := range names {
		raw, err := s.client.Get(ctx, WorkerBindingMetaKey(tenant, namespace, name)).Bytes()
		if err != nil {
			continue
		}
		var rec api.WorkerBinding
		if err := json.Unmarshal(raw, &rec); err == nil {
			out = append(out, rec)
		}
	}
	return out, nil
}

func (s *Store) ListAllWorkerBindings(ctx context.Context) ([]api.WorkerBinding, error) {
	var (
		cursor uint64
		out    []api.WorkerBinding
	)
	for {
		keys, next, err := s.client.Scan(ctx, cursor, "cs:cadence:*:*:workers:index", 100).Result()
		if err != nil {
			return nil, err
		}
		for _, key := range keys {
			parts := strings.Split(key, ":")
			if len(parts) < 5 {
				continue
			}
			tenant := parts[2]
			namespace := parts[3]
			names, err := s.client.SMembers(ctx, key).Result()
			if err != nil {
				continue
			}
			for _, name := range names {
				raw, err := s.client.Get(ctx, WorkerBindingMetaKey(tenant, namespace, name)).Bytes()
				if err != nil {
					continue
				}
				var rec api.WorkerBinding
				if err := json.Unmarshal(raw, &rec); err == nil {
					out = append(out, rec)
				}
			}
		}
		if next == 0 {
			break
		}
		cursor = next
	}
	return out, nil
}

func (s *Store) GetWorkerBinding(ctx context.Context, tenant, namespace, name string) (api.WorkerBinding, error) {
	var out api.WorkerBinding
	raw, err := s.client.Get(ctx, WorkerBindingMetaKey(tenant, namespace, name)).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return out, cserrors.New(cserrors.CSValidationFailed, "worker binding not found")
		}
		return out, err
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return out, err
	}
	return out, nil
}

func (s *Store) DeleteWorkerBinding(ctx context.Context, tenant, namespace, name string) error {
	pipe := s.client.TxPipeline()
	pipe.Del(ctx, WorkerBindingMetaKey(tenant, namespace, name))
	pipe.SRem(ctx, WorkerBindingIndexKey(tenant, namespace), name)
	_, err := pipe.Exec(ctx)
	return err
}

func (s *Store) PutCadenceTaskMapping(ctx context.Context, tenant, namespace, taskTokenHash, activationID string, ttl time.Duration) error {
	payload := map[string]any{"activation_id": activationID, "created_at_ms": time.Now().UnixMilli()}
	raw, _ := json.Marshal(payload)
	return s.client.Set(ctx, CadenceTaskKey(tenant, namespace, taskTokenHash), raw, ttl).Err()
}

func (s *Store) GetCadenceTaskMapping(ctx context.Context, tenant, namespace, taskTokenHash string) (string, error) {
	raw, err := s.client.Get(ctx, CadenceTaskKey(tenant, namespace, taskTokenHash)).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return "", nil
		}
		return "", err
	}
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		return "", err
	}
	actID, _ := payload["activation_id"].(string)
	return actID, nil
}

func (s *Store) DeleteCadenceTaskMapping(ctx context.Context, tenant, namespace, taskTokenHash string) error {
	return s.client.Del(ctx, CadenceTaskKey(tenant, namespace, taskTokenHash)).Err()
}

func (s *Store) IsActivationTerminal(ctx context.Context, tenant, activationID string) (bool, api.ActivationRecord, error) {
	rec, err := s.GetActivation(ctx, tenant, activationID)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			return false, api.ActivationRecord{}, nil
		}
		return false, api.ActivationRecord{}, err
	}
	_, ok := terminalStatuses[rec.Status]
	return ok, rec, nil
}

func (s *Store) RawClient() *redis.Client {
	return s.client
}

func (s *Store) KVGet(ctx context.Context, key string) (string, error) {
	return s.client.Get(ctx, key).Result()
}

func (s *Store) KVSet(ctx context.Context, key, value string, ttl time.Duration) error {
	return s.client.Set(ctx, key, value, ttl).Err()
}

func (s *Store) KVDel(ctx context.Context, key string) error {
	return s.client.Del(ctx, key).Err()
}

func (s *Store) TryAcquireLease(ctx context.Context, key, value string, ttl time.Duration) (bool, error) {
	return s.client.SetNX(ctx, key, value, ttl).Result()
}

func (s *Store) GetLeaseValue(ctx context.Context, key string) (string, error) {
	return s.client.Get(ctx, key).Result()
}

func (s *Store) ExtendLease(ctx context.Context, key string, ttl time.Duration) error {
	return s.client.Expire(ctx, key, ttl).Err()
}

func ParseCursor(v string) int64 {
	if v == "" {
		return 0
	}
	parsed, err := strconv.ParseInt(v, 10, 64)
	if err != nil || parsed < 0 {
		return 0
	}
	return parsed
}

func EncodeCursor(v int64) string {
	if v < 0 {
		v = 0
	}
	return fmt.Sprintf("%d", v)
}
