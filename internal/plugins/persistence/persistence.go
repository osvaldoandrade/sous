package persistence

import (
	"context"
	"time"

	"github.com/osvaldoandrade/sous/internal/api"
)

// Provider defines persistence extension points consumed by services.
type Provider interface {
	Close() error
	Ping(ctx context.Context) error

	CreateFunction(ctx context.Context, rec api.FunctionRecord) error
	GetFunction(ctx context.Context, tenant, namespace, function string) (api.FunctionRecord, error)
	SoftDeleteFunction(ctx context.Context, tenant, namespace, function string, deletedAtMS int64) error

	PutDraft(ctx context.Context, tenant, namespace, function string, draft api.DraftRecord, ttl time.Duration) error
	GetDraft(ctx context.Context, tenant, namespace, function, draftID string) (api.DraftRecord, error)
	MarkDraftConsumed(ctx context.Context, tenant, namespace, function, draftID string, ttl time.Duration) error

	PublishVersion(ctx context.Context, tenant, namespace, function string, versionMeta api.VersionRecord, bundle []byte, alias string) (int64, error)
	GetVersion(ctx context.Context, tenant, namespace, function string, version int64) (api.VersionRecord, []byte, error)
	GetLatestVersion(ctx context.Context, tenant, namespace, function string) (api.VersionRecord, error)
	SetAlias(ctx context.Context, tenant, namespace, function, alias string, version int64) error
	GetAlias(ctx context.Context, tenant, namespace, function, alias string) (api.AliasRecord, error)
	ListAliases(ctx context.Context, tenant, namespace, function string) ([]api.AliasRecord, error)
	ResolveVersion(ctx context.Context, tenant, namespace, function, alias string, version int64) (int64, error)

	PutActivationRunning(ctx context.Context, rec api.ActivationRecord, ttl time.Duration) error
	CompleteActivationCAS(ctx context.Context, rec api.ActivationRecord, ttl time.Duration) (bool, error)
	GetActivation(ctx context.Context, tenant, activationID string) (api.ActivationRecord, error)
	IsActivationTerminal(ctx context.Context, tenant, activationID string) (bool, api.ActivationRecord, error)
	SaveResultByRequestID(ctx context.Context, tenant, requestID string, result api.InvocationResult, ttl time.Duration) error
	GetResultByRequestID(ctx context.Context, tenant, requestID string) (api.InvocationResult, error)

	AppendLogChunk(ctx context.Context, tenant, activationID string, chunk int64, payload []byte, ttl time.Duration) error
	ListLogChunks(ctx context.Context, tenant, activationID string, offset, limit int64) ([]string, int64, error)

	PutSchedule(ctx context.Context, rec api.ScheduleRecord) error
	ListSchedules(ctx context.Context, tenant, namespace string) ([]api.ScheduleRecord, error)
	ListAllSchedules(ctx context.Context) ([]api.ScheduleRecord, error)
	GetSchedule(ctx context.Context, tenant, namespace, name string) (api.ScheduleRecord, error)
	DeleteSchedule(ctx context.Context, tenant, namespace, name string) error
	GetScheduleState(ctx context.Context, tenant, namespace, name string) (api.ScheduleState, error)
	PutScheduleState(ctx context.Context, tenant, namespace, name string, state api.ScheduleState) error
	SetScheduleInflight(ctx context.Context, tenant, namespace, name, activationID string, ttl time.Duration) error
	GetScheduleInflight(ctx context.Context, tenant, namespace, name string) (string, error)
	ClearScheduleInflight(ctx context.Context, tenant, namespace, name string) error

	PutWorkerBinding(ctx context.Context, rec api.WorkerBinding) error
	ListWorkerBindings(ctx context.Context, tenant, namespace string) ([]api.WorkerBinding, error)
	ListAllWorkerBindings(ctx context.Context) ([]api.WorkerBinding, error)
	GetWorkerBinding(ctx context.Context, tenant, namespace, name string) (api.WorkerBinding, error)
	DeleteWorkerBinding(ctx context.Context, tenant, namespace, name string) error

	PutCadenceTaskMapping(ctx context.Context, tenant, namespace, taskTokenHash, activationID string, ttl time.Duration) error
	GetCadenceTaskMapping(ctx context.Context, tenant, namespace, taskTokenHash string) (string, error)
	DeleteCadenceTaskMapping(ctx context.Context, tenant, namespace, taskTokenHash string) error

	// Generic key-value access used by runtime host APIs.
	KVGet(ctx context.Context, key string) (string, error)
	KVSet(ctx context.Context, key, value string, ttl time.Duration) error
	KVDel(ctx context.Context, key string) error

	// Lease API used by scheduler leader election.
	TryAcquireLease(ctx context.Context, key, value string, ttl time.Duration) (bool, error)
	GetLeaseValue(ctx context.Context, key string) (string, error)
	ExtendLease(ctx context.Context, key string, ttl time.Duration) error
}
