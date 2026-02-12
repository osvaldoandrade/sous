package testutil

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/osvaldoandrade/sous/internal/api"
	cserrors "github.com/osvaldoandrade/sous/internal/errors"
	"github.com/osvaldoandrade/sous/internal/plugins/messaging"
)

type FakePersistence struct {
	mu sync.Mutex

	CloseFn func() error
	PingFn  func(context.Context) error

	CreateFunctionFn     func(context.Context, api.FunctionRecord) error
	GetFunctionFn        func(context.Context, string, string, string) (api.FunctionRecord, error)
	SoftDeleteFunctionFn func(context.Context, string, string, string, int64) error

	PutDraftFn          func(context.Context, string, string, string, api.DraftRecord, time.Duration) error
	GetDraftFn          func(context.Context, string, string, string, string) (api.DraftRecord, error)
	MarkDraftConsumedFn func(context.Context, string, string, string, string, time.Duration) error

	PublishVersionFn   func(context.Context, string, string, string, api.VersionRecord, []byte, string) (int64, error)
	GetVersionFn       func(context.Context, string, string, string, int64) (api.VersionRecord, []byte, error)
	GetLatestVersionFn func(context.Context, string, string, string) (api.VersionRecord, error)
	SetAliasFn         func(context.Context, string, string, string, string, int64) error
	GetAliasFn         func(context.Context, string, string, string, string) (api.AliasRecord, error)
	ListAliasesFn      func(context.Context, string, string, string) ([]api.AliasRecord, error)
	ResolveVersionFn   func(context.Context, string, string, string, string, int64) (int64, error)

	PutActivationRunningFn  func(context.Context, api.ActivationRecord, time.Duration) error
	CompleteActivationCASFn func(context.Context, api.ActivationRecord, time.Duration) (bool, error)
	GetActivationFn         func(context.Context, string, string) (api.ActivationRecord, error)
	IsActivationTerminalFn  func(context.Context, string, string) (bool, api.ActivationRecord, error)
	SaveResultByRequestIDFn func(context.Context, string, string, api.InvocationResult, time.Duration) error
	GetResultByRequestIDFn  func(context.Context, string, string) (api.InvocationResult, error)

	AppendLogChunkFn func(context.Context, string, string, int64, []byte, time.Duration) error
	ListLogChunksFn  func(context.Context, string, string, int64, int64) ([]string, int64, error)

	PutScheduleFn           func(context.Context, api.ScheduleRecord) error
	ListSchedulesFn         func(context.Context, string, string) ([]api.ScheduleRecord, error)
	ListAllSchedulesFn      func(context.Context) ([]api.ScheduleRecord, error)
	GetScheduleFn           func(context.Context, string, string, string) (api.ScheduleRecord, error)
	DeleteScheduleFn        func(context.Context, string, string, string) error
	GetScheduleStateFn      func(context.Context, string, string, string) (api.ScheduleState, error)
	PutScheduleStateFn      func(context.Context, string, string, string, api.ScheduleState) error
	SetScheduleInflightFn   func(context.Context, string, string, string, string, time.Duration) error
	GetScheduleInflightFn   func(context.Context, string, string, string) (string, error)
	ClearScheduleInflightFn func(context.Context, string, string, string) error

	PutWorkerBindingFn      func(context.Context, api.WorkerBinding) error
	ListWorkerBindingsFn    func(context.Context, string, string) ([]api.WorkerBinding, error)
	ListAllWorkerBindingsFn func(context.Context) ([]api.WorkerBinding, error)
	GetWorkerBindingFn      func(context.Context, string, string, string) (api.WorkerBinding, error)
	DeleteWorkerBindingFn   func(context.Context, string, string, string) error

	PutCadenceTaskMappingFn    func(context.Context, string, string, string, string, time.Duration) error
	GetCadenceTaskMappingFn    func(context.Context, string, string, string) (string, error)
	DeleteCadenceTaskMappingFn func(context.Context, string, string, string) error

	KVGetFn func(context.Context, string) (string, error)
	KVSetFn func(context.Context, string, string, time.Duration) error
	KVDelFn func(context.Context, string) error

	TryAcquireLeaseFn func(context.Context, string, string, time.Duration) (bool, error)
	GetLeaseValueFn   func(context.Context, string) (string, error)
	ExtendLeaseFn     func(context.Context, string, time.Duration) error
}

func (f *FakePersistence) Close() error {
	if f.CloseFn != nil {
		return f.CloseFn()
	}
	return nil
}

func (f *FakePersistence) Ping(ctx context.Context) error {
	if f.PingFn != nil {
		return f.PingFn(ctx)
	}
	return nil
}

func (f *FakePersistence) CreateFunction(ctx context.Context, rec api.FunctionRecord) error {
	if f.CreateFunctionFn != nil {
		return f.CreateFunctionFn(ctx, rec)
	}
	return nil
}

func (f *FakePersistence) GetFunction(ctx context.Context, tenant, namespace, function string) (api.FunctionRecord, error) {
	if f.GetFunctionFn != nil {
		return f.GetFunctionFn(ctx, tenant, namespace, function)
	}
	return api.FunctionRecord{}, errors.New("not implemented")
}

func (f *FakePersistence) SoftDeleteFunction(ctx context.Context, tenant, namespace, function string, deletedAtMS int64) error {
	if f.SoftDeleteFunctionFn != nil {
		return f.SoftDeleteFunctionFn(ctx, tenant, namespace, function, deletedAtMS)
	}
	return nil
}

func (f *FakePersistence) PutDraft(ctx context.Context, tenant, namespace, function string, draft api.DraftRecord, ttl time.Duration) error {
	if f.PutDraftFn != nil {
		return f.PutDraftFn(ctx, tenant, namespace, function, draft, ttl)
	}
	return nil
}

func (f *FakePersistence) GetDraft(ctx context.Context, tenant, namespace, function, draftID string) (api.DraftRecord, error) {
	if f.GetDraftFn != nil {
		return f.GetDraftFn(ctx, tenant, namespace, function, draftID)
	}
	return api.DraftRecord{}, errors.New("not implemented")
}

func (f *FakePersistence) MarkDraftConsumed(ctx context.Context, tenant, namespace, function, draftID string, ttl time.Duration) error {
	if f.MarkDraftConsumedFn != nil {
		return f.MarkDraftConsumedFn(ctx, tenant, namespace, function, draftID, ttl)
	}
	return nil
}

func (f *FakePersistence) PublishVersion(ctx context.Context, tenant, namespace, function string, versionMeta api.VersionRecord, bundle []byte, alias string) (int64, error) {
	if f.PublishVersionFn != nil {
		return f.PublishVersionFn(ctx, tenant, namespace, function, versionMeta, bundle, alias)
	}
	return 1, nil
}

func (f *FakePersistence) GetVersion(ctx context.Context, tenant, namespace, function string, version int64) (api.VersionRecord, []byte, error) {
	if f.GetVersionFn != nil {
		return f.GetVersionFn(ctx, tenant, namespace, function, version)
	}
	return api.VersionRecord{}, nil, errors.New("not implemented")
}

func (f *FakePersistence) GetLatestVersion(ctx context.Context, tenant, namespace, function string) (api.VersionRecord, error) {
	if f.GetLatestVersionFn != nil {
		return f.GetLatestVersionFn(ctx, tenant, namespace, function)
	}
	return api.VersionRecord{}, cserrors.New(cserrors.CSValidationFailed, "version not found")
}

func (f *FakePersistence) SetAlias(ctx context.Context, tenant, namespace, function, alias string, version int64) error {
	if f.SetAliasFn != nil {
		return f.SetAliasFn(ctx, tenant, namespace, function, alias, version)
	}
	return nil
}

func (f *FakePersistence) GetAlias(ctx context.Context, tenant, namespace, function, alias string) (api.AliasRecord, error) {
	if f.GetAliasFn != nil {
		return f.GetAliasFn(ctx, tenant, namespace, function, alias)
	}
	return api.AliasRecord{}, errors.New("not implemented")
}

func (f *FakePersistence) ListAliases(ctx context.Context, tenant, namespace, function string) ([]api.AliasRecord, error) {
	if f.ListAliasesFn != nil {
		return f.ListAliasesFn(ctx, tenant, namespace, function)
	}
	return nil, nil
}

func (f *FakePersistence) ResolveVersion(ctx context.Context, tenant, namespace, function, alias string, version int64) (int64, error) {
	if f.ResolveVersionFn != nil {
		return f.ResolveVersionFn(ctx, tenant, namespace, function, alias, version)
	}
	if version > 0 {
		return version, nil
	}
	return 1, nil
}

func (f *FakePersistence) PutActivationRunning(ctx context.Context, rec api.ActivationRecord, ttl time.Duration) error {
	if f.PutActivationRunningFn != nil {
		return f.PutActivationRunningFn(ctx, rec, ttl)
	}
	return nil
}

func (f *FakePersistence) CompleteActivationCAS(ctx context.Context, rec api.ActivationRecord, ttl time.Duration) (bool, error) {
	if f.CompleteActivationCASFn != nil {
		return f.CompleteActivationCASFn(ctx, rec, ttl)
	}
	return true, nil
}

func (f *FakePersistence) GetActivation(ctx context.Context, tenant, activationID string) (api.ActivationRecord, error) {
	if f.GetActivationFn != nil {
		return f.GetActivationFn(ctx, tenant, activationID)
	}
	return api.ActivationRecord{}, errors.New("not implemented")
}

func (f *FakePersistence) IsActivationTerminal(ctx context.Context, tenant, activationID string) (bool, api.ActivationRecord, error) {
	if f.IsActivationTerminalFn != nil {
		return f.IsActivationTerminalFn(ctx, tenant, activationID)
	}
	return false, api.ActivationRecord{}, nil
}

func (f *FakePersistence) SaveResultByRequestID(ctx context.Context, tenant, requestID string, result api.InvocationResult, ttl time.Duration) error {
	if f.SaveResultByRequestIDFn != nil {
		return f.SaveResultByRequestIDFn(ctx, tenant, requestID, result, ttl)
	}
	return nil
}

func (f *FakePersistence) GetResultByRequestID(ctx context.Context, tenant, requestID string) (api.InvocationResult, error) {
	if f.GetResultByRequestIDFn != nil {
		return f.GetResultByRequestIDFn(ctx, tenant, requestID)
	}
	return api.InvocationResult{}, errors.New("not implemented")
}

func (f *FakePersistence) AppendLogChunk(ctx context.Context, tenant, activationID string, chunk int64, payload []byte, ttl time.Duration) error {
	if f.AppendLogChunkFn != nil {
		return f.AppendLogChunkFn(ctx, tenant, activationID, chunk, payload, ttl)
	}
	return nil
}

func (f *FakePersistence) ListLogChunks(ctx context.Context, tenant, activationID string, offset, limit int64) ([]string, int64, error) {
	if f.ListLogChunksFn != nil {
		return f.ListLogChunksFn(ctx, tenant, activationID, offset, limit)
	}
	return nil, 0, nil
}

func (f *FakePersistence) PutSchedule(ctx context.Context, rec api.ScheduleRecord) error {
	if f.PutScheduleFn != nil {
		return f.PutScheduleFn(ctx, rec)
	}
	return nil
}

func (f *FakePersistence) ListSchedules(ctx context.Context, tenant, namespace string) ([]api.ScheduleRecord, error) {
	if f.ListSchedulesFn != nil {
		return f.ListSchedulesFn(ctx, tenant, namespace)
	}
	return nil, nil
}

func (f *FakePersistence) ListAllSchedules(ctx context.Context) ([]api.ScheduleRecord, error) {
	if f.ListAllSchedulesFn != nil {
		return f.ListAllSchedulesFn(ctx)
	}
	return nil, nil
}

func (f *FakePersistence) GetSchedule(ctx context.Context, tenant, namespace, name string) (api.ScheduleRecord, error) {
	if f.GetScheduleFn != nil {
		return f.GetScheduleFn(ctx, tenant, namespace, name)
	}
	return api.ScheduleRecord{}, errors.New("not implemented")
}

func (f *FakePersistence) DeleteSchedule(ctx context.Context, tenant, namespace, name string) error {
	if f.DeleteScheduleFn != nil {
		return f.DeleteScheduleFn(ctx, tenant, namespace, name)
	}
	return nil
}

func (f *FakePersistence) GetScheduleState(ctx context.Context, tenant, namespace, name string) (api.ScheduleState, error) {
	if f.GetScheduleStateFn != nil {
		return f.GetScheduleStateFn(ctx, tenant, namespace, name)
	}
	return api.ScheduleState{}, nil
}

func (f *FakePersistence) PutScheduleState(ctx context.Context, tenant, namespace, name string, state api.ScheduleState) error {
	if f.PutScheduleStateFn != nil {
		return f.PutScheduleStateFn(ctx, tenant, namespace, name, state)
	}
	return nil
}

func (f *FakePersistence) SetScheduleInflight(ctx context.Context, tenant, namespace, name, activationID string, ttl time.Duration) error {
	if f.SetScheduleInflightFn != nil {
		return f.SetScheduleInflightFn(ctx, tenant, namespace, name, activationID, ttl)
	}
	return nil
}

func (f *FakePersistence) GetScheduleInflight(ctx context.Context, tenant, namespace, name string) (string, error) {
	if f.GetScheduleInflightFn != nil {
		return f.GetScheduleInflightFn(ctx, tenant, namespace, name)
	}
	return "", nil
}

func (f *FakePersistence) ClearScheduleInflight(ctx context.Context, tenant, namespace, name string) error {
	if f.ClearScheduleInflightFn != nil {
		return f.ClearScheduleInflightFn(ctx, tenant, namespace, name)
	}
	return nil
}

func (f *FakePersistence) PutWorkerBinding(ctx context.Context, rec api.WorkerBinding) error {
	if f.PutWorkerBindingFn != nil {
		return f.PutWorkerBindingFn(ctx, rec)
	}
	return nil
}

func (f *FakePersistence) ListWorkerBindings(ctx context.Context, tenant, namespace string) ([]api.WorkerBinding, error) {
	if f.ListWorkerBindingsFn != nil {
		return f.ListWorkerBindingsFn(ctx, tenant, namespace)
	}
	return nil, nil
}

func (f *FakePersistence) ListAllWorkerBindings(ctx context.Context) ([]api.WorkerBinding, error) {
	if f.ListAllWorkerBindingsFn != nil {
		return f.ListAllWorkerBindingsFn(ctx)
	}
	return nil, nil
}

func (f *FakePersistence) GetWorkerBinding(ctx context.Context, tenant, namespace, name string) (api.WorkerBinding, error) {
	if f.GetWorkerBindingFn != nil {
		return f.GetWorkerBindingFn(ctx, tenant, namespace, name)
	}
	return api.WorkerBinding{}, errors.New("not implemented")
}

func (f *FakePersistence) DeleteWorkerBinding(ctx context.Context, tenant, namespace, name string) error {
	if f.DeleteWorkerBindingFn != nil {
		return f.DeleteWorkerBindingFn(ctx, tenant, namespace, name)
	}
	return nil
}

func (f *FakePersistence) PutCadenceTaskMapping(ctx context.Context, tenant, namespace, taskTokenHash, activationID string, ttl time.Duration) error {
	if f.PutCadenceTaskMappingFn != nil {
		return f.PutCadenceTaskMappingFn(ctx, tenant, namespace, taskTokenHash, activationID, ttl)
	}
	return nil
}

func (f *FakePersistence) GetCadenceTaskMapping(ctx context.Context, tenant, namespace, taskTokenHash string) (string, error) {
	if f.GetCadenceTaskMappingFn != nil {
		return f.GetCadenceTaskMappingFn(ctx, tenant, namespace, taskTokenHash)
	}
	return "", nil
}

func (f *FakePersistence) DeleteCadenceTaskMapping(ctx context.Context, tenant, namespace, taskTokenHash string) error {
	if f.DeleteCadenceTaskMappingFn != nil {
		return f.DeleteCadenceTaskMappingFn(ctx, tenant, namespace, taskTokenHash)
	}
	return nil
}

func (f *FakePersistence) KVGet(ctx context.Context, key string) (string, error) {
	if f.KVGetFn != nil {
		return f.KVGetFn(ctx, key)
	}
	return "", nil
}

func (f *FakePersistence) KVSet(ctx context.Context, key, value string, ttl time.Duration) error {
	if f.KVSetFn != nil {
		return f.KVSetFn(ctx, key, value, ttl)
	}
	return nil
}

func (f *FakePersistence) KVDel(ctx context.Context, key string) error {
	if f.KVDelFn != nil {
		return f.KVDelFn(ctx, key)
	}
	return nil
}

func (f *FakePersistence) TryAcquireLease(ctx context.Context, key, value string, ttl time.Duration) (bool, error) {
	if f.TryAcquireLeaseFn != nil {
		return f.TryAcquireLeaseFn(ctx, key, value, ttl)
	}
	return false, nil
}

func (f *FakePersistence) GetLeaseValue(ctx context.Context, key string) (string, error) {
	if f.GetLeaseValueFn != nil {
		return f.GetLeaseValueFn(ctx, key)
	}
	return "", nil
}

func (f *FakePersistence) ExtendLease(ctx context.Context, key string, ttl time.Duration) error {
	if f.ExtendLeaseFn != nil {
		return f.ExtendLeaseFn(ctx, key, ttl)
	}
	return nil
}

type FakeMessaging struct {
	PublishFn            func(context.Context, string, string, string, any) error
	PublishInvocationFn  func(context.Context, api.InvocationRequest) error
	PublishResultFn      func(context.Context, string, api.InvocationResult) error
	PublishDLQInvokeFn   func(context.Context, string, api.InvocationRequest) error
	PublishDLQResultFn   func(context.Context, string, api.InvocationResult) error
	ConsumeInvocationsFn func(context.Context, string, func(messaging.Envelope, api.InvocationRequest) error) error
	ConsumeResultsFn     func(context.Context, string, func(messaging.Envelope, api.InvocationResult) error) error
	ConsumeTopicFn       func(context.Context, string, string, func(messaging.Envelope) error) error
	WaitForResultFn      func(context.Context, string) (api.InvocationResult, error)
	CloseFn              func() error
}

func (f *FakeMessaging) Close() error {
	if f.CloseFn != nil {
		return f.CloseFn()
	}
	return nil
}

func (f *FakeMessaging) Publish(ctx context.Context, topic, tenant, typ string, body any) error {
	if f.PublishFn != nil {
		return f.PublishFn(ctx, topic, tenant, typ, body)
	}
	return nil
}

func (f *FakeMessaging) PublishInvocation(ctx context.Context, req api.InvocationRequest) error {
	if f.PublishInvocationFn != nil {
		return f.PublishInvocationFn(ctx, req)
	}
	return nil
}

func (f *FakeMessaging) PublishResult(ctx context.Context, tenant string, result api.InvocationResult) error {
	if f.PublishResultFn != nil {
		return f.PublishResultFn(ctx, tenant, result)
	}
	return nil
}

func (f *FakeMessaging) PublishDLQInvoke(ctx context.Context, tenant string, req api.InvocationRequest) error {
	if f.PublishDLQInvokeFn != nil {
		return f.PublishDLQInvokeFn(ctx, tenant, req)
	}
	return nil
}

func (f *FakeMessaging) PublishDLQResult(ctx context.Context, tenant string, result api.InvocationResult) error {
	if f.PublishDLQResultFn != nil {
		return f.PublishDLQResultFn(ctx, tenant, result)
	}
	return nil
}

func (f *FakeMessaging) ConsumeInvocations(ctx context.Context, groupID string, handler func(messaging.Envelope, api.InvocationRequest) error) error {
	if f.ConsumeInvocationsFn != nil {
		return f.ConsumeInvocationsFn(ctx, groupID, handler)
	}
	<-ctx.Done()
	return nil
}

func (f *FakeMessaging) ConsumeResults(ctx context.Context, groupID string, handler func(messaging.Envelope, api.InvocationResult) error) error {
	if f.ConsumeResultsFn != nil {
		return f.ConsumeResultsFn(ctx, groupID, handler)
	}
	<-ctx.Done()
	return nil
}

func (f *FakeMessaging) ConsumeTopic(ctx context.Context, topic, groupID string, handler func(messaging.Envelope) error) error {
	if f.ConsumeTopicFn != nil {
		return f.ConsumeTopicFn(ctx, topic, groupID, handler)
	}
	<-ctx.Done()
	return nil
}

func (f *FakeMessaging) WaitForResult(ctx context.Context, requestID string) (api.InvocationResult, error) {
	if f.WaitForResultFn != nil {
		return f.WaitForResultFn(ctx, requestID)
	}
	return api.InvocationResult{}, errors.New("not implemented")
}
