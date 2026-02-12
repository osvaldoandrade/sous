package kv

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/osvaldoandrade/sous/internal/api"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	t.Cleanup(mr.Close)
	store := NewStore(mr.Addr(), "")
	t.Cleanup(func() { _ = store.Close() })
	if err := store.Ping(context.Background()); err != nil {
		t.Fatalf("ping: %v", err)
	}
	return store
}

func TestStoreFunctionDraftVersionAliasFlow(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)

	rec := api.FunctionRecord{Tenant: "t_abc123", Namespace: "payments", Name: "reconcile", Runtime: "cs-js", Entry: "function.js", Handler: "default", CreatedAtMS: 1}
	if err := store.CreateFunction(ctx, rec); err != nil {
		t.Fatalf("create function: %v", err)
	}
	if err := store.CreateFunction(ctx, rec); err == nil {
		t.Fatal("expected duplicate function error")
	}
	gotFn, err := store.GetFunction(ctx, rec.Tenant, rec.Namespace, rec.Name)
	if err != nil || gotFn.Name != rec.Name {
		t.Fatalf("get function: %v got=%+v", err, gotFn)
	}
	if err := store.SoftDeleteFunction(ctx, rec.Tenant, rec.Namespace, rec.Name, 99); err != nil {
		t.Fatalf("soft delete: %v", err)
	}

	draft := api.DraftRecord{DraftID: "d1", SHA256: "abc", Files: map[string]string{"function.js": "ZnVuYw==", "manifest.json": "e30="}, CreatedAtMS: 1, ExpiresAtMS: 2}
	if err := store.PutDraft(ctx, rec.Tenant, rec.Namespace, rec.Name, draft, time.Hour); err != nil {
		t.Fatalf("put draft: %v", err)
	}
	gotDraft, err := store.GetDraft(ctx, rec.Tenant, rec.Namespace, rec.Name, draft.DraftID)
	if err != nil || gotDraft.DraftID != draft.DraftID {
		t.Fatalf("get draft: %v got=%+v", err, gotDraft)
	}
	if err := store.MarkDraftConsumed(ctx, rec.Tenant, rec.Namespace, rec.Name, draft.DraftID, time.Hour); err != nil {
		t.Fatalf("mark consumed: %v", err)
	}

	verMeta := api.VersionRecord{SHA256: "abc", Config: api.VersionConfig{TimeoutMS: 1000, MemoryMB: 64, MaxConcurrency: 1}, PublishedAtMS: 10}
	version, err := store.PublishVersion(ctx, rec.Tenant, rec.Namespace, rec.Name, verMeta, []byte("bundle"), "prod")
	if err != nil {
		t.Fatalf("publish version: %v", err)
	}
	if version <= 0 {
		t.Fatalf("unexpected version: %d", version)
	}
	gotMeta, gotBundle, err := store.GetVersion(ctx, rec.Tenant, rec.Namespace, rec.Name, version)
	if err != nil {
		t.Fatalf("get version: %v", err)
	}
	if gotMeta.Version != version || string(gotBundle) != "bundle" {
		t.Fatalf("unexpected version payload: meta=%+v bundle=%s", gotMeta, string(gotBundle))
	}
	latest, err := store.GetLatestVersion(ctx, rec.Tenant, rec.Namespace, rec.Name)
	if err != nil {
		t.Fatalf("get latest version: %v", err)
	}
	if latest.Version != version || latest.SHA256 != "abc" {
		t.Fatalf("unexpected latest version payload: %+v", latest)
	}
	if err := store.SetAlias(ctx, rec.Tenant, rec.Namespace, rec.Name, "staging", version); err != nil {
		t.Fatalf("set alias: %v", err)
	}
	alias, err := store.GetAlias(ctx, rec.Tenant, rec.Namespace, rec.Name, "staging")
	if err != nil || alias.Version != version {
		t.Fatalf("get alias: %v alias=%+v", err, alias)
	}
	aliases, err := store.ListAliases(ctx, rec.Tenant, rec.Namespace, rec.Name)
	if err != nil || len(aliases) < 1 {
		t.Fatalf("list aliases: %v aliases=%+v", err, aliases)
	}
	resolved, err := store.ResolveVersion(ctx, rec.Tenant, rec.Namespace, rec.Name, "prod", 0)
	if err != nil || resolved != version {
		t.Fatalf("resolve alias version: %v resolved=%d", err, resolved)
	}
	if _, err := store.ResolveVersion(ctx, rec.Tenant, rec.Namespace, rec.Name, "", 0); err == nil {
		t.Fatal("expected resolve validation error")
	}
}

func TestStoreActivationResultAndLogsFlow(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)

	activation := api.ActivationRecord{ActivationID: "act1", Tenant: "t_abc123", Namespace: "payments", Function: "reconcile", Status: "running", StartMS: 1}
	if err := store.PutActivationRunning(ctx, activation, time.Hour); err != nil {
		t.Fatalf("put activation running: %v", err)
	}
	activation.Status = "success"
	activation.EndMS = 2
	activation.DurationMS = 1
	updated, err := store.CompleteActivationCAS(ctx, activation, time.Hour)
	if err != nil || !updated {
		t.Fatalf("complete activation: %v updated=%v", err, updated)
	}
	gotActivation, err := store.GetActivation(ctx, activation.Tenant, activation.ActivationID)
	if err != nil || gotActivation.Status != "success" {
		t.Fatalf("get activation: %v got=%+v", err, gotActivation)
	}
	terminal, gotTerminal, err := store.IsActivationTerminal(ctx, activation.Tenant, activation.ActivationID)
	if err != nil || !terminal || gotTerminal.Status != "success" {
		t.Fatalf("is terminal: %v terminal=%v got=%+v", err, terminal, gotTerminal)
	}

	result := api.InvocationResult{ActivationID: activation.ActivationID, RequestID: "req1", Status: "success", DurationMS: 1}
	if err := store.SaveResultByRequestID(ctx, activation.Tenant, "req1", result, time.Hour); err != nil {
		t.Fatalf("save result: %v", err)
	}
	gotResult, err := store.GetResultByRequestID(ctx, activation.Tenant, "req1")
	if err != nil || gotResult.RequestID != "req1" {
		t.Fatalf("get result: %v got=%+v", err, gotResult)
	}

	if err := store.AppendLogChunk(ctx, activation.Tenant, activation.ActivationID, 0, []byte("l1"), time.Hour); err != nil {
		t.Fatalf("append log1: %v", err)
	}
	if err := store.AppendLogChunk(ctx, activation.Tenant, activation.ActivationID, 1, []byte("l2"), time.Hour); err != nil {
		t.Fatalf("append log2: %v", err)
	}
	chunks, next, err := store.ListLogChunks(ctx, activation.Tenant, activation.ActivationID, 0, 10)
	if err != nil || len(chunks) != 2 || next != 2 {
		t.Fatalf("list logs: %v chunks=%v next=%d", err, chunks, next)
	}
}

func TestStoreScheduleFlow(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	sch := api.ScheduleRecord{Tenant: "t_abc123", Namespace: "payments", Name: "s1", EverySeconds: 30, OverlapPolicy: "skip", Ref: api.ScheduleRef{Function: "reconcile", Alias: "prod"}, Enabled: true}
	if err := store.PutSchedule(ctx, sch); err != nil {
		t.Fatalf("put schedule: %v", err)
	}
	list, err := store.ListSchedules(ctx, sch.Tenant, sch.Namespace)
	if err != nil || len(list) != 1 {
		t.Fatalf("list schedules: %v list=%+v", err, list)
	}
	all, err := store.ListAllSchedules(ctx)
	if err != nil || len(all) != 1 {
		t.Fatalf("list all schedules: %v all=%+v", err, all)
	}
	got, err := store.GetSchedule(ctx, sch.Tenant, sch.Namespace, sch.Name)
	if err != nil || got.Name != sch.Name {
		t.Fatalf("get schedule: %v got=%+v", err, got)
	}
	state := api.ScheduleState{NextTickMS: 100, TickSeq: 1}
	if err := store.PutScheduleState(ctx, sch.Tenant, sch.Namespace, sch.Name, state); err != nil {
		t.Fatalf("put state: %v", err)
	}
	gotState, err := store.GetScheduleState(ctx, sch.Tenant, sch.Namespace, sch.Name)
	if err != nil || gotState.TickSeq != 1 {
		t.Fatalf("get state: %v state=%+v", err, gotState)
	}
	if err := store.SetScheduleInflight(ctx, sch.Tenant, sch.Namespace, sch.Name, "act1", time.Hour); err != nil {
		t.Fatalf("set inflight: %v", err)
	}
	inflight, err := store.GetScheduleInflight(ctx, sch.Tenant, sch.Namespace, sch.Name)
	if err != nil || inflight != "act1" {
		t.Fatalf("get inflight: %v val=%s", err, inflight)
	}
	if err := store.ClearScheduleInflight(ctx, sch.Tenant, sch.Namespace, sch.Name); err != nil {
		t.Fatalf("clear inflight: %v", err)
	}
	if err := store.DeleteSchedule(ctx, sch.Tenant, sch.Namespace, sch.Name); err != nil {
		t.Fatalf("delete schedule: %v", err)
	}
}

func TestStoreWorkerBindingCadenceKVAndLeaseFlow(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	binding := api.WorkerBinding{Tenant: "t_abc123", Namespace: "payments", Name: "wb1", Domain: "d", Tasklist: "tl", WorkerID: "w", ActivityMap: map[string]api.WorkerBindingRef{"A": {Function: "reconcile", Alias: "prod"}}, Enabled: true}
	if err := store.PutWorkerBinding(ctx, binding); err != nil {
		t.Fatalf("put worker binding: %v", err)
	}
	list, err := store.ListWorkerBindings(ctx, binding.Tenant, binding.Namespace)
	if err != nil || len(list) != 1 {
		t.Fatalf("list bindings: %v list=%+v", err, list)
	}
	all, err := store.ListAllWorkerBindings(ctx)
	if err != nil || len(all) != 1 {
		t.Fatalf("list all bindings: %v all=%+v", err, all)
	}
	got, err := store.GetWorkerBinding(ctx, binding.Tenant, binding.Namespace, binding.Name)
	if err != nil || got.Name != binding.Name {
		t.Fatalf("get binding: %v got=%+v", err, got)
	}
	if err := store.DeleteWorkerBinding(ctx, binding.Tenant, binding.Namespace, binding.Name); err != nil {
		t.Fatalf("delete binding: %v", err)
	}

	if err := store.PutCadenceTaskMapping(ctx, "t_abc123", "payments", "thash", "act1", time.Hour); err != nil {
		t.Fatalf("put task map: %v", err)
	}
	act, err := store.GetCadenceTaskMapping(ctx, "t_abc123", "payments", "thash")
	if err != nil || act != "act1" {
		t.Fatalf("get task map: %v act=%s", err, act)
	}
	if err := store.DeleteCadenceTaskMapping(ctx, "t_abc123", "payments", "thash"); err != nil {
		t.Fatalf("delete task map: %v", err)
	}

	if err := store.KVSet(ctx, "k", "v", time.Hour); err != nil {
		t.Fatalf("kv set: %v", err)
	}
	val, err := store.KVGet(ctx, "k")
	if err != nil || val != "v" {
		t.Fatalf("kv get: %v val=%s", err, val)
	}
	if err := store.KVDel(ctx, "k"); err != nil {
		t.Fatalf("kv del: %v", err)
	}

	ok, err := store.TryAcquireLease(ctx, "lease", "node1", time.Hour)
	if err != nil || !ok {
		t.Fatalf("acquire lease: %v ok=%v", err, ok)
	}
	leaseVal, err := store.GetLeaseValue(ctx, "lease")
	if err != nil || leaseVal != "node1" {
		t.Fatalf("get lease: %v val=%s", err, leaseVal)
	}
	if err := store.ExtendLease(ctx, "lease", time.Hour); err != nil {
		t.Fatalf("extend lease: %v", err)
	}
}
