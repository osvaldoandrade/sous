package kv

import "fmt"

func FunctionMetaKey(tenant, namespace, function string) string {
	return fmt.Sprintf("cs:fn:%s:%s:%s:meta", tenant, namespace, function)
}

func VersionSeqKey(tenant, namespace, function string) string {
	return fmt.Sprintf("cs:fn:%s:%s:%s:version_seq", tenant, namespace, function)
}

func DraftKey(tenant, namespace, function, draftID string) string {
	return fmt.Sprintf("cs:fn:%s:%s:%s:draft:%s", tenant, namespace, function, draftID)
}

func VersionMetaKey(tenant, namespace, function string, version int64) string {
	return fmt.Sprintf("cs:fn:%s:%s:%s:ver:%d:meta", tenant, namespace, function, version)
}

func VersionBundleKey(tenant, namespace, function string, version int64) string {
	return fmt.Sprintf("cs:fn:%s:%s:%s:ver:%d:bundle", tenant, namespace, function, version)
}

func AliasKey(tenant, namespace, function, alias string) string {
	return fmt.Sprintf("cs:fn:%s:%s:%s:alias:%s", tenant, namespace, function, alias)
}

func AliasPattern(tenant, namespace, function string) string {
	return fmt.Sprintf("cs:fn:%s:%s:%s:alias:*", tenant, namespace, function)
}

func ActivationMetaKey(tenant, activationID string) string {
	return fmt.Sprintf("cs:act:%s:%s:meta", tenant, activationID)
}

func ActivationStatusKey(tenant, activationID string) string {
	return fmt.Sprintf("cs:act:%s:%s:status", tenant, activationID)
}

func ActivationResultByRequestKey(tenant, requestID string) string {
	return fmt.Sprintf("cs:req:%s:%s:result", tenant, requestID)
}

func LogChunkKey(tenant, activationID string, chunk int64) string {
	return fmt.Sprintf("cs:log:%s:%s:%012d", tenant, activationID, chunk)
}

func LogChunkIndexKey(tenant, activationID string) string {
	return fmt.Sprintf("cs:log:%s:%s:chunks", tenant, activationID)
}

// LogBytesKey counts cumulative log bytes written for an activation so that
// AppendLogChunk can enforce the per-activation log cap without scanning every
// chunk. The value is an integer-valued string updated via INCRBY.
func LogBytesKey(tenant, activationID string) string {
	return fmt.Sprintf("cs:log:%s:%s:bytes", tenant, activationID)
}

// LogTruncatedKey records that the per-activation log cap was hit. It is set
// once when the truncation sentinel is appended so that read paths can surface
// X-CS-Truncated: logs without scanning the chunk index.
func LogTruncatedKey(tenant, activationID string) string {
	return fmt.Sprintf("cs:log:%s:%s:truncated", tenant, activationID)
}

// ActivationTombstoneKey holds a long-lived marker (TTL >> activation TTL)
// that lets GetActivation distinguish a key that has expired (return
// CS_ACTIVATION_TTL_EXPIRED / 410) from one that never existed (404). The
// value is the activation start/end timestamp; presence is what matters.
func ActivationTombstoneKey(tenant, activationID string) string {
	return fmt.Sprintf("cs:act:%s:%s:tomb", tenant, activationID)
}

func ScheduleMetaKey(tenant, namespace, name string) string {
	return fmt.Sprintf("cs:schedule:%s:%s:%s:meta", tenant, namespace, name)
}

func ScheduleIndexKey(tenant, namespace string) string {
	return fmt.Sprintf("cs:schedule:%s:%s:index", tenant, namespace)
}

func ScheduleStateKey(tenant, namespace, name string) string {
	return fmt.Sprintf("cs:schedule:%s:%s:%s:state", tenant, namespace, name)
}

func ScheduleInflightKey(tenant, namespace, name string) string {
	return fmt.Sprintf("cs:schedule:%s:%s:%s:inflight", tenant, namespace, name)
}

func WorkerBindingMetaKey(tenant, namespace, name string) string {
	return fmt.Sprintf("cs:cadence:%s:%s:worker:%s:meta", tenant, namespace, name)
}

func WorkerBindingIndexKey(tenant, namespace string) string {
	return fmt.Sprintf("cs:cadence:%s:%s:workers:index", tenant, namespace)
}

func CadenceTaskKey(tenant, namespace, taskTokenHash string) string {
	return fmt.Sprintf("cs:cadence:%s:%s:task:%s", tenant, namespace, taskTokenHash)
}

// SubscriptionMetaKey holds the serialized SubscriptionBinding record. The
// (tenant, namespace, name) tuple is the documented uniqueness boundary for
// codeQ subscription triggers (see docs/07-codeq-protocol.md).
func SubscriptionMetaKey(tenant, namespace, name string) string {
	return fmt.Sprintf("cs:subscription:%s:%s:%s:meta", tenant, namespace, name)
}

// SubscriptionIndexKey is the per-namespace SET of subscription names. It
// supports the LIST API surface and lets a background worker iterate all
// bindings for a tenant without scanning the keyspace.
func SubscriptionIndexKey(tenant, namespace string) string {
	return fmt.Sprintf("cs:subscription:%s:%s:index", tenant, namespace)
}
