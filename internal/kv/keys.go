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
