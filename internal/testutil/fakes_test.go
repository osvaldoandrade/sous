package testutil

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/osvaldoandrade/sous/internal/api"
)

var contextType = reflect.TypeOf((*context.Context)(nil)).Elem()

func TestFakePersistence_DefaultBehavior(t *testing.T) {
	f := &FakePersistence{}

	ctx := context.Background()
	if err := f.Close(); err != nil {
		t.Fatalf("Close default returned error: %v", err)
	}
	if err := f.Ping(ctx); err != nil {
		t.Fatalf("Ping default returned error: %v", err)
	}

	if _, err := f.GetFunction(ctx, "t", "n", "fn"); err == nil {
		t.Fatal("GetFunction should return not implemented by default")
	}
	if _, err := f.GetDraft(ctx, "t", "n", "fn", "drf"); err == nil {
		t.Fatal("GetDraft should return not implemented by default")
	}
	if _, _, err := f.GetVersion(ctx, "t", "n", "fn", 1); err == nil {
		t.Fatal("GetVersion should return not implemented by default")
	}
	if _, err := f.GetLatestVersion(ctx, "t", "n", "fn"); err == nil {
		t.Fatal("GetLatestVersion should return validation error by default")
	}
	if _, err := f.GetAlias(ctx, "t", "n", "fn", "a"); err == nil {
		t.Fatal("GetAlias should return not implemented by default")
	}
	if _, err := f.GetActivation(ctx, "t", "act"); err == nil {
		t.Fatal("GetActivation should return not implemented by default")
	}
	if _, err := f.GetResultByRequestID(ctx, "t", "req"); err == nil {
		t.Fatal("GetResultByRequestID should return not implemented by default")
	}
	if _, err := f.GetSchedule(ctx, "t", "n", "s"); err == nil {
		t.Fatal("GetSchedule should return not implemented by default")
	}
	if _, err := f.GetWorkerBinding(ctx, "t", "n", "w"); err == nil {
		t.Fatal("GetWorkerBinding should return not implemented by default")
	}

	v, err := f.ResolveVersion(ctx, "t", "n", "fn", "", 9)
	if err != nil || v != 9 {
		t.Fatalf("ResolveVersion(version) got (%d, %v), want (9, nil)", v, err)
	}
	v, err = f.ResolveVersion(ctx, "t", "n", "fn", "prod", 0)
	if err != nil || v != 1 {
		t.Fatalf("ResolveVersion(alias default) got (%d, %v), want (1, nil)", v, err)
	}

	// Exercise all default method branches.
	invokeAllMethods(t, f, nil)
}

func TestFakePersistence_FunctionDelegationForAllMethods(t *testing.T) {
	f := &FakePersistence{}
	calls := setAllFunctionFields(t, f)

	invokeAllMethods(t, f, nil)

	for field, count := range calls {
		if count == 0 {
			t.Fatalf("function field %s was not invoked", field)
		}
	}
}

func TestFakeMessaging_DefaultBehavior(t *testing.T) {
	f := &FakeMessaging{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := f.Close(); err != nil {
		t.Fatalf("Close default returned error: %v", err)
	}
	if err := f.Publish(context.Background(), "topic", "tenant", "Type", map[string]any{"x": 1}); err != nil {
		t.Fatalf("Publish default returned error: %v", err)
	}
	if _, err := f.WaitForResult(context.Background(), "req_1"); err == nil || !strings.Contains(err.Error(), "not implemented") {
		t.Fatalf("WaitForResult default should return not implemented error, got: %v", err)
	}

	if err := f.ConsumeInvocations(ctx, "g", nil); err != nil {
		t.Fatalf("ConsumeInvocations default returned error: %v", err)
	}
	if err := f.ConsumeResults(ctx, "g", nil); err != nil {
		t.Fatalf("ConsumeResults default returned error: %v", err)
	}
	if err := f.ConsumeTopic(ctx, "t", "g", nil); err != nil {
		t.Fatalf("ConsumeTopic default returned error: %v", err)
	}

	// Exercise all default method branches.
	overrides := map[string]func(paramIdx int, typ reflect.Type) (reflect.Value, bool){
		"ConsumeInvocations": func(paramIdx int, typ reflect.Type) (reflect.Value, bool) {
			if paramIdx == 0 && typ == contextType {
				return reflect.ValueOf(ctx), true
			}
			return reflect.Value{}, false
		},
		"ConsumeResults": func(paramIdx int, typ reflect.Type) (reflect.Value, bool) {
			if paramIdx == 0 && typ == contextType {
				return reflect.ValueOf(ctx), true
			}
			return reflect.Value{}, false
		},
		"ConsumeTopic": func(paramIdx int, typ reflect.Type) (reflect.Value, bool) {
			if paramIdx == 0 && typ == contextType {
				return reflect.ValueOf(ctx), true
			}
			return reflect.Value{}, false
		},
	}
	invokeAllMethods(t, f, overrides)
}

func TestFakeMessaging_FunctionDelegationForAllMethods(t *testing.T) {
	f := &FakeMessaging{}
	calls := setAllFunctionFields(t, f)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	overrides := map[string]func(paramIdx int, typ reflect.Type) (reflect.Value, bool){
		"ConsumeInvocations": func(paramIdx int, typ reflect.Type) (reflect.Value, bool) {
			if paramIdx == 0 && typ == contextType {
				return reflect.ValueOf(ctx), true
			}
			return reflect.Value{}, false
		},
		"ConsumeResults": func(paramIdx int, typ reflect.Type) (reflect.Value, bool) {
			if paramIdx == 0 && typ == contextType {
				return reflect.ValueOf(ctx), true
			}
			return reflect.Value{}, false
		},
		"ConsumeTopic": func(paramIdx int, typ reflect.Type) (reflect.Value, bool) {
			if paramIdx == 0 && typ == contextType {
				return reflect.ValueOf(ctx), true
			}
			return reflect.Value{}, false
		},
	}

	invokeAllMethods(t, f, overrides)

	for field, count := range calls {
		if count == 0 {
			t.Fatalf("function field %s was not invoked", field)
		}
	}
}

func setAllFunctionFields(t *testing.T, target any) map[string]int {
	t.Helper()

	v := reflect.ValueOf(target)
	if v.Kind() != reflect.Pointer || v.Elem().Kind() != reflect.Struct {
		t.Fatalf("target must be pointer to struct, got %T", target)
	}
	s := v.Elem()
	st := s.Type()
	calls := make(map[string]int)

	for i := 0; i < s.NumField(); i++ {
		sf := st.Field(i)
		if !strings.HasSuffix(sf.Name, "Fn") {
			continue
		}
		fv := s.Field(i)
		if fv.Kind() != reflect.Func || !fv.CanSet() {
			continue
		}

		fieldName := sf.Name
		fnType := fv.Type()
		fv.Set(reflect.MakeFunc(fnType, func(args []reflect.Value) []reflect.Value {
			_ = args
			calls[fieldName]++
			out := make([]reflect.Value, fnType.NumOut())
			for j := 0; j < fnType.NumOut(); j++ {
				// Return sensible defaults for common primitive outputs.
				switch fnType.Out(j).Kind() {
				case reflect.Bool:
					out[j] = reflect.ValueOf(true)
				case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
					out[j] = reflect.ValueOf(int64(1)).Convert(fnType.Out(j))
				default:
					out[j] = reflect.Zero(fnType.Out(j))
				}
			}
			return out
		}))
	}
	return calls
}

func invokeAllMethods(t *testing.T, target any, overrides map[string]func(paramIdx int, typ reflect.Type) (reflect.Value, bool)) {
	t.Helper()

	rv := reflect.ValueOf(target)
	rt := rv.Type()

	for i := 0; i < rt.NumMethod(); i++ {
		m := rt.Method(i)
		args := []reflect.Value{rv}

		for paramIdx := 0; paramIdx < m.Type.NumIn()-1; paramIdx++ {
			typ := m.Type.In(paramIdx + 1)
			if ov, ok := overrides[m.Name]; ok {
				if val, hit := ov(paramIdx, typ); hit {
					args = append(args, val)
					continue
				}
			}
			args = append(args, defaultArgValue(typ))
		}

		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("method %s panicked: %v", m.Name, r)
				}
			}()
			_ = m.Func.Call(args)
		}()
	}
}

func defaultArgValue(typ reflect.Type) reflect.Value {
	if typ == contextType {
		return reflect.ValueOf(context.Background())
	}
	if typ.Kind() == reflect.Func {
		return reflect.MakeFunc(typ, func([]reflect.Value) []reflect.Value {
			out := make([]reflect.Value, typ.NumOut())
			for i := 0; i < typ.NumOut(); i++ {
				out[i] = reflect.Zero(typ.Out(i))
			}
			return out
		})
	}
	if typ == reflect.TypeOf(time.Duration(0)) {
		return reflect.ValueOf(250 * time.Millisecond)
	}
	return reflect.Zero(typ)
}

func TestFakeMessagingWaitForResultCustomError(t *testing.T) {
	want := errors.New("boom")
	f := &FakeMessaging{
		WaitForResultFn: func(ctx context.Context, requestID string) (api.InvocationResult, error) {
			_ = ctx
			_ = requestID
			return api.InvocationResult{}, want
		},
	}
	_, err := f.WaitForResult(context.Background(), "req_1")
	if !errors.Is(err, want) {
		t.Fatalf("WaitForResult error = %v, want %v", err, want)
	}
}
