package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"

	"github.com/osvaldoandrade/sous/internal/api"
	"github.com/osvaldoandrade/sous/internal/authz"
	"github.com/osvaldoandrade/sous/internal/config"
	cserrors "github.com/osvaldoandrade/sous/internal/errors"
	"github.com/osvaldoandrade/sous/internal/observability"
	"github.com/osvaldoandrade/sous/internal/testutil"
)

func gatewayRequest(method, path, body string, params map[string]string, principal *authz.Principal) *http.Request {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	routeCtx := chi.NewRouteContext()
	for k, v := range params {
		routeCtx.URLParams.Add(k, v)
	}
	ctx := context.WithValue(req.Context(), chi.RouteCtxKey, routeCtx)
	ctx = observability.WithRequestID(ctx, "req_test")
	if principal != nil {
		ctx = authz.WithPrincipal(ctx, *principal)
	}
	return req.WithContext(ctx)
}

func decodeError(t *testing.T, body []byte) cserrors.HTTPErrorEnvelope {
	t.Helper()
	var env cserrors.HTTPErrorEnvelope
	if err := json.Unmarshal(body, &env); err != nil {
		t.Fatalf("invalid error response: %v body=%s", err, string(body))
	}
	return env
}

func TestHelpers(t *testing.T) {
	a := deriveActivationID("t", "f", "prod", "idem")
	b := deriveActivationID("t", "f", "prod", "idem")
	if a != b {
		t.Fatalf("deriveActivationID must be deterministic for idempotency key")
	}
	if deriveActivationID("t", "f", "prod", "") == "" {
		t.Fatal("deriveActivationID without key must return uuid")
	}

	headers := flattenHeaders(http.Header{"X-Test": {"a", "b"}, "Empty": {}})
	if headers["x-test"] != "a,b" {
		t.Fatalf("flattenHeaders result: %#v", headers)
	}
	if _, ok := headers["empty"]; ok {
		t.Fatalf("flattenHeaders should skip empty header values: %#v", headers)
	}

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("abcdef"))
	out, err := readBoundedBody(req, 6)
	if err != nil || string(out) != "abcdef" {
		t.Fatalf("readBoundedBody exact size failed: %v %q", err, string(out))
	}
	req = httptest.NewRequest(http.MethodPost, "/", strings.NewReader("abcdef"))
	if _, err := readBoundedBody(req, 5); err == nil {
		t.Fatal("readBoundedBody should fail when body is too large")
	}
	req = httptest.NewRequest(http.MethodPost, "/", strings.NewReader("ok"))
	out, err = readBoundedBody(req, 0)
	if err != nil || string(out) != "ok" {
		t.Fatalf("readBoundedBody default max failed: %v %q", err, string(out))
	}

	if got := applyDefaultLimit(0, 10); got != 10 {
		t.Fatalf("applyDefaultLimit default=%d", got)
	}
	if got := applyDefaultLimit(5, 10); got != 5 {
		t.Fatalf("applyDefaultLimit explicit=%d", got)
	}
	if got := estimateHeaderBytes(http.Header{"X-A": {"1"}, "X-B": {"2", "3"}}); got <= 0 {
		t.Fatalf("estimateHeaderBytes=%d", got)
	}
}

func TestInvokeHTTPAuthzFailures(t *testing.T) {
	s := &server{cfg: config.Config{}, store: &testutil.FakePersistence{}, broker: &testutil.FakeMessaging{}}

	w := httptest.NewRecorder()
	req := gatewayRequest(http.MethodPost, "/x", `{}`, map[string]string{
		"tenant": "t1", "namespace": "n1", "function": "f1", "ref": "prod",
	}, nil)
	s.invokeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("missing principal status = %d", w.Code)
	}

	principal := &authz.Principal{Sub: "u1", Tenant: "t1", Roles: []string{"x"}}
	w = httptest.NewRecorder()
	req = gatewayRequest(http.MethodPost, "/x", `{}`, map[string]string{
		"tenant": "t1", "namespace": "n1", "function": "f1", "ref": "prod",
	}, principal)
	s.invokeHTTP(w, req)
	if w.Code != http.StatusForbidden {
		t.Fatalf("action denied status = %d", w.Code)
	}
	env := decodeError(t, w.Body.Bytes())
	if env.Error.Code != cserrors.CSAuthzDenied {
		t.Fatalf("unexpected code: %+v", env)
	}

	principal = &authz.Principal{Sub: "u1", Tenant: "other", Roles: []string{"cs:function:invoke:http"}}
	w = httptest.NewRecorder()
	req = gatewayRequest(http.MethodPost, "/x", `{}`, map[string]string{
		"tenant": "t1", "namespace": "n1", "function": "f1", "ref": "prod",
	}, principal)
	s.invokeHTTP(w, req)
	if w.Code != http.StatusForbidden {
		t.Fatalf("tenant mismatch status = %d", w.Code)
	}
	env = decodeError(t, w.Body.Bytes())
	if env.Error.Code != cserrors.CSAuthzResourceMis {
		t.Fatalf("unexpected code: %+v", env)
	}
}

func TestInvokeHTTPSuccessAndErrorPaths(t *testing.T) {
	var capturedInvocation api.InvocationRequest
	persistence := &testutil.FakePersistence{
		ResolveVersionFn: func(ctx context.Context, tenant, namespace, function, alias string, version int64) (int64, error) {
			_ = ctx
			if alias == "bad" {
				return 0, cserrors.New(cserrors.CSValidationFailed, "bad alias")
			}
			return 3, nil
		},
		GetVersionFn: func(ctx context.Context, tenant, namespace, function string, version int64) (api.VersionRecord, []byte, error) {
			_ = ctx
			_ = tenant
			_ = namespace
			_ = function
			_ = version
			return api.VersionRecord{
				Config: api.VersionConfig{
					TimeoutMS: 50,
					Authz: api.VersionAuthz{
						InvokeHTTPRoles: []string{"role:invoke"},
					},
				},
			}, nil, nil
		},
	}
	messaging := &testutil.FakeMessaging{
		PublishInvocationFn: func(ctx context.Context, req api.InvocationRequest) error {
			_ = ctx
			capturedInvocation = req
			if req.Ref.Function != "f1" || req.Ref.Version != 3 {
				t.Fatalf("unexpected invocation: %+v", req)
			}
			return nil
		},
		WaitForResultFn: func(ctx context.Context, requestID string) (api.InvocationResult, error) {
			_ = ctx
			return api.InvocationResult{
				ActivationID: "act_1",
				RequestID:    requestID,
				Status:       "success",
				Result: &api.FunctionResponse{
					StatusCode:      201,
					Headers:         map[string]string{"x-test": "ok"},
					Body:            base64.StdEncoding.EncodeToString([]byte("hello")),
					IsBase64Encoded: true,
				},
			}, nil
		},
	}
	cfg := config.Config{}
	cfg.CSHTTPGateway.Limits.MaxBodyBytes = 1024
	s := &server{cfg: cfg, store: persistence, broker: messaging}

	principal := &authz.Principal{
		Sub:    "u1",
		Tenant: "t1",
		Roles:  []string{"cs:function:invoke:http", "role:invoke"},
	}

	w := httptest.NewRecorder()
	req := gatewayRequest(http.MethodPost, "/v1/web/t1/n1/f1/prod", `{"ok":true}`, map[string]string{
		"tenant": "t1", "namespace": "n1", "function": "f1", "ref": "prod",
	}, principal)
	req.Header.Set("Idempotency-Key", "idem-1")
	req.Header.Set("X-Custom", "value")
	s.invokeHTTP(w, req)
	if w.Code != 201 {
		t.Fatalf("status = %d body=%s", w.Code, w.Body.String())
	}
	if w.Header().Get("x-test") != "ok" {
		t.Fatalf("missing result header: %#v", w.Header())
	}
	if w.Body.String() != "hello" {
		t.Fatalf("unexpected decoded body: %q", w.Body.String())
	}
	wantActivation := deriveActivationID("t1", "f1", "prod", "idem-1")
	if capturedInvocation.ActivationID != wantActivation {
		t.Fatalf("activation id mismatch got=%s want=%s", capturedInvocation.ActivationID, wantActivation)
	}
	if capturedInvocation.Trigger.Type != "http" {
		t.Fatalf("trigger type mismatch: %+v", capturedInvocation.Trigger)
	}
	event, ok := capturedInvocation.Event.(map[string]any)
	if !ok {
		t.Fatalf("event type mismatch: %#v", capturedInvocation.Event)
	}
	if event["rawQueryString"] != "" || event["isBase64Encoded"] != true {
		t.Fatalf("event mapping mismatch: %+v", event)
	}
	if headers, ok := event["headers"].(map[string]string); !ok || headers["x-custom"] != "value" {
		t.Fatalf("event headers mismatch: %#v", event["headers"])
	}
	encodedBody, _ := event["body"].(string)
	decodedBody, err := base64.StdEncoding.DecodeString(encodedBody)
	if err != nil || string(decodedBody) != `{"ok":true}` {
		t.Fatalf("event body decode mismatch err=%v body=%q", err, string(decodedBody))
	}

	// ResolveVersion failure path.
	w = httptest.NewRecorder()
	req = gatewayRequest(http.MethodPost, "/v1/web/t1/n1/f1/bad", `{}`, map[string]string{
		"tenant": "t1", "namespace": "n1", "function": "f1", "ref": "bad",
	}, principal)
	s.invokeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected validation failure status, got %d body=%s", w.Code, w.Body.String())
	}

	// Broker returns result without payload -> runtime exception.
	s.broker = &testutil.FakeMessaging{
		PublishInvocationFn: func(context.Context, api.InvocationRequest) error { return nil },
		WaitForResultFn: func(ctx context.Context, requestID string) (api.InvocationResult, error) {
			_ = ctx
			return api.InvocationResult{
				RequestID: requestID,
				Status:    "error",
				Error:     &api.InvocationError{Message: "boom"},
			}, nil
		},
	}
	w = httptest.NewRecorder()
	req = gatewayRequest(http.MethodPost, "/v1/web/t1/n1/f1/prod", `{}`, map[string]string{
		"tenant": "t1", "namespace": "n1", "function": "f1", "ref": "prod",
	}, principal)
	s.invokeHTTP(w, req)
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected runtime error status, got %d body=%s", w.Code, w.Body.String())
	}
}

func TestServeReturnsListenError(t *testing.T) {
	cfg := config.Config{}
	cfg.Plugins.AuthN.Driver = "tikti"
	cfg.Plugins.AuthN.Tikti.IntrospectionURL = "http://127.0.0.1:1"
	cfg.CSHTTPGateway.HTTP.Addr = ":-1"
	s := &server{
		cfg:    cfg,
		store:  &testutil.FakePersistence{},
		broker: &testutil.FakeMessaging{},
	}

	if err := s.serve(); err == nil {
		t.Fatal("expected listen error for invalid address")
	}
}

func TestServeAuthNCreationErrorAndInvokeAdditionalBranches(t *testing.T) {
	// authn plugin missing branch.
	cfg := config.Config{}
	cfg.Plugins.AuthN.Driver = "missing"
	cfg.CSHTTPGateway.HTTP.Addr = ":-1"
	s := &server{
		cfg:    cfg,
		store:  &testutil.FakePersistence{},
		broker: &testutil.FakeMessaging{},
	}
	if err := s.serve(); err == nil {
		t.Fatal("expected authn plugin creation error")
	}

	// invokeHTTP extra branches.
	store := &testutil.FakePersistence{
		ResolveVersionFn: func(ctx context.Context, tenant, namespace, function, alias string, version int64) (int64, error) {
			_ = ctx
			if version > 0 {
				return version, nil
			}
			return 1, nil
		},
		GetVersionFn: func(ctx context.Context, tenant, namespace, function string, version int64) (api.VersionRecord, []byte, error) {
			_ = ctx
			return api.VersionRecord{
				Config: api.VersionConfig{
					TimeoutMS: 50,
					Authz: api.VersionAuthz{
						InvokeHTTPRoles: []string{"role:required"},
					},
				},
			}, nil, nil
		},
	}
	broker := &testutil.FakeMessaging{
		PublishInvocationFn: func(context.Context, api.InvocationRequest) error { return nil },
		WaitForResultFn: func(ctx context.Context, requestID string) (api.InvocationResult, error) {
			_ = ctx
			return api.InvocationResult{
				ActivationID: "a1",
				RequestID:    requestID,
				Status:       "success",
				Result: &api.FunctionResponse{
					StatusCode:      200,
					Body:            "%%%invalid-base64%%%",
					IsBase64Encoded: true,
				},
			}, nil
		},
	}
	cfg = config.Config{}
	cfg.CSHTTPGateway.Limits.MaxBodyBytes = 2
	s = &server{cfg: cfg, store: store, broker: broker}

	// role intersection denied
	w := httptest.NewRecorder()
	req := gatewayRequest(http.MethodPost, "/v1/web/t1/ns1/f1/2", "{}", map[string]string{
		"tenant": "t1", "namespace": "ns1", "function": "f1", "ref": "2",
	}, &authz.Principal{
		Sub:    "u1",
		Tenant: "t1",
		Roles:  []string{"cs:function:invoke:http", "role:other"},
	})
	s.invokeHTTP(w, req)
	if w.Code != http.StatusForbidden {
		t.Fatalf("expected role-missing forbidden status, got %d", w.Code)
	}

	// body too large branch
	store.GetVersionFn = func(context.Context, string, string, string, int64) (api.VersionRecord, []byte, error) {
		return api.VersionRecord{
			Config: api.VersionConfig{
				TimeoutMS: 50,
				Authz: api.VersionAuthz{
					InvokeHTTPRoles: []string{"role:required"},
				},
			},
		}, nil, nil
	}
	w = httptest.NewRecorder()
	req = gatewayRequest(http.MethodPost, "/v1/web/t1/ns1/f1/2", "12345", map[string]string{
		"tenant": "t1", "namespace": "ns1", "function": "f1", "ref": "2",
	}, &authz.Principal{
		Sub:    "u1",
		Tenant: "t1",
		Roles:  []string{"cs:function:invoke:http", "role:required"},
	})
	s.invokeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected body-too-large bad request, got %d", w.Code)
	}

	// query too large branch
	cfg.CSHTTPGateway.Limits.MaxBodyBytes = 1024
	cfg.CSHTTPGateway.Limits.MaxQueryBytes = 2
	s.cfg = cfg
	w = httptest.NewRecorder()
	req = gatewayRequest(http.MethodPost, "/v1/web/t1/ns1/f1/2?a=1234", "{}", map[string]string{
		"tenant": "t1", "namespace": "ns1", "function": "f1", "ref": "2",
	}, &authz.Principal{
		Sub:    "u1",
		Tenant: "t1",
		Roles:  []string{"cs:function:invoke:http", "role:required"},
	})
	s.invokeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected query-too-large bad request, got %d", w.Code)
	}
	env := decodeError(t, w.Body.Bytes())
	if env.Error.Code != cserrors.CSValidationFailed {
		t.Fatalf("query-too-large code=%s", env.Error.Code)
	}

	// headers too large branch
	cfg.CSHTTPGateway.Limits.MaxHeaderBytes = 4
	cfg.CSHTTPGateway.Limits.MaxQueryBytes = 1024
	s.cfg = cfg
	w = httptest.NewRecorder()
	req = gatewayRequest(http.MethodPost, "/v1/web/t1/ns1/f1/2", "{}", map[string]string{
		"tenant": "t1", "namespace": "ns1", "function": "f1", "ref": "2",
	}, &authz.Principal{
		Sub:    "u1",
		Tenant: "t1",
		Roles:  []string{"cs:function:invoke:http", "role:required"},
	})
	req.Header.Set("X-Large", "123456")
	s.invokeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected headers-too-large bad request, got %d", w.Code)
	}
	env = decodeError(t, w.Body.Bytes())
	if env.Error.Code != cserrors.CSValidationFailed {
		t.Fatalf("headers-too-large code=%s", env.Error.Code)
	}

	// invalid base64 in function result should fallback to raw body.
	cfg.CSHTTPGateway.Limits.MaxBodyBytes = 1024
	cfg.CSHTTPGateway.Limits.MaxHeaderBytes = 1024
	cfg.CSHTTPGateway.Limits.MaxQueryBytes = 1024
	s.cfg = cfg
	w = httptest.NewRecorder()
	req = gatewayRequest(http.MethodPost, "/v1/web/t1/ns1/f1/2", "{}", map[string]string{
		"tenant": "t1", "namespace": "ns1", "function": "f1", "ref": "2",
	}, &authz.Principal{
		Sub:    "u1",
		Tenant: "t1",
		Roles:  []string{"cs:function:invoke:http", "role:required"},
	})
	s.invokeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected success status, got %d", w.Code)
	}
	if w.Body.String() != "%%%invalid-base64%%%" {
		t.Fatalf("expected fallback raw body write, got %q", w.Body.String())
	}
}

func TestInvokeHTTPErrorContracts(t *testing.T) {
	t.Run("missing principal returns authn invalid token envelope", func(t *testing.T) {
		s := &server{cfg: config.Config{}, store: &testutil.FakePersistence{}, broker: &testutil.FakeMessaging{}}
		w := httptest.NewRecorder()
		req := gatewayRequest(http.MethodPost, "/x", `{}`, map[string]string{
			"tenant": "t1", "namespace": "n1", "function": "f1", "ref": "prod",
		}, nil)
		s.invokeHTTP(w, req)
		if w.Code != http.StatusUnauthorized {
			t.Fatalf("status=%d", w.Code)
		}
		env := decodeError(t, w.Body.Bytes())
		if env.Error.Code != cserrors.CSAuthnInvalidToken {
			t.Fatalf("code=%s", env.Error.Code)
		}
	})

	t.Run("wait for result timeout propagates codeq timeout", func(t *testing.T) {
		store := &testutil.FakePersistence{
			ResolveVersionFn: func(context.Context, string, string, string, string, int64) (int64, error) { return 1, nil },
			GetVersionFn: func(context.Context, string, string, string, int64) (api.VersionRecord, []byte, error) {
				return api.VersionRecord{
					Config: api.VersionConfig{
						TimeoutMS: 50,
						Authz: api.VersionAuthz{
							InvokeHTTPRoles: []string{"role:invoke"},
						},
					},
				}, nil, nil
			},
		}
		broker := &testutil.FakeMessaging{
			PublishInvocationFn: func(context.Context, api.InvocationRequest) error { return nil },
			WaitForResultFn: func(context.Context, string) (api.InvocationResult, error) {
				return api.InvocationResult{}, cserrors.New(cserrors.CSCodeQTimeout, "timeout")
			},
		}
		cfg := config.Config{}
		cfg.CSHTTPGateway.Limits.MaxBodyBytes = 1024
		s := &server{cfg: cfg, store: store, broker: broker}
		w := httptest.NewRecorder()
		req := gatewayRequest(http.MethodPost, "/v1/web/t1/ns1/f1/prod", "{}", map[string]string{
			"tenant": "t1", "namespace": "ns1", "function": "f1", "ref": "prod",
		}, &authz.Principal{
			Sub:    "u1",
			Tenant: "t1",
			Roles:  []string{"cs:function:invoke:http", "role:invoke"},
		})
		s.invokeHTTP(w, req)
		if w.Code != http.StatusGatewayTimeout {
			t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
		}
		env := decodeError(t, w.Body.Bytes())
		if env.Error.Code != cserrors.CSCodeQTimeout {
			t.Fatalf("code=%s", env.Error.Code)
		}
	})

	t.Run("missing function result envelope", func(t *testing.T) {
		store := &testutil.FakePersistence{
			ResolveVersionFn: func(context.Context, string, string, string, string, int64) (int64, error) { return 1, nil },
			GetVersionFn: func(context.Context, string, string, string, int64) (api.VersionRecord, []byte, error) {
				return api.VersionRecord{
					Config: api.VersionConfig{
						TimeoutMS: 50,
						Authz: api.VersionAuthz{
							InvokeHTTPRoles: []string{"role:invoke"},
						},
					},
				}, nil, nil
			},
		}
		broker := &testutil.FakeMessaging{
			PublishInvocationFn: func(context.Context, api.InvocationRequest) error { return nil },
			WaitForResultFn: func(context.Context, string) (api.InvocationResult, error) {
				return api.InvocationResult{
					Status: "error",
				}, nil
			},
		}
		cfg := config.Config{}
		cfg.CSHTTPGateway.Limits.MaxBodyBytes = 1024
		s := &server{cfg: cfg, store: store, broker: broker}
		w := httptest.NewRecorder()
		req := gatewayRequest(http.MethodPost, "/v1/web/t1/ns1/f1/prod", "{}", map[string]string{
			"tenant": "t1", "namespace": "ns1", "function": "f1", "ref": "prod",
		}, &authz.Principal{
			Sub:    "u1",
			Tenant: "t1",
			Roles:  []string{"cs:function:invoke:http", "role:invoke"},
		})
		s.invokeHTTP(w, req)
		if w.Code != http.StatusInternalServerError {
			t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
		}
		env := decodeError(t, w.Body.Bytes())
		if env.Error.Code != cserrors.CSRuntimeException {
			t.Fatalf("code=%s", env.Error.Code)
		}
		if env.Error.Message != "missing function result" {
			t.Fatalf("message=%q", env.Error.Message)
		}
	})
}
