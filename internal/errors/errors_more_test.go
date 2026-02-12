package errors

import (
	"encoding/json"
	stdErrors "errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestCSErrorErrorAndUnwrap(t *testing.T) {
	var nilErr *CSError
	if got := nilErr.Error(); got != "" {
		t.Fatalf("nil error string = %q, want empty", got)
	}
	if nilErr.Unwrap() != nil {
		t.Fatal("nil unwrap should return nil")
	}

	base := New(CSValidationFailed, "bad input")
	if !strings.Contains(base.Error(), "CS_VALIDATION_FAILED: bad input") {
		t.Fatalf("unexpected New error string: %s", base.Error())
	}

	cause := stdErrors.New("root cause")
	wrapped := Wrap(CSRuntimeException, "runtime failed", cause)
	if !stdErrors.Is(wrapped, cause) {
		t.Fatal("wrapped error should unwrap to cause")
	}
	if got := wrapped.Error(); !strings.Contains(got, "runtime failed") || !strings.Contains(got, "root cause") {
		t.Fatalf("unexpected Wrap error string: %s", got)
	}
}

func TestWithRequestID(t *testing.T) {
	base := Wrap(CSRuntimeException, "boom", stdErrors.New("x"))
	withID := WithRequestID(base, "req_123")

	csErr, ok := withID.(*CSError)
	if !ok {
		t.Fatalf("WithRequestID returned %T, want *CSError", withID)
	}
	if csErr.RequestID != "req_123" {
		t.Fatalf("RequestID = %q, want req_123", csErr.RequestID)
	}
	if base.RequestID != "" {
		t.Fatal("WithRequestID should not mutate original error instance")
	}

	plain := stdErrors.New("plain")
	if got := WithRequestID(plain, "req"); got != plain {
		t.Fatal("non-CSError must be returned unchanged")
	}
}

func TestEncodeAndWriteHTTP(t *testing.T) {
	status, body := Encode(New(CSAuthzDenied, "denied"), "req_1")
	if status != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", status)
	}
	var env HTTPErrorEnvelope
	if err := json.Unmarshal(body, &env); err != nil {
		t.Fatalf("failed to decode envelope: %v", err)
	}
	if env.Error.Code != CSAuthzDenied || env.Error.RequestID != "req_1" {
		t.Fatalf("unexpected envelope: %+v", env)
	}

	status, body = Encode(stdErrors.New("plain failure"), "req_2")
	if status != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 for wrapped plain error", status)
	}
	if err := json.Unmarshal(body, &env); err != nil {
		t.Fatalf("failed to decode envelope: %v", err)
	}
	if env.Error.Code != CSValidationFailed || env.Error.Message != "plain failure" || env.Error.RequestID != "req_2" {
		t.Fatalf("unexpected wrapped envelope: %+v", env)
	}

	w := httptest.NewRecorder()
	WriteHTTP(w, New(CSCodeQTimeout, "timed out"), "req_3")
	if w.Code != http.StatusGatewayTimeout {
		t.Fatalf("WriteHTTP status = %d, want 504", w.Code)
	}
	if got := w.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", got)
	}
	if err := json.Unmarshal(w.Body.Bytes(), &env); err != nil {
		t.Fatalf("WriteHTTP body is not valid JSON: %v", err)
	}
	if env.Error.RequestID != "req_3" {
		t.Fatalf("WriteHTTP request_id = %q, want req_3", env.Error.RequestID)
	}
}
