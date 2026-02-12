package errors

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
)

type Code string

const (
	CSAuthnMissingToken   Code = "CS_AUTHN_MISSING_TOKEN"
	CSAuthnInvalidToken   Code = "CS_AUTHN_INVALID_TOKEN"
	CSAuthnExpiredToken   Code = "CS_AUTHN_EXPIRED_TOKEN"
	CSAuthzDenied         Code = "CS_AUTHZ_DENIED"
	CSAuthzRoleMissing    Code = "CS_AUTHZ_ROLE_MISSING"
	CSAuthzResourceMis    Code = "CS_AUTHZ_RESOURCE_MISMATCH"
	CSValidationFailed    Code = "CS_VALIDATION_FAILED"
	CSValidationManifest  Code = "CS_VALIDATION_MANIFEST_INVALID"
	CSValidationBundle    Code = "CS_VALIDATION_BUNDLE_TOO_LARGE"
	CSValidationName      Code = "CS_VALIDATION_NAME_INVALID"
	CSKVUnavailable       Code = "CS_KVROCKS_UNAVAILABLE"
	CSKVWriteFailed       Code = "CS_KVROCKS_WRITE_FAILED"
	CSKVReadFailed        Code = "CS_KVROCKS_READ_FAILED"
	CSKVCASFailed         Code = "CS_KVROCKS_CAS_FAILED"
	CSCodeQPublishFailed  Code = "CS_CODEQ_PUBLISH_FAILED"
	CSCodeQSubFailed      Code = "CS_CODEQ_SUBSCRIBE_FAILED"
	CSCodeQTimeout        Code = "CS_CODEQ_CORRELATION_TIMEOUT"
	CSRuntimeTimeout      Code = "CS_RUNTIME_TIMEOUT"
	CSRuntimeMemory       Code = "CS_RUNTIME_MEMORY_LIMIT"
	CSRuntimeException    Code = "CS_RUNTIME_EXCEPTION"
	CSRuntimeCapDenied    Code = "CS_RUNTIME_CAPABILITY_DENIED"
	CSCadencePollFailed   Code = "CS_CADENCE_POLL_FAILED"
	CSCadenceRespFailed   Code = "CS_CADENCE_RESPOND_FAILED"
	CSCadenceHBFailed     Code = "CS_CADENCE_HEARTBEAT_FAILED"
	CSSchedulerStateWrite Code = "CS_SCHEDULER_STATE_WRITE_FAILED"
)

type CSError struct {
	Code      Code
	Message   string
	RequestID string
	Cause     error
}

func (e *CSError) Error() string {
	if e == nil {
		return ""
	}
	if e.Cause != nil {
		return fmt.Sprintf("%s: %s: %v", e.Code, e.Message, e.Cause)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func (e *CSError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

func New(code Code, message string) *CSError {
	return &CSError{Code: code, Message: message}
}

func Wrap(code Code, message string, err error) *CSError {
	return &CSError{Code: code, Message: message, Cause: err}
}

func WithRequestID(err error, requestID string) error {
	var csErr *CSError
	if errors.As(err, &csErr) {
		clone := *csErr
		clone.RequestID = requestID
		return &clone
	}
	return err
}

func StatusCode(code Code) int {
	switch {
	case strings.HasPrefix(string(code), "CS_AUTHN_"):
		return http.StatusUnauthorized
	case strings.HasPrefix(string(code), "CS_AUTHZ_"):
		return http.StatusForbidden
	case strings.HasPrefix(string(code), "CS_VALIDATION_"):
		return http.StatusBadRequest
	case code == CSCodeQTimeout:
		return http.StatusGatewayTimeout
	case strings.HasSuffix(string(code), "_UNAVAILABLE"):
		return http.StatusServiceUnavailable
	default:
		return http.StatusInternalServerError
	}
}

type HTTPErrorEnvelope struct {
	Error struct {
		Code      Code   `json:"code"`
		Message   string `json:"message"`
		RequestID string `json:"request_id,omitempty"`
	} `json:"error"`
}

func Encode(err error, requestID string) (int, []byte) {
	var csErr *CSError
	if !errors.As(err, &csErr) {
		csErr = Wrap(CSValidationFailed, err.Error(), err)
	}
	if csErr.RequestID == "" {
		csErr.RequestID = requestID
	}
	env := HTTPErrorEnvelope{}
	env.Error.Code = csErr.Code
	env.Error.Message = csErr.Message
	env.Error.RequestID = csErr.RequestID
	b, marshalErr := json.Marshal(env)
	if marshalErr != nil {
		fallback := []byte(`{"error":{"code":"CS_VALIDATION_FAILED","message":"failed to encode error"}}`)
		return http.StatusInternalServerError, fallback
	}
	return StatusCode(csErr.Code), b
}

func WriteHTTP(w http.ResponseWriter, err error, requestID string) {
	status, body := Encode(err, requestID)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(body)
}
