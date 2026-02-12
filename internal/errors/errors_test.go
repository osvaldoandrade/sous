package errors

import "testing"

func TestStatusCodeMappings(t *testing.T) {
	tests := []struct {
		code Code
		want int
	}{
		{CSAuthnMissingToken, 401},
		{CSAuthnInvalidToken, 401},
		{CSAuthnExpiredToken, 401},
		{CSAuthzDenied, 403},
		{CSAuthzRoleMissing, 403},
		{CSAuthzResourceMis, 403},
		{CSValidationFailed, 400},
		{CSValidationManifest, 400},
		{CSValidationBundle, 400},
		{CSValidationName, 400},
		{CSKVUnavailable, 503},
		{CSCodeQTimeout, 504},
		{CSRuntimeException, 500},
	}
	for _, tc := range tests {
		if got := StatusCode(tc.code); got != tc.want {
			t.Fatalf("code %s got %d want %d", tc.code, got, tc.want)
		}
	}
}
