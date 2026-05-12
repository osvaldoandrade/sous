package secrets

import (
	"encoding/json"
	"fmt"
)

// jsonField returns the string value at key in the top-level JSON object
// encoded by raw. Numeric / boolean fields are stringified so a driver can
// expose them through env vars without a second config knob. Returns
// ok=false when the document is well-formed JSON but the key is absent.
func jsonField(raw string, key string) (string, bool, error) {
	var doc map[string]any
	if err := json.Unmarshal([]byte(raw), &doc); err != nil {
		return "", false, fmt.Errorf("decode secret payload as JSON: %w", err)
	}
	v, ok := doc[key]
	if !ok {
		return "", false, nil
	}
	switch val := v.(type) {
	case string:
		return val, true, nil
	case bool:
		if val {
			return "true", true, nil
		}
		return "false", true, nil
	case float64:
		return fmt.Sprintf("%v", val), true, nil
	case nil:
		return "", true, nil
	default:
		b, err := json.Marshal(val)
		if err != nil {
			return "", false, fmt.Errorf("re-encode field %q: %w", key, err)
		}
		return string(b), true, nil
	}
}
