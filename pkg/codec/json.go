package codec

import (
	"encoding/json"
	"fmt"
)

// Codec provides an interface for serialization and deserialization.
type Codec interface {
	// Serialize converts data into a byte slice.
	Serialize(data any) ([]byte, error)

	// Deserialize converts a byte slice into a map.
	Deserialize(body []byte) (map[string]any, error)

	// GetHTTPContentType returns the HTTP content type associated with the codec.
	GetHTTPContentType() string
}

type codecJson struct{}

// NewCodecJson creates a new JSON codec.
func NewCodecJson() Codec {
	return &codecJson{}
}

func (c *codecJson) GetHTTPContentType() string {
	return "application/json"
}

func (c *codecJson) Serialize(data any) ([]byte, error) {
	return json.Marshal(data)
}

func (c *codecJson) Deserialize(body []byte) (map[string]any, error) {
	if len(body) == 0 {
		return nil, fmt.Errorf("failed to deserialize json, empty body")
	}

	payload := make(map[string]any)

	err := json.Unmarshal(body, &payload)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize json [error: %v, payload: %v]", err.Error(), string(body))
	}

	return payload, nil
}
