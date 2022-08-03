package sdstore

import (
	"bytes"
	"encoding/gob"
)

// GobEncoder provides functionality for encoding/decoding Gob encoded data.
type GobEncoder struct{}

// Encode implements the Encoder interface for GobEncoder.
func (GobEncoder) Encode(data any) ([]byte, error) {
	buf := bytes.Buffer{}
	if err := gob.NewEncoder(&buf).Encode(data); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// Decode implements the Decoder interface for GobEncoder.
func (GobEncoder) Decode(b []byte, dest any) error {
	r := bytes.NewBuffer(b)
	return gob.NewDecoder(r).Decode(dest)
}

// Register implements the Registrar interface for GobEncododer.
// It's needed, because the gob package requires registration of
// custom objects.
func (e GobEncoder) Register(value any) {
	gob.Register(value)
}
