package sdstore

import (
	"bytes"
	"fmt"

	"github.com/ugorji/go/codec"
)

// CodecEncoder provides functionality for encoding/decoding Gob encoded data.
type CodecEncoder struct {
	handle codec.Handle
}

// NewCborEncoder returns a CodecEncoder using CBOR encoding.
func NewCborEncoder() CodecEncoder {
	return CodecEncoder{handle: &codec.CborHandle{}}
}

// NewMsgpackEncoder returns a CodecEncoder using Msgpack encoding.
func NewMsgpackEncoder() CodecEncoder {
	return CodecEncoder{handle: &codec.MsgpackHandle{}}
}

// NewBincEncoder returns a CodecEncoder using Binc encoding.
func NewBincEncoder() CodecEncoder {
	return CodecEncoder{handle: &codec.BincHandle{}}
}

// Encode implements the Encoder interface for CodecEncoder.
func (c CodecEncoder) Encode(data any) ([]byte, error) {
	if c.handle == nil {
		return nil, fmt.Errorf("nil handle")
	}

	buf := bytes.Buffer{}
	if err := codec.NewEncoder(&buf, c.handle).Encode(data); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// Decode implements the Decoder interface for CodecEncoder.
func (c CodecEncoder) Decode(b []byte, dest any) error {
	if c.handle == nil {
		return fmt.Errorf("nil handle")
	}

	r := bytes.NewBuffer(b)
	return codec.NewDecoder(r, c.handle).Decode(dest)
}
