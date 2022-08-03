package sdstore

// Encoder is an interface that store encoders have to implement.
type Encoder interface {
	Encode(any) ([]byte, error)
}

// Decoder is an interface that store decoders have to implement.
type Decoder interface {
	Decode([]byte, any) error
}

// Registrar is an interface that store encoders/decoders may implement to
// perform registration of data objects.
type Registrar interface {
	Register(any) error
}

// EncoderFunc is a function providing encoding functionality.
type EncoderFunc func(any) ([]byte, error)

// Encode implements the Encoder interface for EncoderFunc.
func (e EncoderFunc) Encode(data any) ([]byte, error) {
	return e(data)
}

// EncodeFunc wraps the provided function into an EncoderFunc.
func EncodeFunc(f func(any) ([]byte, error)) EncoderFunc {
	return EncoderFunc(f)
}

// DecoderFunc is a function providing decoding functionality.
type DecoderFunc func([]byte, any) error

// Decode implements the Decoder interface for DecoderFUnc.
func (d DecoderFunc) Decode(b []byte, dest any) error {
	return d(b, dest)
}

// DecodeFunc wraps the provided function into an DecoderFunc.
func DecodeFunc(f func([]byte, any) error) DecoderFunc {
	return DecoderFunc(f)
}
