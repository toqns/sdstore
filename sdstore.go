// Package sdstore provides a Simple Disk Store.
package sdstore

import (
	"encoding/json"
	"os"
	"path/filepath"
)

// SDStore is a key/value store
type SDStore struct {
	Path    string
	Name    string
	Encoder Encoder
	Decoder Decoder
	Perms   os.FileMode
}

// StoreOption is an option for the setup of a Store.
type StoreOption func(*SDStore)

// WithPerms is an option to set the store's permissions for the
// working directorry.
func WithPerms(p os.FileMode) StoreOption {
	return func(s *SDStore) {
		s.Perms = p
	}
}

// WithEncoding is an option to set store's encoder and decoder.
func WithEncoding(e Encoder, d Decoder) StoreOption {
	return func(s *SDStore) {
		s.Encoder = e
		s.Decoder = d
	}
}

// WithJSONEncoding is an option to set JSON encoding of records.
func WithJSONEncoding() StoreOption {
	return WithEncoding(EncodeFunc(json.Marshal), DecodeFunc(json.Unmarshal))
}

// WithCborEncoding is an option to set CBOR encoding of records.
func WithCborEncoding() StoreOption {
	e := NewCborEncoder()
	return WithEncoding(e, e)
}

// WithMsgpackEncoding is an option to set Msgpack encoding of records.
func WithMsgpackEncoding() StoreOption {
	e := NewMsgpackEncoder()
	return WithEncoding(e, e)
}

// WithBincEncoding is an option to set Binc encoding of records.
func WithBincEncoding() StoreOption {
	e := NewBincEncoder()
	return WithEncoding(e, e)
}

// New returns an initialized store with json encoding as default.
// name is the store's name, path is the directory path.
// Additionally one or more options can be provided.
func New(name string, path string, opts ...StoreOption) (*SDStore, error) {
	store := SDStore{
		Path:  path,
		Name:  name,
		Perms: defaultDirPerm,
	}

	// Set Gob encoding as the default and loop over options.
	WithCborEncoding()(&store)
	for _, opt := range opts {
		opt(&store)
	}

	os.MkdirAll(filepath.Join(path, name), store.Perms)

	return &store, nil
}

// Collection returns an initialized collection for this store.
func (s *SDStore) Collection(name string, record any, opts ...CollectionOption) (*Collection, error) {
	options := []CollectionOption{
		withDirPerms(s.Perms),
		withEncoding(s.Encoder, s.Decoder),
	}
	options = append(options, opts...)

	return newCollection(name, filepath.Join(s.Path, s.Name), record, options...).Init()
}
