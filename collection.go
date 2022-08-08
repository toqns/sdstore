package sdstore

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"

	"github.com/google/go-cmp/cmp"
)

// IndexedValueNotUniqueError is an error indicating that the value of an indexed field is not unique.
type IndexedValueNotUniqueError struct {
	Field string
}

// Error implements the Error interface for IndexedValueNotUniqueError
func (err *IndexedValueNotUniqueError) Error() string {
	return fmt.Sprintf("%q is not unique", err.Field)
}

var (
	// ErrNotInitialized is an error returned when a user attempts to do an action on a Collection that
	// hasn't been initialized.
	ErrNotInitialized = errors.New("collection is not initialized (run Init() to initialize)")

	// ErrAlreadyInitialized is an error returned whn a users attempts to initialize an already initialized
	// collection.
	ErrAlreadyInitialized = errors.New("collection is already initialized")

	// ErrNotIDNotUnique is an error returned when a user attempts to Create a record that already exists.
	ErrNotIDNotUnique = errors.New("id is not unique")

	// ErrNotFound is an error returned when a record is not found.
	ErrNotFound = errors.New("not found")

	// ErrInvalidRecordType is an error returned when record data isn't of the expected type.
	ErrInvalidRecordType = errors.New("record should be struct or pointer to struct")
)

const (
	defaultFilePerm fs.FileMode = 0600
	defaultDirPerm  fs.FileMode = 0700
)

// Collection provides functionality to work with records,
// which are stored as plain files.
type Collection struct {
	mu          sync.RWMutex
	initialized bool
	record      reflect.Type
	Path        string
	Name        string
	Encoder     Encoder
	Decoder     Decoder
	Indexing    struct {
		Fields  []string
		Indexes map[string]string
	}
	FilePerm fs.FileMode
	DirPerm  fs.FileMode
}

// CollectionOption is an option for the setup of a Collection.
type CollectionOption func(*Collection)

// WithIndexedFields is an option to set which struct fields are to be indexed.
func WithIndexedFields(fields ...string) CollectionOption {
	return func(c *Collection) {
		c.Indexing.Fields = fields
	}
}

// withEncoding is an option to set Collection's encoder and decoder.
func withEncoding(e Encoder, d Decoder) CollectionOption {
	return func(c *Collection) {
		c.Encoder = e
		c.Decoder = d
	}
}

func withDirPerms(perms fs.FileMode) CollectionOption {
	return func(c *Collection) {
		c.DirPerm = perms
	}

}

// WithCollectionPerms is an option to set the Collection's permissions for the
// working directorry and record files.
func WithCollectionPerms(perms fs.FileMode) CollectionOption {
	return func(c *Collection) {
		c.FilePerm = perms
	}
}

// NewCollection returns an initialized Collection with json encoding as defauly.
// name is the collection's name, path is the directory path for the collection.
// Additionally one or more CollectionOptions can be provided.
func newCollection(name string, path string, record any, opts ...CollectionOption) *Collection {
	c := Collection{
		Path: path,
		Name: name,
		Indexing: struct {
			Fields  []string
			Indexes map[string]string
		}{
			Indexes: make(map[string]string),
		},
		FilePerm: defaultFilePerm,
		DirPerm:  defaultDirPerm,
		record:   reflect.TypeOf(record),
	}

	withEncoding(EncodeFunc(json.Marshal), DecodeFunc(json.Unmarshal))(&c)
	for _, opt := range opts {
		opt(&c)
	}

	return &c
}

// filePerms returns the file and directory permissions.
func (c *Collection) filePerms() (fs.FileMode, fs.FileMode) {
	filePerm := c.FilePerm
	if filePerm == 0 {
		filePerm = defaultFilePerm
	}

	dirPerm := c.DirPerm
	if dirPerm == 0 {
		dirPerm = defaultDirPerm
	}

	return filePerm, dirPerm
}

// fullpath returns the full path for the collection.
func (c *Collection) fullpath() string {
	return filepath.Join(c.Path, c.Name)
}

// filepath returns a composed full file path for a record or index
// if index is set true.
func (c *Collection) filepath(name string, index bool) string {
	ext := ".sds"
	if index {
		ext = ".sdx"
	}
	return filepath.Join(c.Path, c.Name, name+ext)
}

// save stores the provided data to disk under the provided filename.
func (c *Collection) save(filename string, data []byte) error {
	if !c.initialized {
		return ErrNotInitialized
	}

	return os.WriteFile(filename, data, 0600)

}

// load returns the content of the provided filename as a slice of bytes.
func (c *Collection) load(filename string) ([]byte, error) {
	return os.ReadFile(filename)
}

// exists returns true if the provided path exists.
func (c *Collection) exists(id string) bool {
	_, err := os.Stat(c.filepath(id, false))
	return !os.IsNotExist(err)
}

// existsIndexed returns true if an index exists for the provided field/value combination.
func (c *Collection) existsIndexed(field string, value any) bool {
	_, ok := c.Indexing.Indexes[key(field, value)]
	return ok
}

// saveIndexes saves the Collection's indexes to an index file.
func (c *Collection) saveIndexes() error {
	// Encode indexes.
	b, err := c.Encoder.Encode(c.Indexing)
	if err != nil {
		return fmt.Errorf("encoding indexes: %w", err)
	}

	// Store to disk.
	if err := c.save(c.filepath(c.Name, true), b); err != nil {
		return fmt.Errorf("saving index: %w", err)
	}

	return nil
}

// loadIndexes loads the Collection's indexes from the index file.
func (c *Collection) loadIndexes() error {
	b, err := c.load(c.filepath(c.Name, true))
	if err != nil {
		return fmt.Errorf("loading index: %w", err)
	}

	if err := c.Decoder.Decode(b, &c.Indexing); err != nil {
		return fmt.Errorf("decoding indexes: %w", err)
	}

	return nil
}

// setIndex stores the id for the provided field/value combination in memory and to disk.
func (c *Collection) setIndex(field string, value any, id string) error {
	c.Indexing.Indexes[key(field, value)] = id
	return c.saveIndexes()
}

func (c *Collection) recreateIndexes() error {
	newIndexes := make(map[string]string)

	if err := filepath.Walk(c.fullpath(), func(path string, info fs.FileInfo, _ error) error {
		// Skip directories.
		if info.IsDir() {
			return nil
		}

		// Skip if the file is not a record.
		if !strings.HasSuffix(path, ".sds") {
			return nil
		}

		// Load and decode the record file.
		b, err := c.load(path)
		if err != nil {
			return err
		}

		rec := reflect.New(c.record).Interface()
		if err := c.Decoder.Decode(b, &rec); err != nil {
			// TODO: Consider returning an error.
			return nil
		}

		for _, fld := range c.Indexing.Fields {
			val := getFieldValue(rec, fld)
			if val == nil {
				return nil
			}

			id := reflect.ValueOf(rec).Field(0).String()
			if id == "" {
				// TODO: Consider returning an error.
				return nil
			}

			newIndexes[key(fld, val)] = id
		}

		c.Indexing.Indexes = newIndexes
		return nil
	}); err != nil {
		return err
	}

	return nil

}

// Init will initialize a Collection.
// Initialization consists of ensuring  the collections file path exists and
// loading and processing of the index file if it exists.
func (c *Collection) Init() (*Collection, error) {
	if c.initialized {
		return nil, ErrAlreadyInitialized
	}

	// Ensure that an encoder and decoder are set.
	if c.Encoder == nil {
		return nil, fmt.Errorf("nil encoder")
	}
	if c.Decoder == nil {
		return nil, fmt.Errorf("nil decoder")
	}

	// Ensure the destination directory exists.
	_, dirPerm := c.filePerms()
	os.MkdirAll(c.fullpath(), dirPerm)

	c.mu.Lock()
	defer c.mu.Unlock()

	// IndexedFields by current settings.
	indexedFields := c.Indexing.Fields

	// Load the index file contents. Continue if there's no index file.
	if err := c.loadIndexes(); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	// Recreate the indexes if the indexed fields from the load and settings differ.
	if diff := cmp.Diff(indexedFields, c.Indexing.Fields); diff != "" {
		if err := c.recreateIndexes(); err != nil {
			return nil, fmt.Errorf("reindexing: %w", err)
		}
	}

	c.initialized = true
	return c, nil
}

// Create encodes and stores the provided record to disk.
func (c *Collection) Create(id string, data any) error {
	if !c.initialized {
		return ErrNotInitialized
	}

	// Ensure that data is in a workable format.
	if !isStruct(data) && !isPointerToStruct(data) {
		return ErrInvalidRecordType
	}

	// IDs must be unique. Fail if the id already exists.
	if c.exists(id) {
		return ErrNotIDNotUnique
	}

	b, err := c.Encoder.Encode(data)
	if err != nil {
		return fmt.Errorf("encoding data: %w", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Loop over all fields that should be indexed
	for _, fld := range c.Indexing.Fields {
		// Get the value from the field.
		// Continue if the field doesn't exist.
		// TODO: Consider to returning error.
		v := getFieldValue(data, fld)
		if v == nil {
			continue
		}

		// Return an error if the field/value combination is not unique.
		if c.existsIndexed(fld, v) {
			err := IndexedValueNotUniqueError{Field: fld}
			return &err
		}

		// Store the index data.
		c.setIndex(fld, v, id)
	}

	// Store the record to disk.
	if err := c.save(c.filepath(id, false), b); err != nil {
		return fmt.Errorf("saving record: %w", err)
	}

	return nil
}

// Query returns a slice of data based on the result of the filter function.
// The filter function uses the type as set in Init.
func (c *Collection) Query(f func(any) bool) (res []any, err error) {
	if !c.initialized {
		return nil, ErrNotInitialized
	}

	// Loop over the directory.
	if err := filepath.Walk(c.fullpath(), func(path string, info fs.FileInfo, _ error) error {
		// Skip directories.
		if info.IsDir() {
			return nil
		}

		// Skip if the file is not a record.
		if !strings.HasSuffix(path, ".sds") {
			return nil
		}

		o := reflect.New(c.record).Interface()
		// Load and decode the record file.
		b, err := c.load(path)
		if err != nil {
			return err
		}
		if err := c.Decoder.Decode(b, &o); err != nil {
			return err
		}

		// Run the filter and append to result if result is positive.
		if f(o) {
			res = append(res, o)
		}

		return nil
	}); err != nil {
		return nil, err
	}

	return res, nil
}

func (c *Collection) QueryPaginated(f func(any) bool, page int, rows int) (res []any, pages int, err error) {
	if !c.initialized {
		return nil, 0, ErrNotInitialized
	}

	recs, err := c.Query(f)
	if err != nil {
		return res, 0, err
	}
	// Return everything if page and row are 0.
	if page == 0 && rows == 0 {
		return recs, 0, nil
	}

	// Check the pagination request.
	count := len(recs)
	pages = count / rows
	if count%rows != 0 {
		pages++
	}

	// If the requested page is larger than the pages we have
	// then return the last page.
	if page > pages {
		page = pages
	}

	// Calculate what to return.
	from := (page * rows) - rows
	to := (page * rows)
	if to > count {
		to = count
	}

	return recs[from:to], pages, nil
}

// Get receives a record from disk by the provided ID
// and will decode the result to dest.
//
// dest should be a pointer to a struct.
func (c *Collection) Get(id string, dest any) error {
	if !c.initialized {
		return ErrNotInitialized
	}

	// Ensure that dest is in a workable format.
	if !isStruct(dest) && !isPointerToStruct(dest) {
		return ErrInvalidRecordType
	}

	// Return an error if the record doesn't exist.
	if !c.exists(id) {
		return ErrNotFound
	}

	// Load the record from disk and decode the file's contents.
	b, err := c.load(c.filepath(id, false))
	if err != nil {
		return fmt.Errorf("loading record: %w", err)
	}
	if err := c.Decoder.Decode(b, dest); err != nil {
		return fmt.Errorf("decoding data: %w", err)
	}

	return nil
}

// GetIndexed receives a record from disk through the index of field/v
// and will decode the result to dest.
//
// dest should be a pointer to a struct.
func (c *Collection) GetIndexed(field string, v string, dest any) error {
	if !c.initialized {
		return ErrNotInitialized
	}

	// Ensure that dest is in a workable format.
	if !isStruct(dest) && !isPointerToStruct(dest) {
		return ErrInvalidRecordType
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Retrieve the id from the index. Return ErrNotFound is it doesn't exist.
	id, ok := c.Indexing.Indexes[key(field, v)]
	if !ok {
		return ErrNotFound
	}

	// Load the record from file and decode contents.
	b, err := c.load(c.filepath(id, false))
	if err != nil {
		return fmt.Errorf("loading record: %w", err)
	}
	if err := c.Decoder.Decode(b, dest); err != nil {
		return fmt.Errorf("decoding data: %w", err)
	}

	return nil
}

// Update stores an updated record to disk.
func (c *Collection) Update(id string, data any) error {
	if !c.initialized {
		return ErrNotInitialized
	}

	// Ensure that data is in a workable format.
	if !isStruct(data) && !isPointerToStruct(data) {
		return ErrInvalidRecordType
	}

	// Get old data or return error if doesn't exist.
	var oldRec any
	{
		b, err := c.load(c.filepath(id, false))
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return ErrNotFound
			}
			return err
		}
		if err := c.Decoder.Decode(b, &oldRec); err != nil {
			return err
		}
	}

	// Encode data
	b, err := c.Encoder.Encode(data)
	if err != nil {
		return fmt.Errorf("encoding data: %w", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Update indexes.
	for _, fld := range c.Indexing.Fields {
		// Remove old value, if any, to prevent index polution.
		oldVals, oldValsOk := oldRec.(map[string]any)
		oldv, oldValOk := oldVals[fld]
		if oldValsOk && oldValOk {
			delete(c.Indexing.Indexes, key(fld, oldv))
		}

		// Set new value.
		v := getFieldValue(data, fld)
		if v == nil {
			continue
		}
		c.setIndex(fld, v, id)
	}

	// Store record to disk.
	if err := c.save(c.filepath(id, false), b); err != nil {
		return fmt.Errorf("saving record: %w", err)
	}

	return nil
}

// Delete removes a record from disk and indexes.
func (c *Collection) Delete(id string) error {
	if !c.initialized {
		return ErrNotInitialized
	}

	// Return an error if the record doesn't exist.
	if !c.exists(id) {
		return ErrNotFound
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	// Remove the physical file.
	if err := os.Remove(c.filepath(id, false)); err != nil {
		return fmt.Errorf("deleting record: %w", err)
	}

	// Remove from indexes.
	for k, v := range c.Indexing.Indexes {
		if v != id {
			continue
		}

		delete(c.Indexing.Indexes, k)
	}

	if err := c.saveIndexes(); err != nil {
		return fmt.Errorf("saving indexes: %w", err)
	}

	return nil
}
