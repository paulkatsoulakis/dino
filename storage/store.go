package storage

import (
	"encoding/binary"
	"errors"
	"sync"
)

// Store represents a key-value store.
type Store interface {
	Put(key, value []byte) (err error)

	// Get should return ErrNotFound if the key is not in the store.
	Get(key []byte) (value []byte, err error)
}

var (
	// ErrNotFound indicates a key is not in the store.
	ErrNotFound = errors.New("not found")
)

type VersionedStore interface {
	// Put should return ErrStalePut if the current version is not the version
	// passed as argument minus one. The client should have to prove that they've
	// seen the most current version before trying to update it.
	Put(version uint64, key []byte, value []byte) (err error)

	// Get should return ErrNotFound if the key is not in the store.
	Get(key []byte) (version uint64, value []byte, err error)
}

var (
	// ErrStalePut indicates that some client has not see the latest version of the
	// key-value pair being put. The client should get the current version, decide
	// if it still wants to do the put, and in that case do the put with the
	// correct version.
	ErrStalePut = errors.New("stale put")
)

// VersionedWrapper is a VersionedStore implementation wraping a given Store
// implementation. This is the quickest way of building a VersionedStore, but
// it's alos the slowest, as it serializes all calls to the underlying Store.
type VersionedWrapper struct {
	sync.Mutex
	delegate Store
}

func NewVersionedWrapper(delegate Store) *VersionedWrapper {
	return &VersionedWrapper{delegate: delegate}
}

// Put stores the given value at the given key, provided the passed version
// number is the current version number. If the put is successful, the version
// number is incremented by one.
func (s *VersionedWrapper) Put(version uint64, key []byte, value []byte) error {
	s.Lock()
	defer s.Unlock()
	curr, err := s.delegate.Get(key)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return err
	}
	if curr != nil {
		expectedVersion := binary.BigEndian.Uint64(curr[0:8]) + 1
		if version < expectedVersion {
			return ErrStalePut
		}
	}
	val := make([]byte, 8+len(value))
	binary.BigEndian.PutUint64(val, version)
	copy(val[8:], value)
	return s.delegate.Put(key, val)
}

// Get retrieves the value associated with a key and its version number.
func (s *VersionedWrapper) Get(key []byte) (version uint64, value []byte, err error) {
	s.Lock()
	defer s.Unlock()
	value, err = s.delegate.Get(key)
	if err == nil {
		version = binary.BigEndian.Uint64(value[:8])
		value = value[8:]
	}
	return
}
