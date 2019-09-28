package storage

import (
	"crypto/sha1"
)

type BlobStore interface {
	Get(key []byte) (value []byte, err error)
	Put(value []byte) (key []byte, err error)
}

// BlobStore wraps a Store to make sure content is never overwritten, by using
// as key for a value the SHA1 hash of the value. Even if there are concurrent
// writes for the same key, those would write the same contents (with very high
// probability).
type BlobStoreWrapper struct {
	delegate Store
}

func NewBlobStore(delegate Store) *BlobStoreWrapper {
	return &BlobStoreWrapper{
		delegate: delegate,
	}
}

func (s *BlobStoreWrapper) Put(value []byte) (key []byte, err error) {
	hash := sha1.Sum(value)
	key = hash[:]
	err = s.delegate.Put(key, value)
	return
}

func (s *BlobStoreWrapper) Get(key []byte) (value []byte, err error) {
	return s.delegate.Get(key)
}
