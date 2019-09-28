package storage

import (
	"crypto/sha512"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
)

// DiskStore implements Store.
type DiskStore struct {
	dir string
}

func NewDiskStore(dir string) *DiskStore {
	return &DiskStore{dir: dir}
}

func (s *DiskStore) Put(key, value []byte) (err error) {
	valpath := s.pathFor(key)
	err = ioutil.WriteFile(valpath, value, 0600)
	if err == nil {
		return nil
	}
	if !os.IsNotExist(err) {
		return fmt.Errorf("could not write %q: %w", valpath, err)
	}
	if err = os.MkdirAll(filepath.Dir(valpath), 0700); err != nil {
		return fmt.Errorf("could not make dir for %q: %w", valpath, err)
	}
	return ioutil.WriteFile(valpath, value, 0600)
}

func (s *DiskStore) Get(key []byte) (value []byte, err error) {
	value, err = ioutil.ReadFile(s.pathFor(key))
	if os.IsNotExist(err) {
		err = fmt.Errorf("%x: %w", key, ErrNotFound)
	}
	return
}

func (s *DiskStore) pathFor(key []byte) string {
	// Prevent ENAMETOOLONG, while retaining low probability of clashes.
	if len(key) > sha512.Size {
		hash := sha512.Sum512(key)
		key = hash[:]
	}
	hex := fmt.Sprintf("%02x", key)
	return filepath.Join(s.dir, hex[:2], hex)
}
