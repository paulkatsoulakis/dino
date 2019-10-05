package storage

import "errors"

type DynamoDBVersionedStore struct{}

func NewDynamoDBVersionedStore(profile, region, table string, opts ...Option) *DynamoDBVersionedStore {
	return &DynamoDBVersionedStore{}
}

func (s *DynamoDBVersionedStore) Start() {
}

func (s *DynamoDBVersionedStore) Stop() {
}

func (s *DynamoDBVersionedStore) Put(version uint64, key []byte, value []byte) (err error) {
	return errors.New("not implemented")
}

func (s *DynamoDBVersionedStore) Get(key []byte) (version uint64, value []byte, err error) {
	return 0, nil, errors.New("not implemented")
}
