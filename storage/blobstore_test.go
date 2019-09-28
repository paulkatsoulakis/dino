package storage_test

import (
	"testing"

	"github.com/nicolagi/dino/message"
	"github.com/nicolagi/dino/storage"
	"github.com/stretchr/testify/assert"
)

func TestBlobStore(t *testing.T) {
	store := storage.NewBlobStore(storage.NewInMemoryStore())
	t.Run("same value, same key", func(t *testing.T) {
		value := message.RandomBytes()
		key1, err1 := store.Put(value)
		key2, err2 := store.Put(value)
		assert.Nil(t, err1)
		assert.Nil(t, err2)
		assert.Len(t, key1, 20)
		assert.Len(t, key2, 20)
		assert.Equal(t, key1, key2)
	})
	t.Run("different values, different keys", func(t *testing.T) {
		value1 := message.RandomBytes()
		value2 := message.RandomBytes()
		key1, err1 := store.Put(value1)
		key2, err2 := store.Put(value2)
		assert.Nil(t, err1)
		assert.Nil(t, err2)
		assert.Len(t, key1, 20)
		assert.Len(t, key2, 20)
		assert.NotEqual(t, key1, key2)
	})
	t.Run("what you put is what you get", func(t *testing.T) {
		before := message.RandomBytes()
		key, err := store.Put(before)
		assert.Nil(t, err)
		after, err := store.Get(key)
		assert.Nil(t, err)
		assert.Equal(t, before, after)
	})
}
