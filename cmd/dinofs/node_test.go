package main

import (
	"context"
	"errors"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fixedReply string

func (reply fixedReply) Get([]byte) (uint64, []byte, error) {
	if reply == "" {
		return 0, nil, nil
	}
	return 0, nil, errors.New(string(reply))
}

func (reply fixedReply) Put(uint64, []byte, []byte) error {
	if reply == "" {
		return nil
	}
	return errors.New(string(reply))
}

func TestNodeMetadataRollback(t *testing.T) {
	ko := func() {
		metadataStore = fixedReply("computer bought the farm")
	}
	ok := func() {
		metadataStore = fixedReply("")
	}
	t.Run("Setxattr", func(t *testing.T) {
		t.Run("rolls back additions", func(t *testing.T) {
			node, err := allocNode()
			require.Nil(t, err)
			ko()
			errno := node.Setxattr(context.Background(), "key", []byte("value"), 0)
			require.Equal(t, syscall.EIO, errno)
			assert.Len(t, node.xattrs, 0)
		})
		t.Run("rolls back updates", func(t *testing.T) {
			node, err := allocNode()
			require.Nil(t, err)
			ok()
			errno := node.Setxattr(context.Background(), "key", []byte("old value"), 0)
			require.EqualValues(t, 0, errno)
			ko()
			errno = node.Setxattr(context.Background(), "key", []byte("value"), 0)
			require.Equal(t, syscall.EIO, errno)
			assert.Len(t, node.xattrs, 1)
			assert.EqualValues(t, "old value", node.xattrs["key"])
		})
	})
}