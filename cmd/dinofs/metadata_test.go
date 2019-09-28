package main

import (
	"math/rand"
	"testing"
	"time"

	"github.com/nicolagi/dino/message"

	"github.com/nicolagi/dino/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNodeSerialization(t *testing.T) {
	g := newInodeNumbersGenerator()
	go g.start()
	defer g.stop()
	rand.Seed(time.Now().UnixNano())
	store := storage.NewInMemoryStore()
	versioned := storage.NewVersionedWrapper(store)
	factory := &dinoNodeFactory{inogen: g, metadata: versioned}
	for i := 0; i < 100; i++ {
		before := randomNode(t, factory)
		err := before.saveMetadata()
		require.Nil(t, err)
		after, err := factory.allocNode()
		require.Nil(t, err)
		err = after.loadMetadata(before.key)
		require.Nil(t, err)
		assert.Equal(t, before.user, after.user)
		assert.Equal(t, before.group, after.group)
		assert.Equal(t, before.mode, after.mode)
		assert.Equal(t, before.time.UnixNano(), after.time.UnixNano())
		assert.Equal(t, before.version, after.version)
		assert.EqualValues(t, before.key, after.key)
		assert.EqualValues(t, before.contentKey, after.contentKey)
	}
}

func randomNode(t *testing.T, factory *dinoNodeFactory) *dinoNode {
	node, err := factory.allocNode()
	require.Nil(t, err)
	node.user = rand.Uint32()
	node.group = rand.Uint32()
	node.mode = rand.Uint32()
	node.time = time.Unix(rand.Int63(), rand.Int63())
	keyLen := rand.Intn(10)
	node.contentKey = make([]byte, keyLen)
	rand.Read(node.contentKey)
	node.version = rand.Uint64()
	node.xattrs = make(map[string][]byte)
	nxattrs := rand.Intn(4)
	for ; nxattrs > 0; nxattrs-- {
		node.xattrs[message.RandomString()] = message.RandomBytes()
	}
	return node
}
