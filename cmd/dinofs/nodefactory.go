package main

import (
	"crypto/rand"
	"fmt"
	"sync"
	"time"

	"github.com/nicolagi/dino/message"
	"github.com/nicolagi/dino/storage"
	log "github.com/sirupsen/logrus"
)

type dinoNodeFactory struct {
	root     *dinoNode
	inogen   *inodeNumbersGenerator
	metadata storage.VersionedStore
	blobs    *storage.BlobStoreWrapper

	mu    sync.Mutex
	known map[[nodeKeyLen]byte]*dinoNode
}

func (factory *dinoNodeFactory) allocNode() (*dinoNode, error) {
	var node dinoNode
	node.factory = factory
	node.time = time.Now()
	n, err := rand.Read(node.key[:])
	if err != nil {
		return nil, err
	}
	if n != nodeKeyLen {
		return nil, fmt.Errorf("could only read %d of %d random bytes", n, nodeKeyLen)
	}
	factory.addKnown(&node)
	return &node, nil
}

func (factory *dinoNodeFactory) existingNode(name string, key [nodeKeyLen]byte) *dinoNode {
	var node dinoNode
	node.factory = factory
	node.key = key
	node.name = name
	node.mode = modeNotLoaded
	factory.addKnown(&node)
	return &node
}

func (factory *dinoNodeFactory) addKnown(node *dinoNode) {
	factory.mu.Lock()
	defer factory.mu.Unlock()
	if factory.known == nil {
		factory.known = make(map[[nodeKeyLen]byte]*dinoNode)
	}
	if _, ok := factory.known[node.key]; ok {
		return
	}
	factory.known[node.key] = node
	logger := log.WithField("key", fmt.Sprintf("%.10x", node.key[:]))
	if node.name != "" {
		logger.WithField("name", node.name).Debug("Discovered node")
	} else {
		logger.Debug("Added node")
	}
}

func (factory *dinoNodeFactory) getKnown(key [nodeKeyLen]byte) *dinoNode {
	factory.mu.Lock()
	defer factory.mu.Unlock()
	if factory.known == nil {
		return nil
	}
	return factory.known[key]
}

func (factory *dinoNodeFactory) invalidateCache(mutation message.Message) {
	logger := log.WithFields(log.Fields{
		"op":       "import",
		"mutation": mutation.String(),
	})
	if len(mutation.Key()) != nodeKeyLen {
		logger.Debug("Not updating (not a metadata key)")
		return
	}
	var key [nodeKeyLen]byte
	copy(key[:], mutation.Key())
	node := factory.getKnown(key)
	if node == nil {
		logger.Debug("Not updating (unknown node)")
		return
	}
	node.mu.Lock()
	defer node.mu.Unlock()
	logger = logger.WithFields(log.Fields{
		"localVersion": node.version,
		"localName":    node.name,
	})
	if mutation.Version() <= node.version {
		logger.Debug("Not updating (stale update)")
		return
	}
	logger.Debug("Marking for update")
	node.shouldReloadMetadata = true
}
