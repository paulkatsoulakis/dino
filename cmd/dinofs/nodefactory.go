package main

import (
	"crypto/rand"
	"fmt"
	"time"

	"github.com/nicolagi/dino/storage"
)

type dinoNodeFactory struct {
	root     *dinoNode
	inogen   *inodeNumbersGenerator
	metadata storage.VersionedStore
	blobs    *storage.BlobStoreWrapper
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
	addKnown(&node)
	return &node, nil
}
