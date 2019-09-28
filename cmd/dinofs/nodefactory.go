package main

import (
	"crypto/rand"
	"fmt"
	"time"
)

type dinoNodeFactory struct {
	inogen *inodeNumbersGenerator
}

func newDinoNodeFactory(inogen *inodeNumbersGenerator) *dinoNodeFactory {
	return &dinoNodeFactory{
		inogen: inogen,
	}
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
