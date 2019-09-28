package main

import (
	"bytes"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/nicolagi/dino/bits"
	"github.com/nicolagi/dino/message"
	"github.com/nicolagi/dino/storage"
	log "github.com/sirupsen/logrus"
)

func (node *dinoNode) serialize() []byte {
	// Could use a pool of buffers, to be reused, instead of putting pressure on
	// the GC.
	size := 24 + len(node.contentKey)
	for attr, value := range node.xattrs {
		size += 4 + len(attr) + len(value)
	}
	for childName := range node.children {
		size += 4 + nodeKeyLen + len(childName)
	}
	buf := make([]byte, size)
	b := buf
	b = bits.Put32(b, node.user)
	b = bits.Put32(b, node.group)
	b = bits.Put32(b, node.mode)
	b = bits.Put64(b, uint64(node.time.UnixNano()))
	b = bits.Putb(b, node.contentKey)
	b = bits.Put16(b, uint16(len(node.xattrs)))
	for attr, value := range node.xattrs {
		b = bits.Puts(b, attr)
		b = bits.Putb(b, value)
	}
	for childName, childNode := range node.children {
		b = bits.Puts(b, childName)
		b = bits.Putb(b, childNode.key[:])
	}
	return buf
}

func (node *dinoNode) unserialize(b []byte) {
	node.user, b = bits.Get32(b)
	node.group, b = bits.Get32(b)
	node.mode, b = bits.Get32(b)
	var unixnano uint64
	unixnano, b = bits.Get64(b)
	node.time = time.Unix(0, int64(unixnano))
	node.contentKey, b = bits.Getb(b)
	if node.mode&fuse.S_IFDIR != 0 {
		node.children = make(map[string]*dinoNode)
	}
	var nxattr uint16
	nxattr, b = bits.Get16(b)
	if nxattr > 0 {
		node.xattrs = make(map[string][]byte)
	}
	for ; nxattr > 0; nxattr-- {
		var attr string
		var value []byte
		attr, b = bits.Gets(b)
		value, b = bits.Getb(b)
		node.xattrs[attr] = value
	}
	if len(b) > 0 {
		var childName string
		var childKey []byte
		for len(b) > 0 {
			childName, b = bits.Gets(b)
			childKey, b = bits.Getb(b)
			var childNode dinoNode
			copy(childNode.key[:], childKey)
			childNode.name = childName
			childNode.mode = modeNotLoaded
			node.children[childNode.name] = &childNode
		}
	}
}

func (node *dinoNode) saveMetadata(store storage.VersionedStore) error {
	value := node.serialize()
	err := store.Put(node.version+1, node.key[:], value)
	if err != nil {
		return err
	}
	node.version++
	return nil
}

func (node *dinoNode) loadMetadata(store storage.VersionedStore, key [nodeKeyLen]byte) error {
	version, b, err := store.Get(key[:])
	if err != nil {
		return err
	}
	node.key = key
	node.version = version
	node.unserialize(b)
	return nil
}

func importMetadata(mutation message.Message) {
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
	knownNodes.Lock()
	node := knownNodes.m[key]
	knownNodes.Unlock()
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

func (node *dinoNode) sync() syscall.Errno {
	if node.shouldSaveContent {
		var err error
		prev := node.contentKey
		node.contentKey, err = blobStore.Put(node.content)
		if err != nil {
			log.WithFields(log.Fields{
				"err": err,
			}).Error("Could not save content")
			return syscall.EIO
		}
		node.shouldSaveContent = false
		if !bytes.Equal(prev, node.contentKey) {
			node.shouldSaveMetadata = true
		}
	}
	if node.shouldSaveMetadata {
		err := node.saveMetadata(metadataStore)
		if err != nil {
			log.WithFields(log.Fields{
				"err": err,
			}).Error("Could not save metadata")
			return syscall.EIO
		}
		node.shouldSaveMetadata = false
	}
	return fs.OK
}
