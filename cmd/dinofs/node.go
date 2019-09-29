package main

import (
	"bytes"
	"context"
	"strconv"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	log "github.com/sirupsen/logrus"
)

const (
	nodeKeyLen    int    = 20
	modeNotLoaded uint32 = 0xffffffff
)

type dinoNode struct {
	fs.Inode

	// Injected by the node factory itself.
	factory *dinoNodeFactory

	mu sync.Mutex

	shouldSaveMetadata   bool
	shouldReloadMetadata bool
	shouldSaveContent    bool

	user  uint32
	group uint32
	mode  uint32
	time  time.Time

	// Not persisted, only for logging
	name string

	// Used as the key to save/retrieve this node in the metadata store. It's a
	// sort of inode number, but it's not assigned by a central entity and can't
	// be reused.
	key [nodeKeyLen]byte

	// Increases by one at each update (by any client connected to the metadata
	// server).
	version uint64

	xattrs map[string][]byte

	// Only makes sense for regular files or symlinks:
	contentKey []byte
	content    []byte

	// Only makes sense for directories:
	children map[string]*dinoNode
}

func (node *dinoNode) Setxattr(ctx context.Context, attr string, data []byte, flags uint32) syscall.Errno {
	// Implementing this method seems to be needed to compile plan9port in dinofs.
	// This is required when executing "install o.mk /n/dino/src/plan9port/bin/mk".
	// Wrapping that with strace shows:
	//
	//	fsetxattr(…) = -1 ENODATA (No data available)
	//
	// After adding this, compilation fails later on with some segmentation fault
	// which I need to investigate at some point, but that use case might be out of
	// scope for this project, at least for now.
	//
	// According to setxattr(2):
	//
	// By default (i.e., flags is zero), the extended attribute will be created if
	// it does not exist, or the value will be replaced if the attribute already
	// exists. To modify these semantics, one of the following values can be
	// specified in flags:
	//
	// XATTR_CREATE Perform a pure create, which fails if the named attribute
	// exists already.
	//
	// XATTR_REPLACE Perform a pure replace operation, which fails if the named
	// attribute does not already exist.
	node.mu.Lock()
	defer node.mu.Unlock()
	if node.xattrs == nil {
		node.xattrs = make(map[string][]byte)
	}
	switch flags {
	case unix.XATTR_CREATE:
		if _, ok := node.xattrs[attr]; ok {
			return syscall.EEXIST
		}
	case unix.XATTR_REPLACE:
		if _, ok := node.xattrs[attr]; !ok {
			return syscall.ENODATA
		}
	}
	rbdata := node.xattrs[attr]
	node.xattrs[attr] = append([]byte{}, data...)
	node.shouldSaveMetadata = true
	errno := node.sync()
	// Rollback.
	if errno != 0 {
		if rbdata != nil {
			node.xattrs[attr] = rbdata
		} else {
			delete(node.xattrs, attr)
		}
	}
	return errno
}

// Getxattr should read data for the given attribute into
// `dest` and return the number of bytes. If `dest` is too
// small, it should return ERANGE and the size of the attribute.
// If not defined, Getxattr will return ENOATTR.
func (node *dinoNode) Getxattr(ctx context.Context, attr string, dest []byte) (uint32, syscall.Errno) {
	node.mu.Lock()
	defer node.mu.Unlock()
	if node.xattrs == nil {
		return 0, syscall.ENODATA
	}
	value, ok := node.xattrs[attr]
	if !ok {
		return 0, syscall.ENODATA
	}
	if len(value) > len(dest) {
		return uint32(len(value)), syscall.ERANGE
	}
	return uint32(copy(dest, value)), 0
}

func (node *dinoNode) Rmdir(ctx context.Context, name string) syscall.Errno {
	node.mu.Lock()
	defer node.mu.Unlock()
	child := node.children[name]
	// go-fuse should know to call into Rmdir only if the child exists.
	// Since a panic() here would break the mount, let's be defensive anyway.
	if child == nil {
		log.WithFields(log.Fields{
			"name": name,
		}).Warn("Asked to remove directory that does not exist")
		return syscall.ENOENT
	}
	child.mu.Lock()
	defer child.mu.Unlock()
	if len(child.children) != 0 {
		return syscall.ENOTEMPTY
	}
	delete(node.children, name)
	node.shouldSaveMetadata = true
	errno := node.sync()
	// Rollback.
	if errno != 0 {
		node.children[name] = child
	}
	return errno
}

func (node *dinoNode) Unlink(ctx context.Context, name string) syscall.Errno {
	node.mu.Lock()
	defer node.mu.Unlock()
	child := node.children[name]
	delete(node.children, name)
	node.shouldSaveMetadata = true
	errno := node.sync()
	// Rollback.
	if errno != 0 && child != nil {
		node.children[name] = child
	}
	return errno
}

// Call with lock held.
func (node *dinoNode) fullPath() string {
	return node.Path(node.factory.root.EmbeddedInode())
}

// Call with lock held.
func (node *dinoNode) reloadIfNeeded() syscall.Errno {
	if !node.shouldReloadMetadata {
		return 0
	}
	logger := log.WithField("parent", node.name)
	nn := &dinoNode{factory: node.factory}
	if err := nn.loadMetadata(node.key); err != nil {
		logger.WithField("err", err).Error("Could not reload")
		return syscall.EIO
	}
	node.shouldSaveMetadata = false
	node.shouldReloadMetadata = false
	node.shouldSaveContent = false
	node.user = nn.user
	node.group = nn.group
	node.mode = nn.mode
	node.time = nn.time
	if node.version != nn.version {
		logger.Debugf("Version changed from %d to %d", node.version, nn.version)
		node.version = nn.version
	}
	node.xattrs = nn.xattrs
	if !bytes.Equal(node.contentKey, nn.contentKey) {
		logger.Debug("Content changed, marking for lazy reload")
		node.contentKey = nn.contentKey
		node.content = nil
	}

	// Children are by far the hardest part to reload. I've spent way too many
	// hours trying to make this work.

	for name, child := range nn.children {
		logger := logger.WithField("name", name)
		if prev := node.children[name]; prev != nil {
			if prev.key == child.key {
				logger.Debug("Child kept same key - no op")
			} else {
				logger.Debug("Child changed key - updating that and marking for reload")
				prev.key = child.key
				prev.shouldReloadMetadata = true
			}
		} else {
			logger.Debug("Child is new, adding for lazy loading")
			child.name = name
			node.children[name] = child
		}
	}

	for name := range node.children {
		if nn.children[name] == nil {
			logger.Debug("Child has been removed, removing here too")
			node.RmChild(name)
			delete(node.children, name)
		}
	}

	return 0
}

func (node *dinoNode) Opendir(ctx context.Context) syscall.Errno {
	node.mu.Lock()
	defer node.mu.Unlock()
	if errno := node.reloadIfNeeded(); errno != 0 {
		return errno
	}
	for _, childNode := range node.children {
		if errno := node.ensureChildLoaded(ctx, childNode); errno != 0 {
			return errno
		}
	}
	return 0
}

func (node *dinoNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	node.mu.Lock()
	defer node.mu.Unlock()
	if errno := node.reloadIfNeeded(); errno != 0 {
		return nil, errno
	}
	child := node.children[name]
	if child == nil {
		return nil, syscall.ENOENT
	}
	if errno := node.ensureChildLoaded(ctx, child); errno != 0 {
		return nil, errno
	}
	return child.EmbeddedInode(), 0
}

// Call with lock held.
func (node *dinoNode) ensureChildLoaded(ctx context.Context, childNode *dinoNode) syscall.Errno {
	if childNode.mode != modeNotLoaded {
		return 0
	}
	if err := childNode.loadMetadata(childNode.key); err != nil {
		log.WithFields(log.Fields{
			"err":    err,
			"child":  childNode.name,
			"parent": node.fullPath(),
		}).Error("could not load metadata")
		return syscall.EIO
	}
	node.AddChild(childNode.name, node.NewInode(ctx, childNode, fs.StableAttr{
		Mode: childNode.mode,
		Ino:  node.factory.inogen.next(),
	}), false)
	return 0
}

func (node *dinoNode) Flush(ctx context.Context, f fs.FileHandle) syscall.Errno {
	node.mu.Lock()
	defer node.mu.Unlock()
	prev := node.contentKey
	errno := node.sync()
	if errno != 0 && !bytes.Equal(prev, node.contentKey) {
		// Rollback.
		node.contentKey = prev
		node.content = nil
	}
	return errno
}

func (node *dinoNode) Fsync(ctx context.Context, f fs.FileHandle, flags uint32) syscall.Errno {
	// Nothing to do here, we are happy with syncing on each flush call. I've added
	// it to be able to use vim, IIRC.
	return 0
}

func (node *dinoNode) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	node.mu.Lock()
	defer node.mu.Unlock()
	if errno := node.reloadIfNeeded(); errno != 0 {
		return errno
	}
	if errno := node.ensureContentLoaded(); errno != 0 {
		return errno
	}
	out.Uid = node.user
	out.Gid = node.group
	out.Mode = node.mode
	out.Atime = uint64(node.time.Unix())
	out.Mtime = uint64(node.time.Unix())
	out.Size = uint64(len(node.content))
	return 0
}

func (node *dinoNode) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (*fs.Inode, fs.FileHandle, uint32, syscall.Errno) {
	node.mu.Lock()
	defer node.mu.Unlock()
	child, rollback, errno := node.createLockedChild(ctx, name, mode, fuse.S_IFREG)
	if errno != 0 {
		return nil, nil, 0, errno
	}
	defer child.mu.Unlock()
	child.shouldSaveMetadata = true
	if errno := child.sync(); errno != 0 {
		rollback()
		return nil, nil, 0, errno
	}
	node.shouldSaveMetadata = true
	if errno := node.sync(); errno != 0 {
		rollback()
		return nil, nil, 0, errno
	}
	return child.EmbeddedInode(), nil, 0, 0
}

func (node *dinoNode) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	node.mu.Lock()
	defer node.mu.Unlock()
	child, rollback, errno := node.createLockedChild(ctx, name, mode, fuse.S_IFDIR)
	if errno != 0 {
		return nil, errno
	}
	defer child.mu.Unlock()
	child.children = make(map[string]*dinoNode)
	child.shouldSaveMetadata = true
	node.shouldSaveMetadata = true
	if errno := child.sync(); errno != 0 {
		rollback()
		return nil, errno
	}
	if errno := node.sync(); errno != 0 {
		rollback()
		return nil, errno
	}
	return child.EmbeddedInode(), 0
}

func (node *dinoNode) Symlink(ctx context.Context, target, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	node.mu.Lock()
	defer node.mu.Unlock()
	child, rollback, errno := node.createLockedChild(ctx, name, 0, fuse.S_IFLNK)
	if errno != 0 {
		return nil, errno
	}
	defer child.mu.Unlock()
	child.shouldSaveContent = true
	child.content = []byte(target)
	child.shouldSaveMetadata = true
	node.shouldSaveMetadata = true
	if errno := child.sync(); errno != 0 {
		rollback()
		return nil, errno
	}
	if errno := node.sync(); errno != 0 {
		rollback()
		return nil, errno
	}
	return child.EmbeddedInode(), 0
}

func (node *dinoNode) createLockedChild(ctx context.Context, name string, mode uint32, orMode uint32) (child *dinoNode, rollback func(), errno syscall.Errno) {
	id := fs.StableAttr{
		Mode: mode | orMode,
		Ino:  node.factory.inogen.next(),
	}
	child, err := node.factory.allocNode()
	if err != nil {
		log.WithFields(log.Fields{
			"err":    err,
			"child":  name,
			"parent": node.fullPath(),
		}).Error("Create child")
		return nil, nil, syscall.EIO
	}
	child.name = name
	child.mode = id.Mode
	node.children[name] = child
	// Lock before adding to the tree. Caller will unlock.
	child.mu.Lock()
	node.AddChild(name, node.NewInode(ctx, child, id), false)
	return child, func() {
		node.RmChild(name)
		delete(node.children, name)
	}, 0
}

func (node *dinoNode) Open(ctx context.Context, flags uint32) (fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	node.mu.Lock()
	defer node.mu.Unlock()
	if errno := node.reloadIfNeeded(); errno != 0 {
		return nil, 0, errno
	}
	return nil, 0, node.ensureContentLoaded()
}

func (node *dinoNode) ensureContentLoaded() syscall.Errno {
	logger := log.WithFields(log.Fields{
		"name": node.name,
	})
	if node.shouldSaveContent {
		return 0
	}
	if node.mode&fuse.S_IFREG == 0 && node.mode&fuse.S_IFLNK == 0 {
		return 0
	}
	if len(node.contentKey) == 0 {
		return 0
	}
	if len(node.content) != 0 {
		return 0
	}
	value, err := node.factory.blobs.Get(node.contentKey)
	if err != nil {
		logger.WithField("err", err).Error("Could not load content")
		return syscall.EIO
	}
	logger.WithField("size", len(value)).Debug("Content loaded")
	node.content = value
	return 0
}

func (node *dinoNode) Read(ctx context.Context, f fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	node.mu.Lock()
	defer node.mu.Unlock()
	if off > int64(len(node.content)) {
		return fuse.ReadResultData(nil), 0
	}
	end := off + int64(len(dest))
	if end > int64(len(node.content)) {
		end = int64(len(node.content))
	}
	return fuse.ReadResultData(node.content[off:end]), 0
}

func (node *dinoNode) Readlink(ctx context.Context) ([]byte, syscall.Errno) {
	return node.content, 0
}

func (node *dinoNode) Rename(ctx context.Context, name string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	node.mu.Lock()
	defer node.mu.Unlock()

	child := node.GetChild(name).Operations().(*dinoNode)
	child.mu.Lock()
	defer child.mu.Unlock()
	child.name = newName

	newParentNode := newParent.EmbeddedInode().Operations().(*dinoNode)
	if node.key != newParentNode.key {
		newParentNode.mu.Lock()
		defer newParentNode.mu.Unlock()
	}
	newParentNode.children[newName] = child
	delete(node.children, name)

	child.shouldSaveMetadata = true
	newParentNode.shouldSaveMetadata = true
	node.shouldSaveMetadata = true
	if errno := child.sync(); errno != 0 {
		return errno
	}
	if errno := newParentNode.sync(); errno != 0 {
		return errno
	}
	if errno := node.sync(); errno != 0 {
		return errno
	}
	return 0
}

func (node *dinoNode) resize(size uint64) (previous []byte) {
	previous = node.content
	if size > uint64(cap(node.content)) {
		larger := make([]byte, size)
		copy(larger, node.content)
		node.content = larger
	} else {
		node.content = node.content[:size]
	}
	return previous
}

func (node *dinoNode) Setattr(ctx context.Context, f fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	node.mu.Lock()
	defer node.mu.Unlock()
	var rbtime *time.Time
	var rbuser *uint32
	var rbgroup *uint32
	var rbmode *uint32
	var rbsize *int
	var rbcontent []byte

	if t, ok := in.GetMTime(); ok {
		rbtime = new(time.Time)
		*rbtime = node.time
		node.time = t
	}
	if uid, ok := in.GetUID(); ok {
		rbuser = new(uint32)
		*rbuser = node.user
		node.user = uid
	}
	if gid, ok := in.GetGID(); ok {
		rbgroup = new(uint32)
		*rbgroup = node.group
		node.group = gid
	}
	if mode, ok := in.GetMode(); ok {
		log.WithFields(log.Fields{
			"name":      node.name,
			"requested": bitsOf(mode),
			"old":       bitsOf(node.mode),
			"new":       bitsOf(node.mode&0xfffff000 | mode&0x00000fff),
		}).Debug("mode change")
		rbmode = new(uint32)
		*rbmode = node.mode
		node.mode = node.mode&0xfffff000 | mode&0x00000fff
	}
	if size, ok := in.GetSize(); ok {
		rbsize = new(int)
		*rbsize = len(node.content)
		rbcontent = node.resize(size)
		node.time = time.Now()
		node.shouldSaveContent = true
	}
	node.shouldSaveMetadata = true
	errno := node.sync()
	if errno != 0 {
		// Rollback.
		if rbtime != nil {
			node.time = *rbtime
		}
		if rbuser != nil {
			node.user = *rbuser
		}
		if rbgroup != nil {
			node.group = *rbgroup
		}
		if rbmode != nil {
			node.mode = *rbmode
		}
		if rbsize != nil {
			node.content = rbcontent
		}
	}
	return errno
}

func bitsOf(mode uint32) string {
	return strconv.FormatUint(uint64(mode), 2)
}

func (node *dinoNode) Write(ctx context.Context, f fs.FileHandle, data []byte, off int64) (written uint32, errno syscall.Errno) {
	node.mu.Lock()
	defer node.mu.Unlock()

	sz := int64(len(data))
	if off+sz > int64(len(node.content)) {
		node.resize(uint64(off + sz))
	}
	copy(node.content[off:], data)
	node.time = time.Now()
	if sz > 0 {
		node.shouldSaveContent = true
	}
	return uint32(sz), 0
}
