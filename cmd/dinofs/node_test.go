package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/google/gops/agent"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/nicolagi/dino/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeVersionedStore struct {
	mu   sync.Mutex
	err  error
	errs []error
}

func (s *fakeVersionedStore) Get([]byte) (uint64, []byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.errs) > 0 {
		err := s.errs[0]
		s.errs = s.errs[1:]
		return 0, nil, err
	}
	return 0, nil, s.err
}

func (s *fakeVersionedStore) Put(uint64, []byte, []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.errs) > 0 {
		err := s.errs[0]
		s.errs = s.errs[1:]
		return err
	}
	return s.err
}

func (s *fakeVersionedStore) setErr(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.err = err
}

func (s *fakeVersionedStore) setErrSequence(errs ...error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.errs = errs
}

func TestNodeMetadataRollback(t *testing.T) {
	if err := agent.Listen(agent.Options{}); err != nil {
		t.Logf("Could not start gops agent: %v", err)
	}
	defer agent.Close()
	rootdir, factory, cleanup := testMount(t)
	defer cleanup()
	ko := func() {
		factory.metadata.(*fakeVersionedStore).setErr(errors.New("computer bought the farm"))
	}
	ok := func() {
		factory.metadata.(*fakeVersionedStore).setErr(nil)
	}
	okko := func() {
		factory.metadata.(*fakeVersionedStore).setErrSequence(nil, errors.New("does not compute"))
	}
	randomName := func() string {
		name := make([]byte, 16)
		rand.Read(name)
		return fmt.Sprintf("%x", name)
	}
	t.Run("Setxattr", func(t *testing.T) {
		t.Run("rolls back additions", func(t *testing.T) {
			node, err := factory.allocNode()
			if err != nil {
				t.Fatal(err)
			}
			ko()
			errno := node.Setxattr(context.Background(), "key", []byte("value"), 0)
			if errno != syscall.EIO {
				t.Fatalf("got %d, want %d", errno, syscall.EIO)
			}
			assert.Len(t, node.xattrs, 0)
		})
		t.Run("rolls back updates", func(t *testing.T) {
			node, err := factory.allocNode()
			require.Nil(t, err)
			ok()
			errno := node.Setxattr(context.Background(), "key", []byte("old value"), 0)
			require.EqualValues(t, 0, errno)
			ko()
			errno = node.Setxattr(context.Background(), "key", []byte("value"), 0)
			if errno != syscall.EIO {
				t.Fatalf("got %d, want %d", errno, syscall.EIO)
			}
			assert.Len(t, node.xattrs, 1)
			assert.EqualValues(t, "old value", node.xattrs["key"])
		})
	})
	t.Run("Rmdir", func(t *testing.T) {
		t.Run("adds back removed child directory", func(t *testing.T) {
			p := filepath.Join(rootdir, randomName())
			ok()
			err := os.Mkdir(p, 0755)
			if err != nil {
				t.Fatalf("got %v, want nil", err)
			}
			if _, err := os.Stat(p); err != nil {
				t.Fatalf("got %v, want nil", err)
			}
			ko()
			if err := os.Remove(p); err == nil {
				t.Fatal("got nil, want non-nil")
			}
			if _, err := os.Stat(p); err != nil {
				t.Fatalf("got %v, want nil", err)
			}
			// Second remove should succeed, while without rollback it would panic
			// (assuming entry from map non-nil) or return syscall.ENOENT if we're being
			// defensive enough.
			ok()
			err = os.Remove(p)
			if err != nil {
				t.Fatalf("got %v, want nil", err)
			}
			if _, err := os.Stat(p); !os.IsNotExist(err) {
				t.Fatalf("got %v, want %v", err, os.ErrNotExist)
			}
		})
	})
	t.Run("Unlink", func(t *testing.T) {
		t.Run("adds back removed child file", func(t *testing.T) {
			p := filepath.Join(rootdir, randomName())
			ok()
			if err := ioutil.WriteFile(p, []byte("Peggy Sue"), 0644); err != nil {
				t.Fatalf("got %v, want nil", err)
			}
			ko()
			if err := os.Remove(p); err == nil {
				t.Fatal("got nil, want non-nil")
			}
			// After remove failure, should still be able to read up the file.
			if b, err := ioutil.ReadFile(p); err != nil {
				t.Errorf("got %v, want nil", err)
			} else if string(b) != "Peggy Sue" {
				t.Errorf("got %q, want %q", b, "Peggy Sue")
			}
		})
	})
	t.Run("Flush", func(t *testing.T) {
		t.Run("reverts to old data if flush fails", func(t *testing.T) {
			p := filepath.Join(rootdir, randomName())
			ok()
			if err := ioutil.WriteFile(p, []byte("old contents"), 0644); err != nil {
				t.Fatalf("got %v, want nil", err)
			}
			f, err := os.OpenFile(p, os.O_WRONLY, 0644)
			if err != nil {
				t.Fatalf("got %v, want nil", err)
			}
			if _, err := f.Write([]byte("new contents")); err != nil {
				t.Fatalf("got %v, want nil", err)
			}
			ko()
			if err := f.Close(); err == nil {
				t.Fatalf("got nil, want non-nil")
			}
			ok()
			b, err := ioutil.ReadFile(p)
			if err != nil {
				t.Fatalf("got %v, want nil", err)
			}
			if !bytes.Equal(b, []byte("old contents")) {
				t.Errorf("got %q, want %q", b, "old contents")
			}
		})
	})
	t.Run("Create", func(t *testing.T) {
		t.Run("removes file just created if child sync fails", func(t *testing.T) {
			p := filepath.Join(rootdir, randomName())
			ko()
			f, err := os.Create(p)
			if err == nil {
				t.Fatal("got nil, want non-nil")
			}
			if f != nil {
				t.Errorf("got %v, want nil", f)
			}
			ok()
			if _, err := os.Stat(p); !os.IsNotExist(err) {
				t.Fatalf("got %v, want %v", err, os.ErrNotExist)
			}
		})
		t.Run("removes file just created if parent sync fails", func(t *testing.T) {
			p := filepath.Join(rootdir, randomName())
			okko()
			f, err := os.Create(p)
			if err == nil {
				t.Fatal("got nil, want non-nil")
			}
			if f != nil {
				t.Errorf("got %v, want nil", f)
			}
			ok()
			if _, err := os.Stat(p); !os.IsNotExist(err) {
				t.Fatalf("got %v, want %v", err, os.ErrNotExist)
			}
		})
	})
	t.Run("Mkdir", func(t *testing.T) {
		t.Run("removes directory just created if child sync fails", func(t *testing.T) {
			p := filepath.Join(rootdir, randomName())
			ko()
			err := os.Mkdir(p, 0755)
			if err == nil {
				t.Fatal("got nil, want non-nil")
			}
			ok()
			if _, err := os.Stat(p); !os.IsNotExist(err) {
				t.Fatalf("got %v, want %v", err, os.ErrNotExist)
			}
		})
		t.Run("removes directory just created if parent sync fails", func(t *testing.T) {
			p := filepath.Join(rootdir, randomName())
			okko()
			err := os.Mkdir(p, 0755)
			if err == nil {
				t.Fatal("got nil, want non-nil")
			}
			ok()
			if _, err := os.Stat(p); !os.IsNotExist(err) {
				t.Fatalf("got %v, want %v", err, os.ErrNotExist)
			}
		})
	})
	t.Run("Symlink", func(t *testing.T) {
		t.Run("removes symlink just created if child sync fails", func(t *testing.T) {
			oldname := filepath.Join(rootdir, randomName())
			err := ioutil.WriteFile(oldname, []byte("content"), 0644)
			if err != nil {
				t.Fatal(err)
			}
			newname := filepath.Join(rootdir, randomName())
			ko()
			if err := os.Symlink(oldname, newname); err == nil {
				t.Fatal("got nil, want non-nil")
			}
			ok()
			if _, err := os.Stat(newname); !os.IsNotExist(err) {
				t.Fatalf("got %v, want %v", err, os.ErrNotExist)
			}
		})
		t.Run("removes symlink just created if parent sync fails", func(t *testing.T) {
			oldname := filepath.Join(rootdir, randomName())
			err := ioutil.WriteFile(oldname, []byte("content"), 0644)
			if err != nil {
				t.Fatal(err)
			}
			newname := filepath.Join(rootdir, randomName())
			okko()
			if err := os.Symlink(oldname, newname); err == nil {
				t.Fatal("got nil, want non-nil")
			}
			ok()
			if _, err := os.Stat(newname); !os.IsNotExist(err) {
				t.Fatalf("got %v, want %v", err, os.ErrNotExist)
			}
		})
	})
	t.Run("Rename", func(t *testing.T) {
		t.Skip("To be able to rollback renaming, we need transactions on the metadataserver.")
	})
	t.Run("Setattr", func(t *testing.T) {
		t.Run("rolls back time change", func(t *testing.T) {
			p := filepath.Join(rootdir, randomName())
			ok()
			if err := ioutil.WriteFile(p, []byte("anything"), 0644); err != nil {
				t.Fatalf("got %v, want nil", err)
			}
			newTime := time.Unix(123456789, 0)
			ko()
			if err := os.Chtimes(p, newTime, newTime); err == nil {
				t.Fatalf("got nil, want non-nil")
			}
			fi, err := os.Stat(p)
			if err != nil {
				t.Fatalf("got %v, want nil", err)
			}
			if fi.ModTime().Equal(newTime) {
				t.Errorf("got %v, want anything else", newTime)
			}
		})
		t.Run("rolls back owner change", func(t *testing.T) {
			p := filepath.Join(rootdir, randomName())
			ok()
			if err := ioutil.WriteFile(p, []byte("anything"), 0644); err != nil {
				t.Fatalf("got %v, want nil", err)
			}
			ko()
			if err := os.Chown(p, 42, -1); err == nil {
				t.Fatalf("got nil, want non-nil")
			}
			fi, err := os.Stat(p)
			if err != nil {
				t.Fatalf("got %v, want nil", err)
			}
			if got := fi.Sys().(*syscall.Stat_t).Uid; got == 42 {
				t.Errorf("got %v, want != 42", got)
			}
		})
		t.Run("rolls back group change", func(t *testing.T) {
			p := filepath.Join(rootdir, randomName())
			ok()
			if err := ioutil.WriteFile(p, []byte("anything"), 0644); err != nil {
				t.Fatalf("got %v, want nil", err)
			}
			ko()
			if err := os.Chown(p, -1, 42); err == nil {
				t.Fatalf("got nil, want non-nil")
			}
			fi, err := os.Stat(p)
			if err != nil {
				t.Fatalf("got %v, want nil", err)
			}
			if got := fi.Sys().(*syscall.Stat_t).Gid; got == 42 {
				t.Errorf("got %v, want != 42", got)
			}
		})
		t.Run("rolls back mode change", func(t *testing.T) {
			p := filepath.Join(rootdir, randomName())
			ok()
			if err := ioutil.WriteFile(p, []byte("anything"), 0644); err != nil {
				t.Fatalf("got %v, want nil", err)
			}
			ko()
			if err := os.Chmod(p, 0111); err == nil {
				t.Fatalf("got nil, want non-nil")
			}
			fi, err := os.Stat(p)
			if err != nil {
				t.Fatalf("got %v, want nil", err)
			}
			if got := fi.Mode(); got == 0111 {
				t.Errorf("got %v, want != 0111", got)
			}
		})
		t.Run("rolls back to smaller buffer", func(t *testing.T) {
			p := filepath.Join(rootdir, randomName())
			ok()
			if err := ioutil.WriteFile(p, []byte("anything"), 0644); err != nil {
				t.Fatalf("got %v, want nil", err)
			}
			ko()
			if err := os.Truncate(p, 3); err == nil {
				t.Fatalf("got nil, want non-nil")
			}
			got, err := ioutil.ReadFile(p)
			if err != nil {
				t.Fatalf("got %v, want nil", err)
			}
			if !bytes.Equal(got, []byte("anything")) {
				t.Errorf("got %v, want %q", got, "anything")
			}
		})
		t.Run("rolls back to larger buffer", func(t *testing.T) {
			p := filepath.Join(rootdir, randomName())
			ok()
			if err := ioutil.WriteFile(p, []byte("anything"), 0644); err != nil {
				t.Fatalf("got %v, want nil", err)
			}
			ko()
			if err := os.Truncate(p, 42); err == nil {
				t.Fatalf("got nil, want non-nil")
			}
			got, err := ioutil.ReadFile(p)
			if err != nil {
				t.Fatalf("got %v, want nil", err)
			}
			if !bytes.Equal(got, []byte("anything")) {
				t.Errorf("got %v, want %q", got, "anything")
			}
		})
	})
}

func testMount(t *testing.T) (mountpoint string, factory *dinoNodeFactory, cleanup func()) {
	t.Helper()

	dir, err := ioutil.TempDir("", "dinofs-test-")
	if err != nil {
		t.Fatal(err)
	}

	factory = &dinoNodeFactory{}
	factory.inogen = newInodeNumbersGenerator()
	go factory.inogen.start()

	factory.metadata = &fakeVersionedStore{}
	factory.blobs = storage.NewBlobStore(storage.NewInMemoryStore())

	var zero [nodeKeyLen]byte
	root := factory.existingNode("root", zero)
	root.mode |= fuse.S_IFDIR
	root.children = make(map[string]*dinoNode)
	factory.root = root

	server, err := fs.Mount(dir, root, &fs.Options{
		UID: uint32(os.Getuid()),
		GID: uint32(os.Getgid()),
	})
	if err != nil {
		factory.inogen.stop()
		t.Fatal(err)
	}

	return dir, factory, func() {
		_ = server.Unmount()
		_ = os.RemoveAll(dir)
		factory.inogen.stop()
	}
}
