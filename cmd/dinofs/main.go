package main

import (
	"errors"
	"flag"
	"os"
	"time"

	"github.com/google/gops/agent"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/nicolagi/dino/metadata/client"
	"github.com/nicolagi/dino/storage"
	log "github.com/sirupsen/logrus"
)

var (
	// The root node is global so that nodes can compute their full path.
	root dinoNode

	// The metadata and blob store are global so all nodes can use them.
	metadataStore storage.VersionedStore
	blobStore     *storage.BlobStoreWrapper
)

func main() {
	optsFile := flag.String("config", os.ExpandEnv("$HOME/lib/dino/dinofs.config"), "location of configuration file")
	flag.Parse()

	opts, err := loadOptions(*optsFile)
	if err != nil {
		log.Fatalf("Loading configuration from %q: %v", *optsFile, err)
	}

	if opts.Debug {
		log.SetLevel(log.DebugLevel)
	}

	if err := agent.Listen(agent.Options{}); err != nil {
		log.WithField("err", err).Warn("Could not start gops agent")
	} else {
		defer agent.Close()
	}

	remoteClient, err := client.New(client.WithAddress(opts.MetadataServer), client.WithTimeout(time.Second))
	if err != nil {
		log.Fatal(err)
	}
	rvs := storage.NewRemoteVersionedStore(remoteClient, importMetadata)
	rvs.Start()
	metadataStore = rvs

	pairedStore := storage.NewPaired(
		storage.NewDiskStore(os.ExpandEnv("$HOME/lib/dino/data")),
		storage.NewRemoteStore(opts.BlobServer),
	)
	blobStore = storage.NewBlobStore(pairedStore)

	generateInodeNumbers()

	var fsopts fs.Options
	fsopts.Debug = opts.DebugFUSE
	fsopts.UID = uint32(os.Getuid())
	fsopts.GID = uint32(os.Getgid())
	fsopts.FsName = opts.Name
	fsopts.Name = "dinofs"
	root.name = "root"
	if err := root.loadMetadata(metadataStore, root.key); err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			log.Infof("Serving an empty file system (no metadata found for root node)")
			root.mode |= fuse.S_IFDIR
			root.children = make(map[string]*dinoNode)
		} else {
			log.Fatalf("Could not load root node metadata: %v", err)
		}
	}

	mount := os.ExpandEnv(opts.Mountpoint)
	server, err := fs.Mount(mount, &root, &fsopts)
	if err != nil {
		log.Fatalf("Could not mount on %q: %v", mount, err)
	}

	addKnown(&root)

	// The following call returns when the filesystem is unmounted (e.g.,
	// with "fusermount -u /n/dino").
	server.Wait()
}
