package main

import (
	"errors"
	"flag"
	"fmt"
	golog "log"
	"os"

	"github.com/google/gops/agent"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/nicolagi/dino/metadata/client"
	"github.com/nicolagi/dino/storage"
	log "github.com/sirupsen/logrus"
)

func main() {
	defaultConfigFile := os.ExpandEnv("$HOME/lib/dino/fs-default.config")
	configFile := flag.String("c", defaultConfigFile, "location of configuration file, or an alias to expand to $HOME/lib/dino/fs-ALIAS.config")
	flag.Parse()

	log.SetFormatter(&log.TextFormatter{
		DisableColors: true,
		FullTimestamp: true,
	})

	config, err := loadConfig(*configFile)
	if err != nil {
		log.Fatalf("Loading configuration from %q: %v", *configFile, err)
	}

	config.applyDefaultsForMissingProperties()

	if config.Debug {
		log.SetLevel(log.DebugLevel)
	}

	cleanup := redirectLogging(config)
	defer cleanup()

	if err := agent.Listen(agent.Options{}); err != nil {
		log.WithField("err", err).Warn("Could not start gops agent")
	} else {
		defer agent.Close()
	}

	var factory dinoNodeFactory

	remoteClient := client.New(client.WithAddress(config.Metadata.Address))
	rvs := storage.NewRemoteVersionedStore(remoteClient, storage.WithChangeListener(factory.invalidateCache))
	rvs.Start()
	factory.metadata = rvs

	remote, err := storeImpl(config)
	if err != nil {
		log.WithField("err", err).Fatal("Could not build store")
	}
	pairedStore := storage.NewPaired(
		storage.NewDiskStore(os.ExpandEnv(config.DataPath)),
		remote,
	)
	factory.blobs = storage.NewBlobStore(pairedStore)

	g := newInodeNumbersGenerator()
	go g.start()
	defer g.stop()
	factory.inogen = g

	var fsopts fs.Options
	fsopts.Debug = config.DebugFUSE
	fsopts.UID = uint32(os.Getuid())
	fsopts.GID = uint32(os.Getgid())
	fsopts.FsName = config.Name
	fsopts.Name = "dinofs"
	var rootKey [nodeKeyLen]byte
	root := factory.existingNode("root", rootKey)
	factory.root = root
	if err := root.loadMetadata(root.key); err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			log.Infof("Serving an empty file system (no metadata found for root node)")
			root.mode |= fuse.S_IFDIR
			root.children = make(map[string]*dinoNode)
		} else {
			log.Fatalf("Could not load root node metadata: %v", err)
		}
	}

	mount := os.ExpandEnv(config.Mountpoint)
	server, err := fs.Mount(mount, root, &fsopts)
	if err != nil {
		log.Fatalf("Could not mount on %q: %v", mount, err)
	}

	// The following call returns when the filesystem is unmounted (e.g.,
	// with "fusermount -u /n/dino").
	server.Wait()
}

func redirectLogging(c *config) (cleanup func()) {
	golog.SetOutput(log.StandardLogger().Writer())
	if c.LogPath == "" {
		return func() {}
	}
	pathname := os.ExpandEnv(c.LogPath)
	logger := log.WithField("pathname", pathname)
	f, err := os.OpenFile(pathname, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0600)
	if err != nil {
		logger.WithField("err", err).Fatal("Could not open log file")
	}
	logger.Info("Lines after this one will logged to a file")
	log.SetOutput(f)
	return func() {
		if err := f.Close(); err != nil {
			// Can't use the logger here!
			_, _ = fmt.Fprintf(os.Stderr, "Could not close log file cleanly %q: %v", pathname, err)
		}
	}
}

func storeImpl(c *config) (storage.Store, error) {
	switch c.Blobs.Type {
	case "dino":
		return storage.NewRemoteStore(c.Blobs.Address), nil
	case "s3":
		return storage.NewS3Store(
			c.Blobs.Profile,
			c.Blobs.Region,
			c.Blobs.Bucket,
		)
	default:
		log.WithField("type", c.Blobs.Type).Fatal("Unknown blobs type")
		panic("not reached")
	}
}
