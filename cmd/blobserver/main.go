package main

import (
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"

	"github.com/google/gops/agent"
	"github.com/nicolagi/dino/storage"
	log "github.com/sirupsen/logrus"
)

func main() {
	defaultConfigFile := os.ExpandEnv("$HOME/lib/dino/blobserver.config")
	configFile := flag.String("config", defaultConfigFile, "location of configuration file")
	flag.Parse()

	opts, err := loadConfig(*configFile)
	if err != nil {
		log.WithFields(log.Fields{
			"err":  err,
			"path": *configFile,
		}).Fatal("Could not load configuration")
	}

	if opts.Debug {
		log.SetLevel(log.DebugLevel)
	}

	if err := agent.Listen(agent.Options{
		ShutdownCleanup: true,
	}); err != nil {
		log.WithField("err", err).Warn("Could not start gops agent")
	} else {
		defer agent.Close()
	}

	dir := os.ExpandEnv("$HOME/lib/dino")
	if err := os.MkdirAll(dir, 0700); err != nil {
		log.Fatalf("Could not ensure directory %q exists: %v", dir, err)
	}
	dir = os.ExpandEnv("$HOME/lib/dino/data")
	store := storage.NewDiskStore(dir)
	log.Infof("Will use a disk-based backend storing data at %s", dir)

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		var logger *log.Entry
		status, body := func() (int, []byte) {
			hkey := r.URL.Path[1:]
			key, err := hex.DecodeString(hkey)
			if err != nil {
				return http.StatusBadRequest, []byte(fmt.Sprintf("%q: not a valid path, expecting hex key only", r.URL.Path))
			}
			logger = log.WithFields(log.Fields{
				"op":  r.Method,
				"key": hkey,
			})
			switch r.Method {
			case http.MethodGet:
				value, err := store.Get(key)
				if errors.Is(err, storage.ErrNotFound) {
					logger.WithField("err", err).Debug("Not found")
					return http.StatusNotFound, nil
				}
				if err != nil {
					logger.WithField("err", err).Error()
					return http.StatusInternalServerError, []byte(fmt.Sprintf("%q: %v", hkey, err))
				}
				logger.Debug("Success")
				return http.StatusOK, value
			case http.MethodPut:
				value, err := ioutil.ReadAll(r.Body)
				if err != nil {
					logger.WithField("err", err).Error()
					return http.StatusInternalServerError, []byte(fmt.Sprintf("%q: %v", hkey, err))
				}
				if err := store.Put(key, value); err != nil {
					logger.WithField("err", err).Error()
					return http.StatusInternalServerError, []byte(fmt.Sprintf("%q: %v", hkey, err))
				}
				logger.Debug("Success")
				return http.StatusOK, nil
			default:
				logger.Warn("Bad request")
				return http.StatusBadRequest, []byte(fmt.Sprintf("%q: invalid method, expecting GET or PUT", r.Method))
			}
		}()
		w.WriteHeader(status)
		if body != nil {
			if _, err := w.Write(body); err != nil {
				logger.WithField("err", err).Error("Failed writing response")
			}
		}
	})

	if err := http.ListenAndServe(opts.BlobServer, nil); err != nil {
		log.WithField("err", err).Fatal("Could not listen and serve")
	}
}
