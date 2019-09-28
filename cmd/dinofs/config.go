package main

import (
	"encoding/json"
	"os"
)

type config struct {
	Mountpoint     string `json:"mountpoint"`
	Name           string `json:"name"`
	MetadataServer string `json:"metadata_server"`
	BlobServer     string `json:"blob_server"`
	Debug          bool   `json:"debug"`
	DebugFUSE      bool   `json:"debug_fuse"`
	LogPath        string `json:"log_path"`
	DataPath       string `json:"data_path"`
}

func loadConfig(pathname string) (*config, error) {
	f, err := os.Open(pathname)
	if err != nil {
		return nil, err
	}
	var c *config
	err = json.NewDecoder(f).Decode(&c)
	return c, err
}

func (c *config) applyDefaultsForMissingProperties() {
	if c.Mountpoint == "" {
		c.Mountpoint = "/n/dino"
	}
	if c.Name == "" {
		c.Name = "dinofs"
	}
	if c.LogPath == "" {
		c.LogPath = "$HOME/lib/dino/log"
	}
	if c.DataPath == "" {
		c.DataPath = "$HOME/lib/dino/data"
	}
}
