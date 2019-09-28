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
