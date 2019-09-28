package main

import (
	"encoding/json"
	"os"
)

type options struct {
	Mountpoint     string `json:"mountpoint"`
	Name           string `json:"name"`
	MetadataServer string `json:"metadata_server"`
	BlobServer     string `json:"blob_server"`
	Debug          bool   `json:"debug"`
	DebugFUSE      bool   `json:"debug_fuse"`
}

func loadOptions(pathname string) (*options, error) {
	f, err := os.Open(pathname)
	if err != nil {
		return nil, err
	}
	var opts *options
	err = json.NewDecoder(f).Decode(&opts)
	return opts, err
}
