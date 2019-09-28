package main

import (
	"encoding/json"
	"os"
)

type options struct {
	Name           string `json:"name"`
	MetadataServer string `json:"metadata_server"`
	Debug          bool   `json:"debug"`
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
