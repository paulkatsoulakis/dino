package main

import (
	"os"

	"github.com/rogpeppe/rjson"
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
	err = rjson.NewDecoder(f).Decode(&opts)
	return opts, err
}
