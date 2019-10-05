package main

import (
	"os"

	"github.com/rogpeppe/rjson"
)

type config struct {
	BlobServer string `json:"blob_server"`
	Debug      bool   `json:"debug"`
}

func loadConfig(pathname string) (*config, error) {
	f, err := os.Open(pathname)
	if err != nil {
		return nil, err
	}
	var c *config
	err = rjson.NewDecoder(f).Decode(&c)
	return c, err
}
