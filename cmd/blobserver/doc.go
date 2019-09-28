// Package blobserver implements an HTTP server that can be used together with
// its client, storage.RemoteStore. It is backed by a disk-based implementation
// of storage.Store, but it should be extended to use any implementation.
//
// Valid requests are GETs and PUTs to paths of the form "/b33f" or "/f00d",
// that is, slash followed by a hexadecimal string, encoding the key to GET or
// PUT. Requests for other paths or with other HTTP verbs will return 400.
//
// If a key is not found, GETs return 404 with no body, which the client should
// propagate as storage.ErrNotFound. Any other error on the GET path returns 500
// and the textual error message in the response body. The happy scenario is
// that of a 200 response status code, and the value as the response body.
//
// As for PUTs, the body is of course the value to be stored. The response is
// either 200 status code and empty body, or 500 status code and the error
// message in the body.
package main // import "github.com/nicolagi/dino/cmd/blobserver"
