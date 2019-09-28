# dino

## Introduction

dinofs is a FUSE file system designed to share data volumes among
several participating nodes. All nodes will work on the same data, and changes
done from one node will be automatically visible to the other nodes.

The dino project consists of
* a metadataserver binary, which should run on a server node,
* a blobserver binary, which should run on a server node (possibly the same),
* and of a dinofs binary, which should run on a set of participating client nodes.

The metadataserver is used by dinofs to commit metadata mutations (e.g., chown,
chmod, setfacl, rename, truncate, ...) and is responsible for pushing updates to
all connected clients.

The blobserver is use for persistent storage of blobs (file contents).

The dinofs binary interfaces with the operating system's FUSE implementation to
provide the filesystem functionality. It's heavily cached and does synchronous
network IO to commit metadata mutations and to fetch blobs that are not yet
cached.

## Why FUSE instead of 9P?

Contrary to the [muscle file system](https://github.com/nicolagi/muscle), I need
to use this file system on servers that don't have a 9P driver.

## Bugs and limitations

Probably.

At the time of writing, the code base is the result of a hackathon. I will be
using this software and fixing bugs and update this file once the project is
more stable.

Also, not a lot of effort has (yet) been made on making this performant.

## Security

No. (Not yet.)

## Details

A file system consists of data (file contents) and metadata (where are the file
contents, what's the size, what are the child nodes, ...).

If a file system is to be synchronized over the network among a set of
participating client nodes, key to its efficiency is limiting the synchronous
network calls necessary to fulfill system calls.

Let's show how we do that.

Avoiding synchronous network calls for dinofs mounts writing data is easy. The
data will be saved to a blob store. The blob store will save any value under a
key that's the hash of its value (content addressing). That prevents competing
clients (running dinofs using the same metadata server and same remote storage
backend for the data) from conflicting. If they write to the same key, they'll
also write the same values. Therefore, dinofs will therefore write data to a
local disk-based store (write-back cache) and asynchronously to the blob server.
In terms of file contents, the synchronous workload will always hit local
storage.

This design for the blob store is the reason why its Put method does not take a
key, it only takes a value, and it *returns* its key (to be stored in the file
system node's metadata).

While file system regular file nodes usually consist of multiple data blocks,
for simplicity and because my current use case for dinofs only entails small
files, I'm storing only one blob per regular file or symlink.

## Flexibility

The basic building block for metadata and data storage is a super simple
interface, that of a key-value store. The same interface is used for
* local write-back data cache,
* the remote persistent data storage,
* the local metadata write-through cache,
* the metadataserver persistent metadata storage.

At the time of writing, the implementations used for the above are hard-coded to
* a disk-based store,
* a remote HTTP server (the blobserver binary) that's also disk based (but could easily be S3-based)
* an in-memory map,
* a Bolt database fronted by a custom TCP server (the metadataserver binary).

Wherever I said "disk-based store", "in-memory map", "Bolt database", "S3",
above, one could substitute anything that allows implementing `Put(key, value
[]byte) error` and `Get(key []byte) (value []byte, err error)`. The
VersionedStoreWrapper and the BlobStoreWrapper class will take care of the
higher level synchronization. The PairedStore will asynchronously propagate data
from a local fast store to a remote slow store (any two stores, really), and it
implements the store interface itself.

One could trade speed for simplicity and avoid any local cache, using only
network-accessible storage, which could be the case if you want your files in a
random server that you need to ssh into.

Slightly more advanced, one could implement the versioned store interface
directly (instead of wrapping a store).

For the metadataserver and client, one could actually use a Redis server and a
somewhat fat client, powered by Lua scripting and Redis PubSub for pushing
metadata updates.

Yet another option would be to use DynamoDB (conditional updates to enforce the
next-version constraint) and DynamoDB Streams to fan out the metadata updates.

