package storage

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/nicolagi/dino/message"
	"github.com/nicolagi/dino/metadata/client"
	log "github.com/sirupsen/logrus"
)

var (
	ErrShutdown            = errors.New("shutdown")
	ErrCancelledRendezvous = errors.New("request and response did not meet")
)

type ChangeListener func(message.Message)

// RemoteVersionedStore is an implementation of VersionedStore, via a client to a remote
// metadataserver process.
type RemoteVersionedStore struct {
	tags   *message.MonotoneTags
	remote *client.Client
	local  VersionedStore

	listener ChangeListener

	// Keeps track of goroutines waiting for a response in the do method, and the
	// goroutine running the receive loop. Used to ensure all of those method calls
	// return when Shutdown is called.
	doing sync.WaitGroup

	mu         sync.Mutex
	rendezvous map[uint16]chan message.Message
	stopped    bool
}

func NewRemoteVersionedStore(remote *client.Client, listener ChangeListener) *RemoteVersionedStore {
	var rs RemoteVersionedStore
	rs.tags = message.NewMonotoneTags()
	rs.remote = remote
	rs.rendezvous = make(map[uint16]chan message.Message)
	rs.local = NewVersionedWrapper(NewInMemoryStore())
	rs.listener = listener
	return &rs
}

func (rs *RemoteVersionedStore) Start() {
	go rs.receiveLoop()
}

func (rs *RemoteVersionedStore) Stop() {
	rs.mu.Lock()
	rs.stopped = true
	rs.mu.Unlock()

	// The goroutines waiting for a response will timeout (and return
	// ErrCancelledRendezvous). The receive loop will fail the receive because
	// of the connection being closed, and will see the stopped flag is set, and
	// exit.
	_ = rs.remote.Close()
	rs.doing.Wait()

	rs.tags.Stop()
}

func (rs *RemoteVersionedStore) newRendezvous(tag uint16) chan message.Message {
	c := make(chan message.Message, 1)
	rs.mu.Lock()
	rs.rendezvous[tag] = c
	rs.mu.Unlock()
	return c
}

func (rs *RemoteVersionedStore) doRendezvous(tag uint16, response message.Message) {
	rs.mu.Lock()
	c := rs.rendezvous[tag]
	if c != nil {
		c <- response
	} else {
		log.WithFields(log.Fields{
			"message": response,
		}).Debug("Response for no request?")
	}
	delete(rs.rendezvous, tag)
	rs.mu.Unlock()
}

func (rs *RemoteVersionedStore) cancelRendezvous(tag uint16) {
	rs.mu.Lock()
	delete(rs.rendezvous, tag)
	rs.mu.Unlock()
}

// do sends a request and waits up to a second for its response.
func (rs *RemoteVersionedStore) do(request message.Message) (response message.Message, err error) {
	rs.doing.Add(1)
	defer rs.doing.Done()
	tag := request.Tag()
	r := rs.newRendezvous(tag)
	if err := rs.remote.Send(request); err != nil {
		rs.cancelRendezvous(tag)
		return response, err
	}
	select {
	case response = <-r:
		return response, nil
	case <-time.After(time.Second):
		rs.cancelRendezvous(tag)
		return response, ErrCancelledRendezvous
	}
}

func (rs *RemoteVersionedStore) Put(version uint64, key []byte, value []byte) (err error) {
	request := message.NewPutMessage(rs.tags.Next(), string(key), string(value), version)
	response, err := rs.do(request)
	if err != nil {
		return err
	}
	switch response.Kind() {
	case message.KindPut:
		if request != response {
			log.WithFields(log.Fields{
				"request":  request,
				"response": response,
			}).Error("request and response do not match")
			return fmt.Errorf("request and response do not match")
		}
		return nil
	case message.KindError:
		if response.Value() == ErrStalePut.Error() {
			return ErrStalePut
		}
		return errors.New(response.Value())
	default:
		return fmt.Errorf("unexpected response kind: %v", response.Kind())
	}
}

func (rs *RemoteVersionedStore) Get(key []byte) (version uint64, value []byte, err error) {
	version, value, err = rs.local.Get(key)
	if err == nil {
		return
	}
	response, err := rs.do(message.NewGetMessage(rs.tags.Next(), string(key)))
	if err != nil {
		return 0, nil, err
	}
	switch response.Kind() {
	case message.KindPut:
		return response.Version(), []byte(response.Value()), nil
	case message.KindError:
		if strings.HasSuffix(response.Value(), "not found") {
			return 0, nil, ErrNotFound
		}
		return 0, nil, errors.New(response.Value())
	default:
		fmt.Println(response.String())
		return 0, nil, fmt.Errorf("unexpected response kind: %v", response.Kind())
	}
}

func (rs *RemoteVersionedStore) receiveLoop() {
	rs.doing.Add(1)
	defer rs.doing.Done()
	for {
		rs.mu.Lock()
		stopped := rs.stopped
		rs.mu.Unlock()
		if stopped {
			break
		}
		var m message.Message
		if err := rs.remote.Receive(&m); err != nil {
			log.WithFields(log.Fields{
				"err": err,
			}).Error("receive error")
			rs.mu.Lock()
			stopped := rs.stopped
			rs.mu.Unlock()
			if stopped {
				break
			}
			time.Sleep(time.Second)
			continue
		}
		tag := m.Tag()
		if tag != 0 {
			rs.doRendezvous(tag, m)
		}
		if tag == 0 && m.Kind() == message.KindPut {
			lres := ApplyMessage(rs.local, m)
			if lres.Kind() == message.KindError {
				log.WithFields(log.Fields{
					"err": lres,
				}).Error("Could not apply locally")
			} else if rs.listener != nil {
				rs.listener(lres)
			}
		}
	}
}
