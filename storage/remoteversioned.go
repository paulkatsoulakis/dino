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
	ErrShutdown = errors.New("shutdown")
)

type ChangeListener func(message.Message)

// RemoteVersionedStore is an implementation of VersionedStore, via a client to a remote
// metadata server.
type RemoteVersionedStore struct {
	tags   *message.MonotoneTags
	remote *client.Client
	local  VersionedStore

	listener ChangeListener

	// Keeps track of goroutines waiting for a response in the do method, and the
	// goroutine running the receive loop. Used to ensure all of those method calls
	// return when Shutdown is called.
	doing sync.WaitGroup

	mu        sync.Mutex
	received  *sync.Cond
	responses map[uint16]message.Message
	stopped   bool
}

func NewRemoteVersionedStore(remote *client.Client, listener ChangeListener) *RemoteVersionedStore {
	var rs RemoteVersionedStore
	rs.tags = message.NewMonotoneTags()
	rs.remote = remote
	rs.received = sync.NewCond(&rs.mu)
	rs.responses = make(map[uint16]message.Message)
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

	// Nothing received, but goroutines waiting on this condition will check the
	// shutdown flag as well. This stops the receive loop as well.
	rs.received.Broadcast()
	rs.doing.Wait()

	rs.tags.Stop()
}

func (rs *RemoteVersionedStore) do(request message.Message) (response message.Message, err error) {
	rs.doing.Add(1)
	defer rs.doing.Done()
	if err := rs.remote.Send(request); err != nil {
		return response, err
	}
	var gotResponse bool
	rs.received.L.Lock()
	for {
		if rs.stopped {
			return response, ErrShutdown
		}
		response, gotResponse = rs.responses[request.Tag()]
		if gotResponse {
			delete(rs.responses, request.Tag())
			break
		}
		// TODO: What if the response never comes? Got read underflow for instance. Fails a receive and gets stuck.
		// Coming from a Symlink operation.
		rs.received.Wait()
	}
	rs.received.L.Unlock()
	return response, nil
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
		shutdown := rs.stopped
		rs.mu.Unlock()
		if shutdown {
			break
		}
		var m message.Message
		if err := rs.remote.Receive(&m); err != nil {
			// Timeouts can happen, as we're polling for messages.
			// Report other errors.
			if !errors.Is(err, client.ErrTimeout) {
				log.WithFields(log.Fields{
					"err": err,
				}).Error("receive error")
			}

			// TODO We should reconnect perhaps, since we can't synchronize in the stream?
			// Or perhaps the put can timeout (above) and give syscall.EIO.

			// Should have a back off strategy here.
			time.Sleep(250 * time.Millisecond)
			continue
		} else {
			rs.mu.Lock()
			rs.responses[m.Tag()] = m
			rs.mu.Unlock()
			rs.received.Broadcast()
		}
		if m.Tag() == 0 && m.Kind() == message.KindPut {
			lres := ApplyMessage(rs.local, m)
			if lres.Kind() == message.KindError {
				log.WithFields(log.Fields{
					"err": lres,
				}).Error("applying locally")
			} else if rs.listener != nil {
				rs.listener(lres)
			}
		}
	}
}
