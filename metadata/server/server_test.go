package server_test

import (
	"errors"
	"testing"
	"time"

	"github.com/nicolagi/dino/message"
	"github.com/nicolagi/dino/metadata/client"
	"github.com/nicolagi/dino/metadata/server"
	"github.com/nicolagi/dino/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestServer(t *testing.T) {
	t.Run("can be shutdown right after start", func(t *testing.T) {
		_, cleanup := newDisposableServer(t)
		defer cleanup()
	})
	t.Run("send error message", func(t *testing.T) {
		address, cleanup := newDisposableServer(t)
		defer cleanup()
		c := newAttachedClient(t, address)
		request := message.NewErrorMessage(431, "test error")
		assert.Nil(t, c.Send(request))
		var response message.Message
		require.Nil(t, c.Receive(&response))
		assert.Equal(t, message.KindError, response.Kind())
		assert.Equal(t, "error messages cannot be applied", response.Value())
	})
	t.Run("notify to closed connection", func(t *testing.T) {
		address, cleanup := newDisposableServer(t)
		defer cleanup()

		// Connect two clients.
		c1 := newAttachedClient(t, address)
		c2 := newAttachedClient(t, address)

		// Close c2, then send a put via c1.
		// This would attempt a notification on c2.
		require.Nil(t, c2.Close())
		req1 := message.NewPutMessage(1, "genre", "jazz", 1)
		require.Nil(t, c1.Send(req1))

		// Check req1 got a proper response on c1, despite propagation to c2
		// failing.
		var res1 message.Message
		require.Nil(t, c1.Receive(&res1))
		assert.Equal(t, req1, res1)
	})
	t.Run("one client puts, another client puts, conflict", func(t *testing.T) {
		address, cleanup := newDisposableServer(t)
		defer cleanup()

		// Send conflicting puts from two clients.
		client1 := newAttachedClient(t, address)
		client2 := newAttachedClient(t, address)
		req1 := message.NewPutMessage(42, "name", "Alberto", 0)
		req2 := message.NewPutMessage(42, "name", "Leonardo", 0)
		require.Nil(t, client1.Send(req1))
		require.Nil(t, client2.Send(req2))

		var res11, res12, res21, res22 message.Message
		err11 := client1.Receive(&res11)
		err12 := client1.Receive(&res12)
		err21 := client2.Receive(&res21)
		err22 := client2.Receive(&res22)

		// If the second response for client1 timed out, it means client1 did
		// the succesful put. If not, swap everything, so we can write
		// assertions more easily.
		if !errors.Is(err12, client.ErrTimeout) {
			err11, err12, err21, err22 = err21, err22, err11, err12
			res11, res12, res21, res22 = res21, res22, res11, res12
			req1, req2 = req2, req1
		}

		require.Nil(t, err11)
		require.True(t, errors.Is(err12, client.ErrTimeout))
		require.Nil(t, err21)
		require.Nil(t, err22)

		assert.Equal(t, req1, res11)

		// Not sure if client2 got the broadcast message or the response first.
		if res21.Kind() != message.KindError {
			res21, res22 = res22, res21
		}

		// Stale put
		assert.Equal(t, message.KindError, res21.Kind())
		assert.Equal(t, "stale put", res21.Value())
		// Broadcast
		assert.EqualValues(t, 0, res22.Tag())
		assert.Equal(t, message.KindPut, res22.Kind())
	})
	t.Run("one client puts, another one gets", func(t *testing.T) {
		address, cleanup := newDisposableServer(t)
		defer cleanup()

		// Use one client to connect and put a key.
		c1 := newAttachedClient(t, address)
		req1 := message.NewPutMessage(1, "username", "glenda", 1)
		require.Nil(t, c1.Send(req1))
		var res1 message.Message
		require.Nil(t, c1.Receive(&res1))
		assert.Equal(t, req1, res1)

		// Connect a new client, get, receive what was put by first client.
		c2 := newAttachedClient(t, address)
		require.Nil(t, c2.Send(message.NewGetMessage(1, "username")))
		var res2 message.Message
		require.Nil(t, c2.Receive(&res2))
		if res2.Tag() == 0 {
			// Race condition, we got the broadcast message.
			// That's fine, get another message in that case:
			require.Nil(t, c2.Receive(&res2))
		}
		assert.Equal(t, message.KindPut, res2.Kind())
		assert.EqualValues(t, 1, res2.Tag())
		assert.Equal(t, "username", res2.Key())
		assert.Equal(t, "glenda", res2.Value())
		assert.EqualValues(t, 1, res2.Version())

		// No more messages should be sent (in particular, c1 doesn't get
		// the broadcast message).
		assert.True(t, errors.Is(c1.Receive(&res1), client.ErrTimeout))
		assert.True(t, errors.Is(c2.Receive(&res2), client.ErrTimeout))
	})
	t.Run("successful put fans out to many clients", func(t *testing.T) {
		address, cleanup := newDisposableServer(t)
		defer cleanup()

		// Connect three clients.
		c1 := newAttachedClient(t, address)
		c2 := newAttachedClient(t, address)
		c3 := newAttachedClient(t, address)

		// One client makes a succesful put.
		req1 := message.NewPutMessage(1, "foo", "bar", 1)
		require.Nil(t, c1.Send(req1))

		// Everyone gets a message.
		var res1, res2, res3 message.Message
		require.Nil(t, c1.Receive(&res1))
		require.Nil(t, c2.Receive(&res2))
		require.Nil(t, c3.Receive(&res3))
		assert.Equal(t, req1, res1)
		assert.Equal(t, req1.ForBroadcast(), res2)
		assert.Equal(t, req1.ForBroadcast(), res3)

		// No more messages should be sent (in particular, c1 doesn't get
		// the broadcast message).
		assert.True(t, errors.Is(c1.Receive(&res1), client.ErrTimeout))
		assert.True(t, errors.Is(c2.Receive(&res2), client.ErrTimeout))
		assert.True(t, errors.Is(c3.Receive(&res3), client.ErrTimeout))
	})
}

func newDisposableServer(t *testing.T) (address string, cleanup func()) {
	store := storage.NewInMemoryStore()
	versionedStore := storage.NewVersionedWrapper(store)
	metadataServer := server.New(
		server.WithAddress("localhost:0"),
		server.WithVersionedStore(versionedStore),
	)
	address, err := metadataServer.Listen()
	require.Nil(t, err)
	errc := make(chan error, 1)
	go func() {
		errc <- metadataServer.Serve()
	}()
	return address, func() {
		assert.Nil(t, metadataServer.Shutdown())
		assert.Nil(t, <-errc)
	}
}

func newAttachedClient(t *testing.T, address string) (c *client.Client) {
	c, err := client.New(client.WithAddress(address), client.WithTimeout(500*time.Millisecond))
	require.Nil(t, err)
	return c
}
