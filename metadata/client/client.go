package client

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/nicolagi/dino/message"
)

var (
	ErrAttached = errors.New("client is attached")
	ErrDetached = errors.New("client is detached")
	ErrTimeout  = errors.New("timeout")
)

type options struct {
	address string
	timeout time.Duration
}

type Option func(*options)

func WithAddress(value string) Option {
	return func(o *options) {
		o.address = value
	}
}

func WithTimeout(value time.Duration) Option {
	return func(o *options) {
		o.timeout = value
	}
}

// Client is a low-level metadata server client that can send and receive
// message.Message's. It can be used to build higher level clients, e.g., a
// storage.VersionedStore implementation.
type Client struct {
	opts options

	emu     sync.Mutex
	encoder *message.Encoder

	dmu     sync.Mutex
	decoder *message.Decoder

	cmu  sync.Mutex
	conn net.Conn
}

func New(opts ...Option) (*Client, error) {
	var c Client
	c.opts.address = "127.0.0.1:6660"
	c.encoder = new(message.Encoder)
	c.decoder = new(message.Decoder)
	for _, o := range opts {
		o(&c.opts)
	}
	conn, err := net.Dial("tcp", c.opts.address)
	if err != nil {
		return nil, err
	}
	c.conn = conn
	return &c, nil
}

// Detach closes the connection. Returns an error if already detached. The
// client must be considered detached even if this method returns an error.
func (c *Client) Close() error {
	c.cmu.Lock()
	defer c.cmu.Unlock()
	if c.conn == nil {
		return nil
	}
	err := c.conn.Close()
	c.conn = nil
	return err
}

// Send sends the message to the server. Requires the client to be attached. Non
// temporary errors will detach the client.
func (c *Client) Send(m message.Message) error {
	return c.do(func(conn net.Conn) error {
		c.emu.Lock()
		err := c.encoder.Encode(conn, m)
		defer c.emu.Unlock()
		return err
	})
}

// Receive receives a message from the server. Requires the client to be
// attached. Non temporary errors will detach the client.
func (c *Client) Receive(m *message.Message) error {
	return c.do(func(conn net.Conn) error {
		c.dmu.Lock()
		err := c.decoder.Decode(conn, m)
		c.dmu.Unlock()
		return err
	})
}

func (c *Client) do(fn func(net.Conn) error) error {
	c.cmu.Lock()
	if c.conn == nil {
		conn, err := net.Dial("tcp", c.opts.address)
		if err != nil {
			return err
		}
		c.conn = conn
	}
	conn := c.conn
	c.cmu.Unlock()
	if err := conn.SetDeadline(time.Now().Add(c.opts.timeout)); err != nil {
		return err
	}
	err := fn(conn)
	if operr, ok := err.(*net.OpError); ok {
		if operr.Timeout() {
			return ErrTimeout
		}
		if !operr.Temporary() {
			_ = conn.Close()
			_ = c.Close()
			return fmt.Errorf("error not temporary: %q, hence %w", operr.Error(), ErrDetached)
		}
	}
	return err
}
