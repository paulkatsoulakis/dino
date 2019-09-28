package message

import (
	"errors"
	"fmt"
	"io"

	"github.com/nicolagi/dino/bits"
)

var (
	// ErrUnderflow is returned when not all bytes can be written (in the encoder)
	// or read (in the decoder).
	ErrUnderflow = errors.New("underflow")

	// ErrBadMessage could be returned by Encoder.Encode, which would never happen
	// if messages are constructed via the provided helper message, e.g.,
	// NewGetMessage.
	ErrBadMessage = errors.New("bad message")
)

type Encoder struct {
	buf []byte
	off int
}

func (e *Encoder) Encode(w io.Writer, m Message) error {
	e.off = 0
	e.makeroom(3)
	e.put8(uint8(m.kind))
	e.put16(m.tag)
	switch m.kind {
	case KindGet:
		e.makeroom(e.off + 2 + len(m.key))
		e.puts(m.key)
	case KindPut:
		e.makeroom(e.off + 12 + len(m.key) + len(m.value))
		e.puts(m.key)
		e.puts(m.value)
		e.put64(m.version)
	case KindError:
		e.makeroom(e.off + 2 + len(m.value))
		e.puts(m.value)
	default:
		return ErrBadMessage
	}
	n, err := w.Write(e.buf[:e.off])
	if err != nil {
		return err
	}
	if n != e.off {
		return fmt.Errorf("wrote %d of %d bytes: %w", n, e.off, ErrUnderflow)
	}
	return nil
}

func (e *Encoder) makeroom(required int) {
	if len(e.buf) >= required {
		return
	}
	larger := make([]byte, required)
	copy(larger, e.buf)
	e.buf = larger
}

func (e *Encoder) put8(v uint8) {
	bits.Put8(e.buf[e.off:], v)
	e.off++
}

func (e *Encoder) put16(v uint16) {
	bits.Put16(e.buf[e.off:], v)
	e.off += 2
}

func (e *Encoder) put64(v uint64) {
	bits.Put64(e.buf[e.off:], v)
	e.off += 8
}

func (e *Encoder) puts(v string) {
	bits.Puts(e.buf[e.off:], v)
	e.off += 2 + len(v)
}

type Decoder struct {
	buf []byte
	off int

	// For each Decode call, contains the first read error or underflow error.
	// Reset to nil at the beginning of each Decode call.
	err error
}

func (d *Decoder) Decode(r io.Reader, m *Message) error {
	d.err = nil
	d.read(r, 5)
	m.kind = Kind(d.get8())
	m.tag = d.get16()
	switch m.kind {
	case KindGet:
		n := d.get16()
		d.read(r, n)
		m.key = d.gets(n)
	case KindPut:
		n := d.get16()
		d.read(r, n+2)
		m.key = d.gets(n)
		n = d.get16()
		d.read(r, n+8)
		m.value = d.gets(n)
		m.version = d.get64()
	case KindError:
		n := d.get16()
		d.read(r, n)
		m.value = d.gets(n)
	}
	return d.err
}

func (d *Decoder) get8() uint8 {
	v, _ := bits.Get8(d.buf[d.off:])
	d.off++
	return v
}

func (d *Decoder) get16() uint16 {
	v, _ := bits.Get16(d.buf[d.off:])
	d.off += 2
	return v
}

func (d *Decoder) get64() uint64 {
	v, _ := bits.Get64(d.buf[d.off:])
	d.off += 8
	return v
}

func (d *Decoder) gets(n uint16) string {
	b := d.buf[d.off : d.off+int(n)]
	d.off += int(n)
	return string(b)
}

func (d *Decoder) read(r io.Reader, n uint16) {
	if uint16(len(d.buf)-d.off) < n {
		larger := make([]byte, uint16(d.off)+n)
		copy(larger, d.buf)
		d.buf = larger
	}

	if d.err != nil {
		return
	}

	// Assume all buffer consumed (that's why we read).
	d.off = 0

	var m int
	m, d.err = io.ReadFull(r, d.buf[:n])
	if d.err == nil && uint16(m) != n {
		d.err = fmt.Errorf("read %d of %d bytes: %w", m, n, ErrUnderflow)
	}
}
