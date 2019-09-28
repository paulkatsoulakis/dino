package message_test

import (
	"bytes"
	"io"
	"math/rand"
	"testing"
	"time"

	"github.com/nicolagi/dino/message"
	"github.com/stretchr/testify/assert"
)

func TestMessageWhatYouEncodeIsWhatYouDecode(t *testing.T) {

	testWithNewEncoderAndDecoder := func(t *testing.T, before message.Message) {
		var buf bytes.Buffer
		err := new(message.Encoder).Encode(&buf, before)
		assert.Nil(t, err)
		var after message.Message
		err = new(message.Decoder).Decode(&buf, &after)
		assert.Nil(t, err)
		assert.Equal(t, before, after)
	}

	test := func(t *testing.T, encoder *message.Encoder, decoder *message.Decoder, rw io.ReadWriter, before message.Message) {
		var after message.Message
		assert.Nil(t, encoder.Encode(rw, before))
		assert.Nil(t, decoder.Decode(rw, &after))
		assert.Equal(t, before, after)
	}

	const iters = 100

	rand.Seed(time.Now().UnixNano())

	var buf bytes.Buffer
	encoder := new(message.Encoder)
	decoder := new(message.Decoder)

	t.Run("pack and unpack get messages", func(t *testing.T) {
		for i := 0; i < iters; i++ {
			m := message.NewGetMessage(message.RandomTag(), message.RandomString())
			testWithNewEncoderAndDecoder(t, m)
			test(t, encoder, decoder, &buf, m)
		}
	})

	t.Run("pack and unpack put messages", func(t *testing.T) {
		for i := 0; i < iters; i++ {
			m := message.NewPutMessage(message.RandomTag(), message.RandomString(), message.RandomString(), message.RandomVersion())
			testWithNewEncoderAndDecoder(t, m)
			test(t, encoder, decoder, &buf, m)
		}
	})

	t.Run("pack and unpack error messages", func(t *testing.T) {
		for i := 0; i < iters; i++ {
			m := message.NewErrorMessage(message.RandomTag(), message.RandomString())
			testWithNewEncoderAndDecoder(t, m)
			test(t, encoder, decoder, &buf, m)
		}
	})
}
