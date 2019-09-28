package message_test

import (
	"testing"

	"github.com/nicolagi/dino/message"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMonotoneTags(t *testing.T) {
	t.Run("generate a hundred tags", func(t *testing.T) {
		tags := message.NewMonotoneTags()
		for i := 1; i <= 100; i++ {
			require.EqualValues(t, i, tags.Next())
		}
		tags.Stop()
	})
	t.Run("skips the reserved zero tag", func(t *testing.T) {
		tags := message.NewMonotoneTags()
		prev := tags.Next()
		require.EqualValues(t, 1, prev)
		curr := tags.Next()
		require.EqualValues(t, 2, curr)
		for {
			prev = curr
			curr = tags.Next()
			if curr == 1 {
				assert.EqualValues(t, 65535, prev)
				break
			}
		}
		tags.Stop()
	})
}
