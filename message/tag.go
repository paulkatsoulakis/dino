package message

// MonotoneTags provides thread-safe increasing tag numbers. It's a facility for
// metadataserver clients. (Each client should generate messages with distinct
// tags. Tags are used to correlate requests with responses.)
type MonotoneTags struct {
	next chan uint16
	stop chan struct{}
}

func NewMonotoneTags() *MonotoneTags {
	var tags MonotoneTags
	tags.next = make(chan uint16, 42)
	tags.stop = make(chan struct{})
	go func() {
		var tag uint16 = 1
		for {
			select {
			case tags.next <- tag:
				tag++
				if tag == 0 {
					tag++
				}
			case <-tags.stop:
				return
			}
		}
	}()
	return &tags
}

// Next returns the next tag. It will loop around and start back from 1 after
// 65535. The zero tag is reserved for broadcast messages (not answers to any
// request). (Hopefully no client will have more than 65535 requests in flight.)
func (t *MonotoneTags) Next() uint16 {
	return <-t.next
}

// Stop stops the goroutine that generates tag numbers. Calling Next after Stop
// will panic.
func (t *MonotoneTags) Stop() {
	close(t.stop)
}
