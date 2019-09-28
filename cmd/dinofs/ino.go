package main

type inodeNumbersGenerator struct {
	nextc chan uint64
	stopc chan struct{}
}

func newInodeNumbersGenerator() *inodeNumbersGenerator {
	g := &inodeNumbersGenerator{
		// Keep a buffer of ready to consume inode numbers.
		nextc: make(chan uint64, 42),
		stopc: make(chan struct{}),
	}
	return g
}

func (g *inodeNumbersGenerator) start() {
	// 1 is reserved for the root.
	var ino uint64 = 2
	for {
		select {
		case g.nextc <- ino:
			// No check. I don't think we'll run out if inode numbers.
			ino++
		case <-g.stopc:
			return
		}
	}
}
func (g *inodeNumbersGenerator) stop() {
	close(g.stopc)
}

func (g *inodeNumbersGenerator) next() uint64 {
	return <-g.nextc
}
