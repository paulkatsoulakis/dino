package main

var (
	// 1 is reserved for the root.
	inodeNumber uint64 = 2

	// Keep a buffer of ready to consume inode numbers.
	inodeNumberChan = make(chan uint64, 42)
)

func nextInodeNumber() uint64 {
	return <-inodeNumberChan
}

func generateInodeNumbers() {
	// Will never exit.
	go func() {
		for {
			inodeNumberChan <- inodeNumber
			inodeNumber++
		}
	}()
}
