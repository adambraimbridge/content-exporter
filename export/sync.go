package export

import "sync"

type Locker struct {
	Locked chan bool
	Acked  chan struct{}
}

func NewLocker() *Locker {
	lockedCh := make(chan bool)
	ackedCh := make(chan struct{})
	return &Locker{
		Locked: lockedCh,
		Acked:  ackedCh,
	}
}

type Terminator struct {
	Quit             chan struct{}
	Cleanup          sync.Once
	ShutDownPrepared bool
	ShutDown         bool
}

func NewTerminator() *Terminator {
	quitCh := make(chan struct{}, 2)
	return &Terminator{
		Quit: quitCh,
	}
}
