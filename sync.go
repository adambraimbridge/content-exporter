package main

type Locker struct {
	locked chan bool
	acked  chan struct{}
}

func NewLocker() *Locker {
	lockedCh := make(chan bool)
	ackedCh := make(chan struct{})
	return &Locker{
		locked: lockedCh,
		acked:  ackedCh,
	}
}
