package util

import (
	"os"
	"os/signal"
	"sync"
	"syscall"
)

func ConcurrentRun(fn func(), num int) {
	if num <= 0 {
		num = 1
	}
	var w sync.WaitGroup
	w.Add(num)
	for i := 0; i < num; i++ {
		go func() {
			fn()
			w.Done()
		}()
	}
	w.Wait()
}

func WrapSignal(fn func()) {
	ch := make(chan os.Signal, 1)
	signal.Notify(
		ch,
		os.Interrupt,
		syscall.SIGINT,
		syscall.SIGTERM,
	)
	go fn()
	<-ch
}
