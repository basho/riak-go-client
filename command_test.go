package riak

import (
	"sync"
	"testing"
)

func TestEnqueueDequeueCommandsConcurrently(t *testing.T) {
	queueSize := uint16(64)
	queue := newQueue(queueSize)

	w := &sync.WaitGroup{}
	for i := uint16(0); i < queueSize; i++ {
		w.Add(1)
		go func() {
			cmd := &PingCommand{}
			async := &Async{
				Command: cmd,
			}
			if err := queue.enqueue(async); err != nil {
				t.Error(err)
			}
			w.Done()
		}()
	}

	w.Wait()

	cmd := &PingCommand{}
	async := &Async{
		Command: cmd,
	}
	if err := queue.enqueue(async); err == nil {
		t.Error("expected non-nil err when enqueueing one more command than max")
	}
	if expected, actual := false, queue.isEmpty(); expected != actual {
		t.Errorf("expected %v, got %v", expected, actual)
	}

	w = &sync.WaitGroup{}
	for i := uint16(0); i < queueSize; i++ {
		w.Add(1)
		go func() {
			cmd, err := queue.dequeue()
			if cmd == nil {
				t.Error("expected non-nil cmd")
			}
			if err != nil {
				t.Error("expected nil err")
			}
			w.Done()
		}()
	}

	w.Wait()

	if expected, actual := true, queue.isEmpty(); expected != actual {
		t.Errorf("expected %v, got %v", expected, actual)
	}

	queue.destroy()

	_, err := queue.dequeue()
	if err == nil {
		t.Error("expected non-nil err")
	}
}
