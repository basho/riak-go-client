package riak

import (
	"sync"
	"testing"
)

func TestReadFromEmptyQueue(t *testing.T) {
	q := newQueue(1)
	v, err := q.dequeue()
	if err != nil {
		t.Error("expected nil error when reading from empty queue")
	}
	if v != nil {
		t.Error("expected nil value when reading from empty queue")
	}
	if expected, actual := uint16(0), q.count(); expected != actual {
		t.Errorf("expected %v, got %v", expected, actual)
	}
}

func TestIterateEmptyQueue(t *testing.T) {
	count := uint16(128)
	q := newQueue(count)
	executed := false
	var f = func(val interface{}) (bool, bool) {
		executed = true
		return false, true
	}
	err := q.iterate(f)
	if err != nil {
		t.Error("expected nil error when iterating queue")
	}
	if expected, actual := false, executed; expected != actual {
		t.Errorf("expected %v, got %v", expected, actual)
	}
	if expected, actual := uint16(0), q.count(); expected != actual {
		t.Errorf("expected %v, got %v", expected, actual)
	}
}

func TestConcurrentIterateQueue(t *testing.T) {
	count := uint16(128)
	wg := &sync.WaitGroup{}
	q := newQueue(count)
	for i := uint16(0); i < count; i++ {
		q.enqueue(i)
	}

	for i := uint16(0); i < count; i++ {
		wg.Add(1)
		go func() {
			c := uint16(0)
			var f = func(val interface{}) (bool, bool) {
				c++
				j := val.(uint16)
				if j == 64 {
					// won't re-enqueue value 64
					return false, false
				} else {
					return false, true
				}
			}
			err := q.iterate(f)
			if err != nil {
				t.Error("expected nil error when iterating queue")
			}
			if expected, actual := count, c; expected != actual {
				t.Errorf("expected %v, got %v", expected, actual)
			}
			wg.Done()
		}()
	}

	wg.Wait()
	if expected, actual := count-1, q.count(); expected != actual {
		t.Errorf("expected %v, got %v", expected, actual)
	}
}
