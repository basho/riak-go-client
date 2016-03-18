package riak

import (
	"sync"
	"time"

	"github.com/basho/backoff"
)

// Async object is used to pass required arguments to execute a Command asynchronously
type Async struct {
	Command    Command
	Done       chan Command
	Wait       *sync.WaitGroup
	Error      error
	rb         *backoff.Backoff // rb - Retry Backoff
	enqueuedAt time.Time
	executeAt  time.Time
	qb         *backoff.Backoff // qb - Queue Backoff
}

func (a *Async) onExecute() {
	if a.rb == nil {
		a.rb = &backoff.Backoff{
			Jitter: true,
		}
	} else {
		a.rb.Reset()
	}
}

func (a *Async) onRetry() {
	d := a.rb.Duration()
	logDebug("[Async]", "onRetry cmd: %s sleep: %v", a.Command.Name(), d)
	time.Sleep(d)
}

func (a *Async) onEnqueued() {
	if a.qb == nil {
		a.enqueuedAt = time.Now()
		a.qb = &backoff.Backoff{
			Factor: 1.5,
			Jitter: true,
		}
	}
	a.executeAt = a.enqueuedAt.Add(a.qb.Duration())
}

func (a *Async) done(err error) {
	if err != nil {
		// TODO FUTURE evaluate debug logging
		// logDebugln("[Async]", "done error:", err)
		a.Error = err
	}
	if a.Done != nil {
		// TODO FUTURE evaluate debug logging
		// logDebug("[Async]", "signaling a.Done channel with '%s'", a.Command.Name())
		a.Done <- a.Command
	}
	if a.Wait != nil {
		// TODO FUTURE evaluate debug logging
		// logDebug("[Async]", "signaling a.Wait WaitGroup for '%s'", a.Command.Name())
		a.Wait.Done()
	}
}
