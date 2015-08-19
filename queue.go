package riak

type queue struct {
	queueSize uint16
	queueChan chan interface{}
}

func newQueue(queueSize uint16) *queue {
	return &queue{
		queueSize: queueSize,
		queueChan: make(chan interface{}, queueSize),
	}
}

func (q *queue) enqueue(v interface{}) error {
	if v == nil {
		panic("attempt to enqueue nil value")
	}
	if len(q.queueChan) == int(q.queueSize) {
		return newClientError("attempt to enqueue when queue is full")
	}
	q.queueChan <- v
	return nil
}

func (q *queue) dequeue() (interface{}, error) {
	v, ok := <-q.queueChan
	if !ok {
		return nil, newClientError("attempt to dequeue from closed queue")
	}
	return v, nil
}

func (q *queue) isEmpty() bool {
	return len(q.queueChan) == 0
}

func (q *queue) destroy() {
	close(q.queueChan)
}
