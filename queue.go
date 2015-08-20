package riak

type queue struct {
	queueSize uint16
	queueChan chan interface{}
}

func newQueue(queueSize uint16) *queue {
	if queueSize == 0 {
		panic("[queue] size must be greater than zero!")
	}
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
	select {
	case v, ok := <-q.queueChan:
		if !ok {
			return nil, newClientError("attempt to dequeue from closed queue")
		}
		return v, nil
	default:
		return nil, nil
	}
}

func (q *queue) iterate(f func(interface{}) (bool, bool)) error {
	for i := uint16(0); i < q.queueSize; i++ {
		v, err := q.dequeue()
		if err != nil {
			return err
		}
		if v == nil {
			// NB: empty queue
			break
		}
		brk, re_queue := f(v)
		if re_queue {
			err = q.enqueue(v)
			if err != nil {
				return err
			}
		}
		if brk {
			break
		}
	}
	return nil
}

func (q *queue) isEmpty() bool {
	return len(q.queueChan) == 0
}

func (q *queue) count() uint16 {
	return uint16(len(q.queueChan))
}

func (q *queue) destroy() {
	close(q.queueChan)
}
