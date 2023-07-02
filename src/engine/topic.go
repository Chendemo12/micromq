package engine

import (
	"container/list"
	"github.com/Chendemo12/fastapi-tool/helper"
	"sync"
	"time"
)

type Topic struct {
	Name        string
	HistorySize int
	offset      uint64
	queue       chan *Message
	consumers   *sync.Map
	history     *Queue
	mu          *sync.Mutex
}

func (t *Topic) makeOffset() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()

	of := t.offset
	t.offset++

	return of
}

func (t *Topic) AddConsumer(con *Consumer) {
	t.consumers.Store(con.Addr, con)
}

func (t *Topic) GetConsumer(addr string) *Consumer {
	v, ok := t.consumers.Load(addr)
	if !ok {
		return nil
	} else {
		return v.(*Consumer)
	}
}

func (t *Topic) DelConsumer(addr string) {
	t.consumers.Delete(addr)
}

func (t *Topic) Product(msg *Message) uint64 {
	offset := t.makeOffset()
	msg.Offset = offset
	msg.ProductTime = time.Now()

	t.queue <- msg

	return offset
}

func (t *Topic) Consume() {
	for msg := range t.queue {
		bytes, err := helper.JsonMarshal(msg)
		if err != nil {
			t.history.Append(msg)
			continue
		}

		t.consumers.Range(func(key, value any) bool {
			cons := value.(*Consumer)
			_, err2 := cons.Conn.Write(bytes)
			if err2 != nil {
			} else {
				go func() {
					_ = cons.Conn.Drain()
				}()
			}
			return true
		})

		t.history.Append(msg)
	}
}

func NewTopic(name string, historySize int) *Topic {
	t := &Topic{
		Name:        name,
		HistorySize: historySize,
		offset:      0,
		queue:       make(chan *Message, historySize),
		consumers:   &sync.Map{},
		history:     NewQueue(historySize),
		mu:          &sync.Mutex{},
	}
	go t.Consume()

	return t
}

type Queue struct {
	list     *list.List
	capacity int
	mu       *sync.Mutex
}

func NewQueue(capacity int) *Queue {
	return &Queue{
		list:     list.New(),
		capacity: capacity,
		mu:       &sync.Mutex{},
	}
}

func (q *Queue) Capacity() int { return q.capacity }

func (q *Queue) Length() int { return q.list.Len() }

func (q *Queue) Front() any { return q.list.Front() }

func (q *Queue) Append(value any) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.list.Len() >= q.capacity {
		q.list.Remove(q.list.Front())
	}
	q.list.PushBack(value)
}

func (q *Queue) PopLeft() any {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.list.Len() == 0 {
		return nil
	}

	element := q.list.Front()
	q.list.Remove(element)
	return element.Value
}
