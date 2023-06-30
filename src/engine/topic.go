package engine

import (
	"container/list"
	"github.com/Chendemo12/functools/tcp"
	"sync"
	"time"
)

type Message struct {
	Topic       string
	Offset      uint64
	Key         []byte
	Value       []byte
	ProductTime time.Time
}

type Consumer interface {
	Topics() []string
	Handler(msg *Message)
}

type _consumer struct {
	meta Consumer
	tcp  *tcp.Remote
}

func (c *_consumer) setConn(r *tcp.Remote) {
	c.tcp = r
}

func NewTopic(name string, historySize int) *Topic {
	return &Topic{
		Name:        name,
		HistorySize: historySize,
		offset:      0,
		queue:       make(chan *Message, historySize),
		consumers:   make([]*_consumer, 0),
		history:     NewQueue(historySize),
		mu:          &sync.Mutex{},
	}
}

type Topic struct {
	Name        string
	HistorySize int
	offset      uint64
	queue       chan *Message
	consumers   []*_consumer
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

func (t *Topic) AddConsumer(con Consumer) {
	t.consumers = append(t.consumers, &_consumer{
		meta: con,
	})
}

func (t *Topic) Product(key, value []byte) {
	msg := &Message{
		Topic:       t.Name,
		Offset:      t.makeOffset(),
		Key:         key,
		Value:       value,
		ProductTime: time.Now(),
	}
	t.queue <- msg
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
