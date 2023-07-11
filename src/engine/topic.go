package engine

import (
	"container/list"
	"github.com/Chendemo12/fastapi-tool/helper"
	"github.com/Chendemo12/synshare-mq/src/proto"
	"sync"
	"sync/atomic"
	"time"
)

type Counter struct {
	count uint64
}

func (c *Counter) Increment() {
	atomic.AddUint64(&c.count, 1)
}

func (c *Counter) Value() uint64 {
	return atomic.LoadUint64(&c.count)
}

func (c *Counter) ValueBeforeIncrement() uint64 {
	v := c.Value()
	c.Increment()
	return v
}

func (c *Counter) ValueAfterIncrement() uint64 {
	c.Increment()
	return c.Value()
}

type Event struct {
	q chan struct{}
}

func (e *Event) IsSet() bool { return len(e.q) > 0 }
func (e *Event) Set()        { e.q <- struct{}{} }
func (e *Event) Wait()       { <-e.q }

func NewEvent() *Event { return &Event{q: make(chan struct{})} }

type Topic struct {
	Name           string              // 唯一标识
	BufferSize     int                 // 生产者消息缓冲区大小
	offset         uint64              // 当前数据偏移量
	counter        *Counter            // 生产者消息计数器,用于计算数据偏移量
	consumers      *sync.Map           // 全部消费者: {addr: Consumer}, TODO: slice
	consumerQueue  chan *proto.Message // 等待消费者消费的数据, 若生产者消息发生了挤压,则会丢弃最早的未消费的数据
	publisherQueue *Queue              // 生产者生产的数据
	publisherEvent *Event
	mu             *sync.Mutex
}

func (t *Topic) makeOffset() uint64 {
	t.offset = t.counter.ValueBeforeIncrement()
	return t.offset
}

func (t *Topic) msgMove() {
	for {
		t.publisherEvent.Wait()
		msg := t.publisherQueue.Front()
		if msg != nil {
			t.consumerQueue <- msg.(*proto.Message)
		}
	}
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

func (t *Topic) RemoveConsumer(addr string) {
	t.consumers.Delete(addr)
}

func (t *Topic) Publisher(msg *proto.Message) uint64 {
	offset := t.makeOffset()
	msg.Offset = offset
	msg.ProductTime = time.Now()

	// 若缓冲区已满, 则丢弃最早的数据
	t.publisherQueue.Append(msg)
	t.publisherEvent.Set()

	return offset
}

func (t *Topic) Consume() {
	for msg := range t.consumerQueue {
		bytes, err := helper.JsonMarshal(msg)
		mPool.Put(msg)
		if err != nil {
			continue
		}

		t.consumers.Range(func(key, value any) bool {
			cons, ok := value.(*Consumer)
			if !ok {
				return true
			}
			go func() {
				_, _ = cons.Conn.Write(bytes)
				_ = cons.Conn.Drain()
			}()

			return true
		})
	}
}

func NewTopic(name string, bufferSize int) *Topic {
	t := &Topic{
		Name:           name,
		BufferSize:     bufferSize,
		offset:         0,
		counter:        &Counter{count: 0},
		consumers:      &sync.Map{},
		consumerQueue:  make(chan *proto.Message, bufferSize),
		publisherQueue: NewQueue(bufferSize),
		publisherEvent: NewEvent(),
		mu:             &sync.Mutex{},
	}
	go t.msgMove()
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
