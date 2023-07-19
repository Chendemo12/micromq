package proto

import (
	"container/list"
	"sync"
	"sync/atomic"
)

type NoCopy struct{}

func (*NoCopy) Lock()   {}
func (*NoCopy) Unlock() {}

// BuildPMessages 构造生产者消息序列
// TODO: 改为 io.Writer
func BuildPMessages(pms ...*PMessage) (slice []byte) {
	//		TopicLength byte
	//		Topic       []byte
	//		KeyLength   byte
	//		Key         []bytes
	//		ValueLength uint16
	//		Value       []byte
	sliceLength := 0
	for _, m := range pms {
		sliceLength += 4 // TopicLength + KeyLength + ValueLength
		sliceLength += len(m.Topic)
		sliceLength += len(m.Key)
		sliceLength += len(m.Value)
	}
	slice = make([]byte, 0, sliceLength) // 分配最大长度

	for _, m := range pms {
		slice = append(slice, byte(len(m.Topic)))
		slice = append(slice, m.Topic...)
		slice = append(slice, byte(len(m.Key)))
		slice = append(slice, m.Key...)
		slice = append(slice, byte(len(m.Value)))
		slice = append(slice, m.Value...)
	}

	return slice
}

// BuildCMessages 构造消产者消息序列
// TODO: 改为 io.Writer
func BuildCMessages(cms ...*CMessage) (slice []byte) {
	//		TopicLength byte
	//		Topic       []byte
	//		KeyLength   byte
	//		Key         []byte
	//		ValueLength uint16
	//		Value       []byte
	//		Offset      uint64
	//		ProductTime int64 // time.Time.Unix()
	sliceLength := 0
	for _, m := range cms {
		sliceLength += m.Length()
	}
	slice = make([]byte, 0, sliceLength) // 分配最大长度

	for _, m := range cms {
		slice = append(slice, byte(len(m.Pm.Topic)))
		slice = append(slice, m.Pm.Topic...)
		slice = append(slice, byte(len(m.Pm.Key)))
		slice = append(slice, m.Pm.Key...)
		slice = append(slice, byte(len(m.Pm.Value)))
		slice = append(slice, m.Pm.Value...)
		slice = append(slice, m.Offset[:7]...)
		slice = append(slice, m.ProductTime[:7]...)
	}
	return slice
}

// CalcChecksum 经典校验和算法
func CalcChecksum(data []byte) uint16 {
	sum := 0
	for i := 0; i < len(data); i += 2 {
		if i+1 == len(data) {
			sum += int(data[i])
		} else {
			sum += int(data[i])<<8 + int(data[i+1])
		}
	}
	sum = (sum >> 16) + (sum & 0xffff)
	sum += sum >> 16

	return uint16(^sum)
}

func NewCounter() *Counter { return &Counter{count: 0} }

// Counter 计数器
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

func NewQueue(capacity int) *Queue {
	return &Queue{
		list:     list.New(),
		capacity: capacity,
		mu:       &sync.Mutex{},
	}
}

type Queue struct {
	list     *list.List
	capacity int
	mu       *sync.Mutex
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

// ParsePMFrame 循环解析生产者消息
func ParsePMFrame(pms *[]*PMessage, content []byte) {
	// TODO:

}

// ParseCMFrame 循环解析消费者消息
func ParseCMFrame(cms *[]*CMessage, content []byte) {
	// TODO:

}

func NewCRegisterMessage(topics ...string) *RegisterMessage {
	return &RegisterMessage{
		Topics: topics,
		Ack:    AllConfirm,
		Type:   ConsumerLinkType,
	}
}

func NewPRegisterMessage() *RegisterMessage {
	return &RegisterMessage{
		Topics: []string{},
		Ack:    AllConfirm,
		Type:   ProducerLinkType,
	}
}
