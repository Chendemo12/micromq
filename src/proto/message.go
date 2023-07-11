package proto

import (
	"sync"
	"time"
)

type NoCopy struct{}

func (*NoCopy) Lock()   {}
func (*NoCopy) Unlock() {}

// Message 消费者或生产者消息记录, 不允许复制
type Message struct {
	noCopy      NoCopy
	Topic       string
	Key         []byte
	Value       []byte
	Offset      uint64
	ProductTime time.Time
}

func (m *Message) Reset() {
	m.Topic = ""
	m.Offset = 0
	m.Key = nil
	m.Value = nil
}

type MessageResponse struct {
	noCopy      NoCopy
	Result      bool      `json:"result"`
	Offset      uint64    `json:"offset,omitempty"`
	ReceiveTime time.Time `json:"receive_time,omitempty"`
}

func (m *MessageResponse) Reset() {
	m.Result = false
	m.Offset = 0
}

// RegisterMessage 消息注册,适用于生产者和消费者
type RegisterMessage struct {
	Topics []string `json:"topics"` // 对于生产者,无意义
	Ack    AckType  `json:"ack"`
	Type   LinkType `json:"type"`
}

type MessagePool struct {
	pool *sync.Pool
}

func (m *MessagePool) Get() *Message {
	v := m.pool.Get().(*Message)
	v.Reset()
	v.ProductTime = time.Now()
	return v
}

func (m *MessagePool) Put(v *Message) {
	v.Reset()
	m.pool.Put(v)
}

func NewMessagePool() *MessagePool {
	return &MessagePool{
		pool: &sync.Pool{New: func() any { return &Message{} }},
	}
}

type MessageRespPool struct {
	pool *sync.Pool
}

func (m *MessageRespPool) Get() *MessageResponse {
	v := m.pool.Get().(*MessageResponse)
	v.Reset()
	return v
}

func (m *MessageRespPool) Put(v *MessageResponse) {
	m.pool.Put(v)
}

func NewMessageRespPool() *MessageRespPool {
	return &MessageRespPool{
		pool: &sync.Pool{New: func() any { return &MessageResponse{} }},
	}
}
