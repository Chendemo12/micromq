package engine

import (
	"sync"
	"time"
)

const (
	RegisterMessageType       byte = 0
	RegisterMessageRespType   byte = 1
	ProductionMessageType     byte = 100
	ProductionMessageRespType byte = 101
)

type Message struct {
	Topic       string
	Offset      uint64
	Key         []byte
	Value       []byte
	ProductTime time.Time
}

type MessageResponse struct {
	Result      bool      `json:"result"`
	Offset      uint64    `json:"offset,omitempty"`
	ReceiveTime time.Time `json:"receive_time,omitempty"`
}

type AckType string

const (
	NoConfirm     AckType = "0"
	LeaderConfirm AckType = "1"
	AllConfirm    AckType = "all"
)

type RegisterMessage struct {
	Topics []string `json:"topics"`
	Ack    AckType  `json:"ack"`
}

// --------------------------------------------------------------------------------------

type messagePool struct {
	poll *sync.Pool
}

func (m *messagePool) Get() *Message {
	v := m.poll.Get().(*Message)
	v.ProductTime = time.Now()
	return v
}

func (m *messagePool) Put(v *Message) {
	v.Topic = ""
	v.Key = nil
	v.Value = nil
	v.Offset = 0
	m.poll.Put(v)
}

var mPool = &messagePool{
	poll: &sync.Pool{
		New: func() any { return &Message{} },
	},
}
