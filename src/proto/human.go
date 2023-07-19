package proto

import (
	"time"
)

// HumanMessage 直接返回给调用者的消息定义
type HumanMessage interface {
	MessageType() MessageType         // 消息类别
	MarshalMethod() MarshalMethodType // 消息序列化方法
}

// ConsumerMessage 直接投递给消费者的单条数据消息
// 需要从 TransferFrame 中转换
type ConsumerMessage struct {
	Topic       string    `json:"topic"`
	Key         string    `json:"key"`
	Value       []byte    `json:"value"`
	Offset      uint64    `json:"offset"`
	ProductTime time.Time `json:"product_time"` // 服务端收到消息时的时间戳
}

func (m *ConsumerMessage) MessageType() MessageType         { return CMessageType }
func (m *ConsumerMessage) MarshalMethod() MarshalMethodType { return JsonMarshalMethod }

func (m *ConsumerMessage) Reset() {
	m.Topic = ""
	m.Key = ""
	m.Value = nil
	m.Offset = 0
}

// ProducerMessage 生产者直接发送的数据
// 会转换成 TransferFrame 后发送
type ProducerMessage struct {
	Topic string `json:"topic"`
	Key   string `json:"key"`
	Value []byte `json:"value"`
}

func (m *ProducerMessage) MessageType() MessageType         { return PMessageType }
func (m *ProducerMessage) MarshalMethod() MarshalMethodType { return JsonMarshalMethod }

func (m *ProducerMessage) Reset() {
	m.Topic = ""
	m.Key = ""
	m.Value = nil
}
