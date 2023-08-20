package sdk

import (
	"encoding/binary"
	"fmt"
	"github.com/Chendemo12/fastapi-tool/helper"
	"github.com/Chendemo12/micromq/src/proto"
	"time"
)

// ConsumerMessage 用于SDK直接传递给消费者的单条数据消息
// 需要从 TransferFrame 中转换
type ConsumerMessage struct {
	counter     uint64    // 用以追踪此对象的实例是否由池创建
	Topic       string    `json:"topic"`
	Key         string    `json:"key"`
	Value       []byte    `json:"value"`
	Offset      uint64    `json:"offset"`
	ProductTime time.Time `json:"product_time"` // 服务端收到消息时的时间戳
}

func (m *ConsumerMessage) String() string {
	// "<message:ConsumerMessage> on [ T::DNS_UPDATE | K::2023-07-22T12:23:48.767 | O::2342 ] with 200 bytes of payload"
	return fmt.Sprintf(
		"<message:%s> on [ T::%s | K::%s | O::%d ] with %d bytes of payload",
		proto.GetDescriptor(m.MessageType()).Text(), m.Topic, m.Key, m.Offset, len(m.Value),
	)
}

func (m *ConsumerMessage) MessageType() proto.MessageType { return proto.CMessageType }

func (m *ConsumerMessage) MarshalMethod() proto.MarshalMethodType {
	return proto.JsonMarshalMethod
}

func (m *ConsumerMessage) ParseFromCMessage(cm *proto.CMessage) {
	m.Topic = string(cm.PM.Topic)
	m.Key = string(cm.PM.Key)
	m.Value = cm.PM.Value
	m.Offset = binary.BigEndian.Uint64(cm.Offset)
	m.ProductTime = time.Unix(int64(binary.BigEndian.Uint64(cm.ProductTime)), 0)
}

func (m *ConsumerMessage) Reset() {
	m.Topic = ""
	m.Key = ""
	m.Value = nil
	m.Offset = 0
}

// ShouldBindJSON 将数据反序列化到一个JSON模型上
func (m *ConsumerMessage) ShouldBindJSON(v any) error {
	return helper.JsonUnmarshal(m.Value, v)
}

// ProducerMessage 生产者直接发送的数据
// 会转换成 TransferFrame 后发送
type ProducerMessage struct {
	counter uint64 // 用以追踪此对象的实例是否由池创建
	Topic   string `json:"topic"`
	Key     string `json:"key"`
	Value   []byte `json:"value"`
}

func (m *ProducerMessage) String() string {
	// "<message:ConsumerMessage> on [ T::DNS_UPDATE | K::2023-07-22T12:23:48.767 ] with 200 bytes of payload"
	return fmt.Sprintf(
		"<message:%s> on [ T::%s | K::%s ] with %d bytes of payload",
		proto.GetDescriptor(m.MessageType()).Text(), m.Topic, m.Key, len(m.Value),
	)
}

func (m *ProducerMessage) MessageType() proto.MessageType { return proto.PMessageType }

func (m *ProducerMessage) MarshalMethod() proto.MarshalMethodType {
	return proto.JsonMarshalMethod
}

func (m *ProducerMessage) Reset() {
	m.Topic = ""
	m.Key = ""
	m.Value = nil
}

// BindFromJSON 从JSON模型获取序列化数据
func (m *ProducerMessage) BindFromJSON(v any) error {
	_bytes, err := helper.JsonMarshal(v)
	if err != nil {
		return err
	}
	m.Value = _bytes

	return nil
}
