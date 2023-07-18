// Package proto 若涉及到字节序,则全部为大端序
package proto

import (
	"fmt"
	"io"
	"time"
)

type Message interface {
	MessageType() MessageType         // 消息类别
	MarshalMethod() MarshalMethodType // 消息序列化方法
	Length() int                      // 消息长度
	Reset()                           // 重置消息体
	ParseFrom(reader io.Reader) error // 从流中解析一个消息
}

// PMessage 生产者消息数据, 不允许复制
type PMessage struct {
	noCopy NoCopy
	Topic  []byte // 字符串转字节
	Key    []byte
	Value  []byte
}

func (m *PMessage) MessageType() MessageType         { return PMessageType }
func (m *PMessage) MarshalMethod() MarshalMethodType { return BinaryMarshalMethod }

func (m *PMessage) Length() int { return len(m.Topic) + len(m.Key) + len(m.Value) }

func (m *PMessage) Reset() {
	m.Topic = nil
	m.Key = nil
	m.Value = nil
}

func (m *PMessage) ParseFrom(reader io.Reader) error {
	// 		TopicLength byte
	//		Topic       []byte
	//		KeyLength   byte
	//		Key         []byte
	//		ValueLength uint16
	//		Value       []byte

	bc := bcPool.Get()
	defer bcPool.Put(bc)

	// topicLength , topic
	i, err := reader.Read(bc.oneByte)
	if err != nil {
		return fmt.Errorf("topic did not read: %v", err)
	}
	m.Topic = make([]byte, i)
	i, err = reader.Read(m.Topic)
	if err != nil {
		return fmt.Errorf("topic did not read: %v", err)
	}

	// keyLength , key
	i, err = reader.Read(bc.oneByte)
	if err != nil {
		return fmt.Errorf("key did not read: %v", err)
	}
	m.Key = make([]byte, i)
	i, err = reader.Read(m.Key)
	if err != nil {
		return fmt.Errorf("key did not read: %v", err)
	}

	// valueLength , value
	i, err = reader.Read(bc.twoByte)
	if err != nil {
		return fmt.Errorf("value did not read: %v", err)
	}
	m.Value = make([]byte, i)
	i, err = reader.Read(m.Value)
	if err != nil {
		return fmt.Errorf("key did not read: %v", err)
	}

	return nil
}

// CMessage 消费者消息记录, 不允许复制
type CMessage struct {
	Offset      []byte // uint64
	ProductTime []byte // time.Time.Unix() 消息创建的Unix时间戳
	Pm          *PMessage
}

func (m *CMessage) MessageType() MessageType         { return CMessageType }
func (m *CMessage) MarshalMethod() MarshalMethodType { return BinaryMarshalMethod }

func (m *CMessage) Length() int { return 16 + m.Pm.Length() }

func (m *CMessage) Reset() {
	m.Offset = make([]byte, 8)
	m.ProductTime = make([]byte, 8)
	m.Pm.Reset()
}

func (m *CMessage) ParseFrom(reader io.Reader) error {
	// 		TopicLength byte
	//		Topic       []byte
	//		KeyLength   byte
	//		Key         []byte
	//		ValueLength uint16
	//		Value       []byte
	//		Offset      uint64
	//		ProductTime int64 // time.Time.Unix()

	err := m.Pm.ParseFrom(reader)
	if err != nil {
		return err
	}

	_, err = reader.Read(m.Offset)
	if err != nil {
		return fmt.Errorf("offset did not read: %v", err)
	}

	_, err = reader.Read(m.ProductTime)
	if err != nil {
		return fmt.Errorf("ProductTime did not read: %v", err)
	}

	return nil
}

// MessageResponse 消息响应， P和C通用
type MessageResponse struct {
	noCopy      NoCopy
	Result      bool      `json:"result"`
	Offset      uint64    `json:"offset,omitempty"`
	ReceiveTime time.Time `json:"receive_time,omitempty"`
}

// MessageType 依据偏移量字段判断消息类型
func (m *MessageResponse) MessageType() MessageType {
	if m.Offset == 0 {
		return RegisterMessageRespType
	}
	return MessageRespType
}

func (m *MessageResponse) MarshalMethod() MarshalMethodType { return JsonMarshalMethod }

func (m *MessageResponse) Length() int { return 0 }

func (m *MessageResponse) Reset() {
	m.Result = false
	m.Offset = 0
}

func (m *MessageResponse) ParseFrom(_ io.Reader) error {
	return ErrParseFromNotImplemented
}

// RegisterMessage 消息注册,适用于生产者和消费者
type RegisterMessage struct {
	Topics []string `json:"topics"` // 对于生产者,无意义
	Ack    AckType  `json:"ack"`
	Type   LinkType `json:"type"`
}

func (m *RegisterMessage) MessageType() MessageType         { return RegisterMessageType }
func (m *RegisterMessage) MarshalMethod() MarshalMethodType { return JsonMarshalMethod }

func (m *RegisterMessage) Length() int { return 0 }
func (m *RegisterMessage) Reset()      {}

func (m *RegisterMessage) ParseFrom(reader io.Reader) error {
	return ErrParseFromNotImplemented
}

type ValidMessage struct{}

func (m ValidMessage) MessageType() MessageType         { return ValidMessageType }
func (m ValidMessage) MarshalMethod() MarshalMethodType { return BinaryMarshalMethod }

func (m ValidMessage) Length() int { return 0 }
func (m ValidMessage) Reset()      {}

func (m ValidMessage) ParseFrom(_ io.Reader) error {
	return ErrMethodNotImplemented
}
