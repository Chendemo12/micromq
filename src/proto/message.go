// Package proto 若涉及到字节序,则全部为大端序
package proto

import (
	"encoding/binary"
	"fmt"
	"github.com/Chendemo12/fastapi-tool/helper"
	"io"
	"time"
)

type mh struct{}

func (b mh) Build(m Message) ([]byte, error) {
	if m.MarshalMethod() == JsonMarshalMethod {
		return helper.JsonMarshal(m)
	}
	return m.Build()
}

func (b mh) BuildTo(writer io.Writer, m Message) (int, error) {
	bytes, err := m.Build()
	if err != nil {
		return 0, err
	}
	return writer.Write(bytes)
}

var mHelper = &mh{}

// ========================================== 生产者消息数据协议定义 ==========================================

type Message interface {
	HumanMessage                           // 类别和消息解码方法
	Length() int                           // 编码后的消息序列长度
	Reset()                                // 重置消息体
	ParseFrom(reader io.Reader) error      // 从流中解析一个消息
	Build() ([]byte, error)                // 构建消息序列
	BuildTo(writer io.Writer) (int, error) // 直接将待构建的消息序列写入流内
}

// PMessage 生产者消息数据, 不允许复制
type PMessage struct {
	noCopy NoCopy
	Topic  []byte // 字符串转字节
	Key    []byte
	Value  []byte
}

func (m *PMessage) MessageType() MessageType { return PMessageType }

func (m *PMessage) MarshalMethod() MarshalMethodType {
	return BinaryMarshalMethod
}

// Length 获取编码后的消息序列长度
func (m *PMessage) Length() int {
	return len(m.Topic) + len(m.Key) + len(m.Value)
}

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

func (m *PMessage) Build() ([]byte, error) {
	slice := make([]byte, 0, m.Length()) // 分配最大长度
	vl := make([]byte, 2)
	binary.BigEndian.PutUint16(vl, uint16(len(m.Value)))

	//		TopicLength byte
	//		Topic       []byte
	//		KeyLength   byte
	//		Key         []bytes
	//		ValueLength uint16
	//		Value       []byte
	slice = append(slice, byte(len(m.Topic)))
	slice = append(slice, m.Topic...)
	slice = append(slice, byte(len(m.Key)))
	slice = append(slice, m.Key...)
	slice = append(slice, vl...)
	slice = append(slice, m.Value...)

	return slice, nil
}

func (m *PMessage) BuildTo(writer io.Writer) (int, error) {
	bytes, _ := m.Build()
	return writer.Write(bytes)
}

// ========================================== 消费者消息记录协议定义 ==========================================

// CMessage 消费者消息记录, 不允许复制
type CMessage struct {
	Offset      []byte // uint64
	ProductTime []byte // time.Time.Unix() 消息创建的Unix时间戳
	Pm          *PMessage
}

func (m *CMessage) MessageType() MessageType { return CMessageType }

func (m *CMessage) MarshalMethod() MarshalMethodType {
	return BinaryMarshalMethod
}

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

func (m *CMessage) Build() ([]byte, error) {
	slice := make([]byte, 0, m.Length()) // 分配最大长度

	//		TopicLength byte
	//		Topic       []byte
	//		KeyLength   byte
	//		Key         []byte
	//		ValueLength uint16
	//		Value       []byte
	//		Offset      uint64
	//		ProductTime int64 // time.Time.Unix()
	bytes, _ := m.Pm.Build()
	slice = append(slice, bytes...)
	slice = append(slice, m.Offset[:7]...)
	slice = append(slice, m.ProductTime[:7]...)

	return slice, nil
}

func (m *CMessage) BuildTo(writer io.Writer) (int, error) {
	bytes, _ := m.Build()
	return writer.Write(bytes)
}

// ========================================== 消息响应协议定义 ==========================================

// MessageResponse 消息响应， P和C通用
type MessageResponse struct {
	Result      bool      `json:"result"`
	Offset      uint64    `json:"offset,omitempty"`
	ReceiveTime time.Time `json:"receive_time,omitempty"`
	// 定时器间隔，单位ms，仅生产者有效，生产者需要按照此间隔发送帧消息
	TickerInterval time.Duration `json:"ticker_duration"`
}

// MessageType 依据偏移量字段判断消息类型
func (m *MessageResponse) MessageType() MessageType {
	if m.Offset == 0 {
		return RegisterMessageRespType
	}
	return MessageRespType
}

func (m *MessageResponse) MarshalMethod() MarshalMethodType {
	return JsonMarshalMethod
}

func (m *MessageResponse) Length() int { return 0 }

func (m *MessageResponse) Reset() {
	m.Result = false
	m.Offset = 0
}

func (m *MessageResponse) ParseFrom(_ io.Reader) error {
	return ErrMethodNotImplemented
}

func (m *MessageResponse) Build() ([]byte, error) {
	return mHelper.Build(m)
}

func (m *MessageResponse) BuildTo(writer io.Writer) (int, error) {
	return mHelper.BuildTo(writer, m)
}

// ========================================== 消息注册协议定义 ==========================================

// RegisterMessage 消息注册,适用于生产者和消费者
type RegisterMessage struct {
	Topics []string `json:"topics"` // 对于生产者,无意义
	Ack    AckType  `json:"ack"`
	Type   LinkType `json:"type"`
}

func (m *RegisterMessage) MessageType() MessageType { return RegisterMessageType }

func (m *RegisterMessage) MarshalMethod() MarshalMethodType {
	return JsonMarshalMethod
}

func (m *RegisterMessage) Length() int { return 0 }
func (m *RegisterMessage) Reset()      {}

func (m *RegisterMessage) ParseFrom(reader io.Reader) error {
	_bytes := make([]byte, 0, 65535)
	n, err := reader.Read(_bytes)
	if n == 0 {
		return err
	}
	fmt.Println(len(_bytes))
	return helper.JsonUnmarshal(_bytes, m)
}

func (m *RegisterMessage) String() string {
	return fmt.Sprintf("<%s> register with '%s'", m.Type, m.Ack)
}

func (m *RegisterMessage) Build() ([]byte, error) {
	return mHelper.Build(m)
}

func (m *RegisterMessage) BuildTo(writer io.Writer) (int, error) {
	return mHelper.BuildTo(writer, m)
}

// ========================================== 协议定义 ==========================================

type ValidMessage struct{}

func (m ValidMessage) MessageType() MessageType         { return ValidMessageType }
func (m ValidMessage) MarshalMethod() MarshalMethodType { return BinaryMarshalMethod }
func (m ValidMessage) ParseFrom(_ io.Reader) error      { return ErrMethodNotImplemented }
func (m ValidMessage) Length() int                      { return 0 }
func (m ValidMessage) Reset()                           {}

func (m ValidMessage) Build() ([]byte, error) {
	return nil, ErrMethodNotImplemented
}

func (m ValidMessage) BuildTo(_ io.Writer) (int, error) {
	return 0, ErrMethodNotImplemented
}
