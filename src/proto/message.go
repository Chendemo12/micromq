// Package proto 若涉及到字节序,则全部为大端序
package proto

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/Chendemo12/fastapi-tool/helper"
	"io"
)

// ========================================== 生产者消息数据协议定义 ==========================================

type MessageType byte

// EncryptionAllowed 是否允许加密
func (m MessageType) EncryptionAllowed() bool {
	switch m {
	case RegisterMessageRespType:
		return false
	default:
		return true
	}
}

// 如果增加了新的协议代码，都需要在 descriptors 中添加其类型
const (
	NotImplementMessageType MessageType = 0
	RegisterMessageType     MessageType = 1   // 客户端消费者/生产者注册消息类别 c -> s RegisterMessage
	RegisterMessageRespType MessageType = 2   // s -> c MessageResponse
	HeartbeatMessageType    MessageType = 4   // c -> s
	MessageRespType         MessageType = 100 // 生产者消息响应 s -> c MessageResponse
	PMessageType            MessageType = 101 // 生产者消息类别 c -> s PMessage
	CMessageType            MessageType = 102 // 消费者消息类别 s -> c CMessage
)

// HumanMessage 直接返回给调用者的消息定义
type HumanMessage interface {
	MessageType() MessageType         // 消息类别
	MarshalMethod() MarshalMethodType // 消息序列化方法
	String() string
}

type Message interface {
	HumanMessage                           // 类别和消息解码方法
	Length() int                           // 编码后的消息序列长度
	Reset()                                // 重置消息体
	Parse(stream []byte) error             // 从字节序中解析消息
	ParseFrom(reader io.Reader) error      // 从流中解析一个消息
	Build() ([]byte, error)                // 构建消息序列
	BuildTo(writer io.Writer) (int, error) // 直接将待构建的消息序列写入流内
}

// PMessage 生产者消息数据, 不允许复制
//
//	消息结构：
//		|   TopicLen   |      Topic      |   KeyLen   |        key        |   ValueLen   |   Value   |
//		|--------------|-----------------|------------|-------------------|--------------|-----------|
//	len	|      1       | N [1-255] bytes |      1     |  N [1-255] bytes  |       2      |     N     |
//	   	|--------------|-----------------|------------|-------------------|--------------|-----------|
//
// 打包后的总长度不能超过 65526 字节
type PMessage struct {
	noCopy NoCopy
	Topic  []byte // 字符串转字节
	Key    []byte
	Value  []byte
}

func (m *PMessage) String() string {
	// "<message:ConsumerMessage> on [ T::DNS_UPDATE | K::2023-07-22T12:23:48.767 ] with 200 bytes of payload"
	return fmt.Sprintf(
		"<message:%s> on [ T::%s | K::%s ] with %d bytes of payload",
		GetDescriptor(m.MessageType()).Text(), m.Topic, m.Key, len(m.Value),
	)
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

func (m *PMessage) Parse(stream []byte) error {
	return m.ParseFrom(bytes.NewReader(stream))
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
	bc.i, bc.err = reader.Read(bc.oneByte)
	if bc.err != nil {
		return fmt.Errorf("topic did not read: %v", bc.err)
	}

	m.Topic = make([]byte, bc.OneValue())
	bc.i, bc.err = reader.Read(m.Topic)
	if bc.err != nil {
		return fmt.Errorf("topic did not read: %v", bc.err)
	}

	// keyLength , key
	bc.i, bc.err = reader.Read(bc.oneByte)
	if bc.err != nil {
		return fmt.Errorf("key did not read: %v", bc.err)
	}

	m.Key = make([]byte, bc.OneValue())
	bc.i, bc.err = reader.Read(m.Key)
	if bc.err != nil {
		return fmt.Errorf("key did not read: %v", bc.err)
	}

	// valueLength , value
	bc.i, bc.err = reader.Read(bc.twoByte)
	if bc.err != nil {
		return fmt.Errorf("value did not read: %v", bc.err)
	}

	m.Value = make([]byte, bc.TwoValue())
	bc.i, bc.err = reader.Read(m.Value)
	if bc.err != nil {
		return fmt.Errorf("key did not read: %v", bc.err)
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
	_bytes, _ := m.Build()
	return writer.Write(_bytes)
}

// ========================================== 消费者消息记录协议定义 ==========================================

// CMessage 消费者消息记录, 不允许复制
//
//	消息结构：
//		|   TopicLen   |      Topic      |   KeyLen   |        key        |   ValueLen   |   Value   |   Offset   |   ProductTime   |
//		|--------------|-----------------|------------|-------------------|--------------|-----------|------------|-----------------|
//	len	|      1       | N [1-255] bytes |      1     |  N [1-255] bytes  |       2      |     N     |      8     |         8       |
//	   	|--------------|-----------------|------------|-------------------|--------------|-----------|------------|-----------------|
//
// 打包后的总长度不能超过 65526 字节
type CMessage struct {
	Offset      []byte // uint64
	ProductTime []byte // time.Time.Unix() 消息创建的Unix时间戳
	PM          *PMessage
}

func (m *CMessage) String() string {
	// "<message:ConsumerMessage> on [ T::DNS_UPDATE | K::2023-07-22T12:23:48.767 | O::2342 ] with 200 bytes of payload"
	return fmt.Sprintf(
		"<message:%s> on [ T::%s | K::%s | O::%d ] with %d bytes of payload",
		GetDescriptor(m.MessageType()).Text(), m.PM.Topic, m.PM.Key, m.Offset, len(m.PM.Value),
	)
}

func (m *CMessage) MessageType() MessageType { return CMessageType }

func (m *CMessage) MarshalMethod() MarshalMethodType {
	return BinaryMarshalMethod
}

func (m *CMessage) Length() int { return 16 + m.PM.Length() }

func (m *CMessage) Reset() {
	m.Offset = make([]byte, 8)
	m.ProductTime = make([]byte, 8)
	if m.PM != nil {
		m.PM.Reset()
	}
}

func (m *CMessage) Parse(stream []byte) error {
	return m.ParseFrom(bytes.NewReader(stream))
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

	err := m.PM.ParseFrom(reader)
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
	_bytes, _ := m.PM.Build()
	slice = append(slice, _bytes...)
	slice = append(slice, m.Offset...)
	slice = append(slice, m.ProductTime...)

	return slice, nil
}

func (m *CMessage) BuildTo(writer io.Writer) (int, error) {
	_bytes, _ := m.Build()
	return writer.Write(_bytes)
}

// ========================================== 消息注册协议定义 ==========================================

// RegisterMessage 消息注册,适用于生产者和消费者
type RegisterMessage struct {
	Topics []string `json:"topics"` // 对于生产者,无意义
	Ack    AckType  `json:"ack"`
	Type   LinkType `json:"type"`
	Token  string   `json:"token,omitempty"` // 认证密钥的hash值，当此值不为空时强制有效
}

func (m *RegisterMessage) String() string {
	return fmt.Sprintf(
		"<message:%s> %s with %s",
		GetDescriptor(m.MessageType()).Text(), m.Type, m.Ack,
	)
}

func (m *RegisterMessage) MessageType() MessageType { return RegisterMessageType }

func (m *RegisterMessage) MarshalMethod() MarshalMethodType {
	return JsonMarshalMethod
}

func (m *RegisterMessage) Length() int { return 0 }

func (m *RegisterMessage) Reset() {}

func (m *RegisterMessage) Parse(stream []byte) error {
	return JsonMessageParse(stream, m)
}

// ParseFrom 从reader解析消息，此操作不够优化，应考虑使用 Parse 方法
func (m *RegisterMessage) ParseFrom(reader io.Reader) error {
	return JsonMessageParseFrom(reader, m)
}

func (m *RegisterMessage) Build() ([]byte, error) {
	return JsonMessageBuild(m)
}

func (m *RegisterMessage) BuildTo(writer io.Writer) (int, error) {
	return JsonMessageBuildTo(writer, m)
}

// ========================================== 协议定义 End ==========================================

// HeartbeatMessage 心跳
type HeartbeatMessage struct {
	Type      LinkType `json:"type" description:"客户端类型"`
	CreatedAt int64    `json:"created_at" description:"客户端创建时间戳"`
}

func (h *HeartbeatMessage) MessageType() MessageType {
	return HeartbeatMessageType
}

func (h *HeartbeatMessage) MarshalMethod() MarshalMethodType {
	return JsonMarshalMethod
}

func (h *HeartbeatMessage) String() string {
	return fmt.Sprintf(
		"<message:%s> from %s", GetDescriptor(h.MessageType()).Text(), h.Type,
	)
}

func (h *HeartbeatMessage) Length() int { return 0 }

func (h *HeartbeatMessage) Reset() {}

func (h *HeartbeatMessage) Parse(stream []byte) error {
	return JsonMessageParse(stream, h)
}

func (h *HeartbeatMessage) ParseFrom(reader io.Reader) error {
	return JsonMessageParseFrom(reader, h)
}

func (h *HeartbeatMessage) Build() ([]byte, error) {
	return JsonMessageBuild(h)
}

func (h *HeartbeatMessage) BuildTo(writer io.Writer) (int, error) {
	return JsonMessageBuildTo(writer, h)
}

// ========================================== 通用的消息响应协议定义 ==========================================

type MessageResponseStatus string

const (
	AcceptedStatus       MessageResponseStatus = "0" // 已接受，正常状态
	RefusedStatus        MessageResponseStatus = "1"
	TokenIncorrectStatus MessageResponseStatus = "10" // 密钥不正确
	ReRegisterStatus     MessageResponseStatus = "11" // 令客户端重新发起注册流程, 无消息体
)

func GetMessageResponseStatusText(status MessageResponseStatus) string {
	switch status {
	case AcceptedStatus:
		return "Accepted"
	case TokenIncorrectStatus:
		return "TokenIncorrect"
	case ReRegisterStatus:
		return "Let Re-Register"
	}

	return "Refused"
}

// MessageResponse 消息响应， P和C通用
type MessageResponse struct {
	// 仅当 AcceptedStatus 时才认为服务器接受了请求并下方了有效的参数
	Status      MessageResponseStatus `json:"status"`
	Offset      uint64                `json:"offset"`
	ReceiveTime int64                 `json:"receive_time"`
	// 定时器间隔，单位ms，仅生产者有效，生产者需要按照此间隔发送帧消息
	TickerInterval int `json:"ticker_duration" description:"定时器间隔，单位ms"`
	// 消费者需要按照此参数，在此周期内向服务端发送心跳
	// 生产者在此周期内若没有数据产生，也应发送心跳
	Keepalive float64 `json:"keepalive" description:"心跳间隔，单位s"`
}

func (m *MessageResponse) String() string {
	return fmt.Sprintf(
		"<message:%s> with status: %s",
		GetDescriptor(m.MessageType()).Text(), GetMessageResponseStatusText(m.Status),
	)
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
	m.Status = RefusedStatus
	m.Offset = 0
	m.ReceiveTime = 0
	m.TickerInterval = 0
	m.Keepalive = 0
}

func (m *MessageResponse) Parse(stream []byte) error {
	return helper.JsonUnmarshal(stream, m)
}

// ParseFrom 从reader解析消息，此操作不够优化，应考虑使用 Parse 方法
func (m *MessageResponse) ParseFrom(reader io.Reader) error {
	_bytes := make([]byte, 65526)
	n, err := reader.Read(_bytes)
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}

	return helper.JsonUnmarshal(_bytes[:n], m)
}

func (m *MessageResponse) Build() ([]byte, error) {
	return JsonMessageBuild(m)
}

func (m *MessageResponse) BuildTo(writer io.Writer) (int, error) {
	return JsonMessageBuildTo(writer, m)
}

func (m *MessageResponse) Accepted() bool { return m.Status == AcceptedStatus }

// ========================================== 协议定义 End ==========================================

type NotImplementMessage struct{}

func (m NotImplementMessage) String() string {
	return fmt.Sprintf(
		"<message:%s> not implemented", GetDescriptor(m.MessageType()).Text())
}

func (m NotImplementMessage) MessageType() MessageType         { return NotImplementMessageType }
func (m NotImplementMessage) MarshalMethod() MarshalMethodType { return BinaryMarshalMethod }
func (m NotImplementMessage) Length() int                      { return 0 }
func (m NotImplementMessage) Reset()                           {}

func (m NotImplementMessage) Parse(_ []byte) error {
	return ErrMethodNotImplemented
}

func (m NotImplementMessage) ParseFrom(_ io.Reader) error {
	return ErrMethodNotImplemented
}

func (m NotImplementMessage) Build() ([]byte, error) {
	return nil, ErrMethodNotImplemented
}

func (m NotImplementMessage) BuildTo(_ io.Writer) (int, error) {
	return 0, ErrMethodNotImplemented
}
