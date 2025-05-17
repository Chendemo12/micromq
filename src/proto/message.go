// Package proto 若涉及到字节序,则全部为大端序
package proto

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/Chendemo12/functools/helper"
)

// ========================================== 生产者消息数据协议定义 ==========================================

type MessageType byte

// 如果增加了新的协议代码，都需要在 descriptors 中添加其类型
const (
	NotImplementMessageType MessageType = 0
	RegisterMessageType     MessageType = 1   // 客户端消费者/生产者注册消息类别 c -> s RegisterMessage
	RegisterMessageRespType MessageType = 2   // s -> c MessageResponse
	HeartbeatMessageType    MessageType = 4   // c -> s HeartbeatMessage
	MessageRespType         MessageType = 100 // 生产者消息响应 s -> c MessageResponse
	PMessageType            MessageType = 101 // 生产者消息类别 c -> s PMessage
	CMessageType            MessageType = 102 // 消费者消息类别 s -> c CMessage
)

// EncryptionAllowed 是否允许加密消息体
func (m MessageType) EncryptionAllowed() bool {
	switch m {
	case RegisterMessageRespType, MessageRespType:
		// 为保证客户端在token错误情况下也可以识别注册响应，此消息应禁止加密
		return false
	default:
		return true
	}
}

// CombinationAllowed 是否允许组合多个消息为一个传输帧 TransferFrame
func (m MessageType) CombinationAllowed() bool {
	switch m {
	case PMessageType, CMessageType:
		return true
	case RegisterMessageType, HeartbeatMessageType:
		// 具有实时性和身份验证，不允许组合
		return false
	default:
		return false
	}
}

// Message 消息定义
// 消息编解码过程中不支持加解密操作, 消息的加解密是对编码后的字节序列进行的操作,与消息定义无关
type Message interface {
	MessageType() MessageType         // 消息类别
	MarshalMethod() MarshalMethodType // 消息序列化方法
	String() string                   // 类别和消息解码方法
	Reset()                           // 重置消息体
	parse(stream []byte) error        // 从字节序中解析消息
	parseFrom(reader io.Reader) error // 从流中解析一个消息
	build() ([]byte, error)           // 构建消息序列, 由于很难预先确定编码后的消息长度,因此暂不实现WriteTo方法
}

// PMessage 生产者消息数据, 不允许复制
//
//	消息结构：
//		|   TopicLen   |      Topic      |   KeyLen   |        key        |   ValueLen   |   Value   |
//		|--------------|-----------------|------------|-------------------|--------------|-----------|
//	len	|      1       | N [1-255] bytes |      1     |  N [1-255] bytes  |       2      |     N     |
//	   	|--------------|-----------------|------------|-------------------|--------------|-----------|
type PMessage struct {
	noCopy NoCopy
	Topic  []byte // 字符串转字节
	Key    []byte
	Value  []byte
}

func (m *PMessage) String() string {
	// "<Message:ConsumerMessage> on [ T::DNS_UPDATE | K::2023-07-22T12:23:48.767 ] with 200 bytes of payload"
	return fmt.Sprintf(
		"<Message:%s> on [ T::%s | K::%s ] with %d bytes of payload",
		descriptors[m.MessageType()].text, m.Topic, m.Key, len(m.Value),
	)
}

func (m *PMessage) MessageType() MessageType { return PMessageType }

func (m *PMessage) MarshalMethod() MarshalMethodType {
	return BinaryMarshalMethod
}

// Length 获取编码后的消息序列长度
func (m *PMessage) length() int {
	return len(m.Topic) + len(m.Key) + len(m.Value) + 4
}

func (m *PMessage) Reset() {
	m.Topic = nil
	m.Key = nil
	m.Value = nil
}

func (m *PMessage) parse(stream []byte) error {
	return m.parseFrom(bytes.NewReader(stream))
}

func (m *PMessage) parseFrom(reader io.Reader) error {
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

func (m *PMessage) build() ([]byte, error) {
	slice := make([]byte, 0, m.length()) // 分配最大长度
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

// ========================================== 消费者消息记录协议定义 ==========================================

// CMessage 消费者消息记录, 不允许复制
//
//	消息结构：
//		|   TopicLen   |      Topic      |   KeyLen   |        key        |   ValueLen   |   Value   |   Offset   |   ProductTime   |
//		|--------------|-----------------|------------|-------------------|--------------|-----------|------------|-----------------|
//	len	|      1       | N [1-255] bytes |      1     |  N [1-255] bytes  |       2      |     N     |      8     |         8       |
//	   	|--------------|-----------------|------------|-------------------|--------------|-----------|------------|-----------------|
type CMessage struct {
	Offset      []byte // uint64
	ProductTime []byte // time.Time.Unix() 消息创建的Unix时间戳
	PM          *PMessage
}

func (m *CMessage) String() string {
	// "<Message:ConsumerMessage> on [ T::DNS_UPDATE | K::2023-07-22T12:23:48.767 | O::2342 ] with 200 bytes of payload"
	return fmt.Sprintf(
		"<Message:%s> on [ T::%s | K::%s | O::%d ] with %d bytes of payload",
		descriptors[m.MessageType()].text, m.PM.Topic, m.PM.Key, m.Offset, len(m.PM.Value),
	)
}

func (m *CMessage) MessageType() MessageType { return CMessageType }

func (m *CMessage) MarshalMethod() MarshalMethodType {
	return BinaryMarshalMethod
}

func (m *CMessage) Reset() {
	m.Offset = make([]byte, 8)
	m.ProductTime = make([]byte, 8)
	if m.PM != nil {
		m.PM.Reset()
	}
}

func (m *CMessage) parse(stream []byte) error {
	return m.parseFrom(bytes.NewReader(stream))
}

func (m *CMessage) parseFrom(reader io.Reader) error {
	// 		TopicLength byte
	//		Topic       []byte
	//		KeyLength   byte
	//		Key         []byte
	//		ValueLength uint16
	//		Value       []byte
	//		Offset      uint64
	//		ProductTime int64 // time.Time.Unix()

	err := m.PM.parseFrom(reader)
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

func (m *CMessage) build() ([]byte, error) {
	slice := make([]byte, 0) // 分配最大长度

	//		TopicLength byte
	//		Topic       []byte
	//		KeyLength   byte
	//		Key         []byte
	//		ValueLength uint16
	//		Value       []byte
	//		Offset      uint64
	//		ProductTime int64 // time.Time.Unix()
	_bytes, _ := m.PM.build()
	slice = append(slice, _bytes...)
	slice = append(slice, m.Offset...)
	slice = append(slice, m.ProductTime...)

	return slice, nil
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
		"<Message:%s> %s with %s",
		descriptors[m.MessageType()].text, m.Type, m.Ack,
	)
}

func (m *RegisterMessage) MessageType() MessageType { return RegisterMessageType }

func (m *RegisterMessage) MarshalMethod() MarshalMethodType {
	return JsonMarshalMethod
}

func (m *RegisterMessage) Reset() {}

func (m *RegisterMessage) parse(stream []byte) error {
	return helper.JsonUnmarshal(stream, m)
}

// ParseFrom 从reader解析消息，此操作不够优化，应考虑使用 Parse 方法
func (m *RegisterMessage) parseFrom(reader io.Reader) error {
	return JsonMessageParseFrom(reader, m)
}

func (m *RegisterMessage) build() ([]byte, error) {
	return helper.JsonMarshal(m)
}

// ========================================== 协议定义 End ==========================================

// HeartbeatMessage 心跳
type HeartbeatMessage struct {
	Type      LinkType `json:"type" description:"客户端类型"`
	CreatedAt int64    `json:"created_at" description:"客户端创建时间戳"`
}

func (m *HeartbeatMessage) MessageType() MessageType {
	return HeartbeatMessageType
}

func (m *HeartbeatMessage) MarshalMethod() MarshalMethodType {
	return JsonMarshalMethod
}

func (m *HeartbeatMessage) String() string {
	return fmt.Sprintf(
		"<Message:%s> from %s", descriptors[m.MessageType()].text, m.Type,
	)
}

func (m *HeartbeatMessage) Reset() {}

func (m *HeartbeatMessage) parse(stream []byte) error {
	return helper.JsonUnmarshal(stream, m)
}

func (m *HeartbeatMessage) parseFrom(reader io.Reader) error {
	return JsonMessageParseFrom(reader, m)
}

func (m *HeartbeatMessage) build() ([]byte, error) {
	return helper.JsonMarshal(m)
}

// ========================================== 通用的消息响应协议定义 ==========================================

// MessageResponse 消息响应， P和C通用
type MessageResponse struct {
	Type MessageType `json:"-"`
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
		"<Message:%s> with status: %s",
		descriptors[m.MessageType()].text, GetMessageResponseStatusText(m.Status),
	)
}

// MessageType 依据偏移量字段判断消息类型
func (m *MessageResponse) MessageType() MessageType {
	if m.Type == 0 {
		return MessageRespType
	}
	return m.Type
}

func (m *MessageResponse) MarshalMethod() MarshalMethodType {
	return JsonMarshalMethod
}

func (m *MessageResponse) Reset() {
	m.Status = RefusedStatus
	m.Offset = 0
	m.ReceiveTime = 0
	m.TickerInterval = 0
	m.Keepalive = 0
}

func (m *MessageResponse) parse(stream []byte) error {
	return helper.JsonUnmarshal(stream, m)
}

// ParseFrom 从reader解析消息，此操作不够优化，应考虑使用 Parse 方法
func (m *MessageResponse) parseFrom(reader io.Reader) error {
	_bytes := make([]byte, 65526)
	n, err := reader.Read(_bytes)
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}

	return helper.JsonUnmarshal(_bytes[:n], m)
}

func (m *MessageResponse) build() ([]byte, error) {
	return helper.JsonMarshal(m)
}

func (m *MessageResponse) Accepted() bool { return m.Status == AcceptedStatus }

// ========================================== 协议定义 End ==========================================

type NotImplementMessage struct{}

func (m NotImplementMessage) String() string {
	return fmt.Sprintf(
		"<Message:%s> not implemented", descriptors[m.MessageType()].text)
}

func (m NotImplementMessage) MessageType() MessageType         { return NotImplementMessageType }
func (m NotImplementMessage) MarshalMethod() MarshalMethodType { return BinaryMarshalMethod }
func (m NotImplementMessage) Reset()                           {}

func (m NotImplementMessage) parse(_ []byte) error {
	return ErrMethodNotImplemented
}

func (m NotImplementMessage) parseFrom(_ io.Reader) error {
	return ErrMethodNotImplemented
}

func (m NotImplementMessage) build() ([]byte, error) {
	return nil, ErrMethodNotImplemented
}
