package proto

import "errors"

const FrameMinLength int = 7
const TotalNumberOfMessages int = 256

type MessageType byte

// 如果增加了新的协议代码，则都需要在 descriptors 中添加其类型
const (
	NotImplementMessageType MessageType = 0
	RegisterMessageType     MessageType = 1   // 客户端消费者/生产者注册消息类别 c -> s RegisterMessage
	RegisterMessageRespType MessageType = 2   // s -> c MessageResponse
	ReRegisterMessageType   MessageType = 3   // s -> c 令客户端重新发起注册流程, 无消息体
	HeartbeatMessageType    MessageType = 4   // c -> s
	MessageRespType         MessageType = 100 // 生产者消息响应 s -> c MessageResponse
	PMessageType            MessageType = 101 // 生产者消息类别 c -> s PMessage
	CMessageType            MessageType = 102 // 消费者消息类别s -> c CMessage
)

type AckType string

const (
	NoConfirm     AckType = "0"
	LeaderConfirm AckType = "1"
	AllConfirm    AckType = "all"
)

type LinkType string

const (
	ConsumerLinkType LinkType = "CONSUMER"
	ProducerLinkType LinkType = "PRODUCER"
)

const (
	FrameHead = 0x3C // 0x3C (可见字符: <)
	FrameTail = 0x0D // 0x0D (回车符)
)

type MarshalMethodType string

const (
	JsonMarshalMethod   MarshalMethodType = "JSON"
	BinaryMarshalMethod MarshalMethodType = "BINARY"
)

var (
	ErrMethodNotImplemented = errors.New("method not implemented")
	ErrMessageNotFull       = errors.New("message is not full")
)
