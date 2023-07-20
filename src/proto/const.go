package proto

import "errors"

type MessageType byte

const FrameMinLength int = 7

const (
	ValidMessageType        MessageType = 0
	RegisterMessageType     MessageType = 1   // 客户端消费者/生产者注册消息类别 c -> s
	RegisterMessageRespType MessageType = 2   // s -> c
	ReRegisterMessageType   MessageType = 3   // s -> c 令客户端重新发起注册流程
	MessageRespType         MessageType = 100 // 生产者消息响应 s -> c
	PMessageType            MessageType = 101 // 生产者消息类别 c -> s
	CMessageType            MessageType = 102 // 消费者消息类别s -> c
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
	ErrMethodNotImplemented    = errors.New("method not implemented")
	ErrParseFromNotImplemented = errors.New("json marshal does support ParseFrom method")
)

var (
	ErrMessageNotFull      = errors.New("message is not full")
	ErrProducerNotRegister = errors.New("producer not register")
)
