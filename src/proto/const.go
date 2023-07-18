package proto

type MessageType byte

const (
	ValidMessageType        MessageType = 0
	RegisterMessageType     MessageType = 1 // c -> s
	RegisterMessageRespType MessageType = 2 // s -> c
	MessageRespType         MessageType = 100
	PMessageType            MessageType = 101 // c -> s
	PMessageRespType        MessageType = 102 // s -> c
	CMessageType            MessageType = 103 // s -> c
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
