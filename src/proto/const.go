package proto

type MessageType byte

const (
	ValidMessageType        MessageType = 0
	RegisterMessageType     MessageType = 1
	RegisterMessageRespType MessageType = 2
	PMessageType            MessageType = 101
	PMessageRespType        MessageType = 102
	CMessageType            MessageType = 103
	CMessageRespType        MessageType = 104
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
