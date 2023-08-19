package proto

import "errors"

const FrameMinLength int = 7
const TotalNumberOfMessages int = 256

const (
	FrameHead = 0x3C // 0x3C (可见字符: <)
	FrameTail = 0x0D // 0x0D (回车符)
)

type MarshalMethodType string

const (
	JsonMarshalMethod   MarshalMethodType = "JSON"
	BinaryMarshalMethod MarshalMethodType = "BINARY"
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

var (
	ErrMethodNotImplemented   = errors.New("method not implemented")
	ErrMessageNotFull         = errors.New("message is not full")
	ErrMessageSplitNotAllowed = errors.New("split is not allowed")
)
