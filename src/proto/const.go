package proto

const (
	RegisterMessageType       byte = 0
	RegisterMessageRespType   byte = 1
	ProductionMessageType     byte = 100
	ProductionMessageRespType byte = 101
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
