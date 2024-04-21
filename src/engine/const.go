package engine

import "errors"

var (
	ErrConsumerNotRegister = errors.New("consumer not register")
	ErrProducerNotRegister = errors.New("producer not register")
	ErrPMNotFound          = errors.New("producer-message not found in frame")
	// ErrNoNeedToReply 不再回复响应给客户端
	ErrNoNeedToReply = errors.New("no need to reply to the client")
)

// ForwardingAddrsMaxNum topic 最大转发目标数量
const ForwardingAddrsMaxNum = 1 << 4

// const ForwardingAddrsMaxNum = 1 << 8

var (
	ErrSrcNotExist    = errors.New("source topic not exist")
	ErrDstNotExist    = errors.New("destination topic not exist")
	ErrExchangeSame   = errors.New("exchange is same") // 目标转发器和自己一样
	ErrExchangeIsFull = errors.New("exchange is full") // 超出最大数量限制
	ErrExchangeExist  = errors.New("exchange exist")   // 转发目标已存在
)
