package engine

import "errors"

var (
	ErrConsumerNotRegister = errors.New("consumer not register")
	ErrProducerNotRegister = errors.New("producer not register")
	ErrPMNotFound          = errors.New("producer-message not found in frame")
	// ErrNoNeedToReply 不再回复响应给客户端
	ErrNoNeedToReply = errors.New("no need to reply to the client")
)
