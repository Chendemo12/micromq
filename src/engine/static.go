package engine

import "errors"

var (
	ErrConsumerNotRegister = errors.New("consumer not register")
	ErrProducerNotRegister = errors.New("producer not register")
	ErrPMNotFound          = errors.New("ProducerMessage not found in frame")
)
