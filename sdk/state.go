package sdk

import "errors"

var ErrTopicEmpty = errors.New("topic is empty")
var ErrConsumerHandlerIsNil = errors.New("consumer handler is nil")
