package sdk

import (
	"errors"
	"github.com/Chendemo12/synshare-mq/src/proto"
	"time"
)

var ErrTopicEmpty = errors.New("topic is empty")
var ErrConsumerHandlerIsNil = errors.New("consumer handler is nil")

// Message 消费者或生产者消息记录, 不允许复制
type Message struct {
	noCopy      proto.NoCopy
	Topic       string
	Key         []byte
	Value       []byte
	Offset      uint64
	ProductTime time.Time
}
