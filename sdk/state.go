package sdk

import (
	"errors"
	"github.com/Chendemo12/synshare-mq/src/proto"
)

var framePool = proto.NewFramePool()
var mPool = proto.NewHCPMPool()
var emPool = proto.NewCPMPool()

var ErrTopicEmpty = errors.New("topic is empty")
var ErrConsumerHandlerIsNil = errors.New("consumer handler is nil")

const (
	AllConfirm    = proto.AllConfirm
	NoConfirm     = proto.NoConfirm
	LeaderConfirm = proto.LeaderConfirm
)

type ConsumerMessage = proto.ConsumerMessage
type ProducerMessage = proto.ProducerMessage
