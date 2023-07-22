package sdk

import (
	"errors"
	"github.com/Chendemo12/synshare-mq/src/proto"
	"time"
)

var framePool = proto.NewFramePool()
var mPool = proto.NewHCPMPool()
var emPool = proto.NewCPMPool()

var ErrTopicEmpty = errors.New("topic is empty")
var ErrConsumerHandlerIsNil = errors.New("consumer handler is nil")

const (
	DefaultProducerSendInterval = 500 * time.Millisecond
)

//goland:noinspection GoUnusedGlobalVariable
const (
	AllConfirm    = proto.AllConfirm
	NoConfirm     = proto.NoConfirm
	LeaderConfirm = proto.LeaderConfirm
)

type ConsumerMessage = proto.ConsumerMessage
type ProducerMessage = proto.ProducerMessage

type Queue = proto.Queue

//goland:noinspection GoUnusedGlobalVariable
var NewQueue = proto.NewQueue
