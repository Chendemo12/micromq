package sdk

import (
	"errors"
	"github.com/Chendemo12/micromq/src/proto"
	"time"
)

var framePool = proto.NewFramePool()
var hmPool = proto.NewHCPMPool()
var emPool = proto.NewCPMPool()

var ErrTopicEmpty = errors.New("topic is empty")
var ErrConsumerHandlerIsNil = errors.New("consumer handler is nil")
var ErrConsumerUnregistered = errors.New("consumer unregistered")
var ErrConsumerUnconnected = errors.New("consumer unconnected")
var ErrProducerUnregistered = errors.New("producer unregistered")
var ErrProducerUnconnected = errors.New("producer unconnected")

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
