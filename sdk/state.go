package sdk

import (
	"errors"
	"github.com/Chendemo12/synshare-mq/src/proto"
)

var framePool = proto.NewFramePool()
var mPool = proto.NewHCPMPool()

var ErrTopicEmpty = errors.New("topic is empty")
var ErrConsumerHandlerIsNil = errors.New("consumer handler is nil")

func FrameToCMessage(frame *proto.TransferFrame, cm *proto.ConsumerMessage) error {
	//if frame.Type != proto.CMessageType {
	//	return errors.New("frame is not a consumer message")
	//}

	//reader := bytes.NewReader(frame.Data)
	//i, err := reader.Read()

	return nil
}
