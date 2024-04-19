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

type ErrCode string

const ErrCodeOK ErrCode = "1"

// exchange add 0e00
const (
	ErrCodeSrcNotExist ErrCode = "0e0001"
	ErrCodeDstNotExist ErrCode = "0e0002"
	// ErrCodeExchangeSame 目标转发器和自己一样
	ErrCodeExchangeSame ErrCode = "0e0003"
	// ErrCodeExchangeIsFull 超出最大数量限制
	ErrCodeExchangeIsFull ErrCode = "0e0004"
)

// exchange del 0e01
const ()

var ErrCodeZhDescription = map[ErrCode]string{
	ErrCodeOK:             "成功",
	ErrCodeSrcNotExist:    "源不存在",
	ErrCodeDstNotExist:    "目标不存在",
	ErrCodeExchangeSame:   "目标转发器和自己一样",
	ErrCodeExchangeIsFull: "超出最大数量限制",
}

var ErrCodeEnDescription = map[ErrCode]string{
	ErrCodeOK:             "OK",
	ErrCodeSrcNotExist:    "Source not exist",
	ErrCodeDstNotExist:    "Destination not exist",
	ErrCodeExchangeSame:   "Exchange is same",
	ErrCodeExchangeIsFull: "Exchange is full",
}
