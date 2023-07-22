package engine

import "github.com/Chendemo12/synshare-mq/src/proto"

type Hook struct {
	Type       proto.Message
	Message    proto.Message
	Text       string
	Fun        func(frame *proto.TransferFrame)
	UserDefine bool
}

var routers = [256]*Hook{}
