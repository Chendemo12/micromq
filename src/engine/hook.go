package engine

import (
	"github.com/Chendemo12/functools/tcp"
	"github.com/Chendemo12/micromq/src/proto"
)

type HookHandler func(frame *proto.TransferFrame, r *tcp.Remote) (bool, error)

type Hook struct {
	Type    proto.MessageType
	Handler HookHandler
	IsAsync bool
}

func emptyHookHandler(_ *proto.TransferFrame, _ *tcp.Remote) (bool, error) {
	return false, nil
}
