package test

import (
	"github.com/Chendemo12/micromq/src/proto"
	"testing"
)

func TestTransferFrame_BuildFrom(t *testing.T) {
	msg := &proto.RegisterMessage{
		Topics: []string{"DDNS_REPORT", "DDNS_CHANGED"},
		Ack:    proto.AllConfirm,
		Type:   proto.ConsumerLinkType,
	}

	frame := &proto.TransferFrame{}
	err := frame.BuildFrom(msg)
	if err != nil {
		t.Errorf("RegisterMessage build failed: %v", err)
	} else {
		t.Logf("RegisterMessage built: %s", string(frame.Build()))
	}
}
