package test

import (
	"github.com/Chendemo12/micromq/src/proto"
	"testing"
)

func TestRegisterMessage(t *testing.T) {
	msg := &proto.RegisterMessage{
		Topics: []string{"DDNS_REPORT", "DDNS_CHANGED"},
		Ack:    proto.AllConfirm,
		Type:   proto.ConsumerLinkType,
	}

	bytes, err := msg.Build()
	if err != nil {
		t.Errorf("RegisterMessage build failed: %v", err)
	} else {
		t.Logf("RegisterMessage built: %s", string(bytes))
	}
}
