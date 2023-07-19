package test

import (
	"github.com/Chendemo12/synshare-mq/src/proto"
	"testing"
)

func TestRegisterMessage(t *testing.T) {
	msg := proto.NewCRegisterMessage("DDNS_REPORT", "DDNS_CHANGED")

	bytes, err := msg.Build()
	if err != nil {
		t.Errorf("RegisterMessage build failed: %v", err)
	} else {
		t.Logf("RegisterMessage built: %s", string(bytes))
	}
}
