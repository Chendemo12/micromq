package test

import (
	"context"
	"github.com/Chendemo12/fastapi-tool/logger"
	"github.com/Chendemo12/synshare-mq/sdk"
	"github.com/Chendemo12/synshare-mq/src/proto"
	"testing"
	"time"
)

type DnsConsumer struct {
	Host   string `json:"host"`
	Port   string `json:"port"`
	topics []string
	ctx    context.Context
	logger logger.Iface
	c      *sdk.Consumer
}

func (c *DnsConsumer) Topics() []string { return c.topics }

func (c *DnsConsumer) Handler(record *proto.ConsumerMessage) {
	dns := &DnsReport{}
	_ = record.ShouldBindJSON(dns)
	c.logger.Debug("receive message from: (%s-%s)", record.Topic, record.Key)
	c.logger.Debug("receive dns update: %s -> %s", dns.Domain, dns.IP)
}

func (c *DnsConsumer) OnConnected() {
	c.logger.Info("consumer connected.")
}

func (c *DnsConsumer) OnClosed() {
	c.logger.Info("connection closed, retry...")
}

func (c *DnsConsumer) Start() error {
	con, err := sdk.NewAsyncConsumer(sdk.Config{
		Host: c.Host,
		Port: c.Port,
		Ack:  sdk.AllConfirm,
	}, c)

	if err != nil {
		return err
	}

	c.c = con
	return nil
}

func TestConsumer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)

	consumer := &DnsConsumer{
		Host:   "127.0.0.1",
		Port:   "7270",
		topics: []string{"DNS_REPORT", "DNS_UPDATE"},
		ctx:    ctx,
		logger: logger.NewDefaultLogger(),
	}

	err := consumer.Start()

	if err != nil {
		cancel()
		t.Errorf("consumer connect failed: %s", err)
	} else {
		t.Logf("consumer started.")

		<-ctx.Done()
		cancel()
		t.Logf("consumer finished.")
	}
}
