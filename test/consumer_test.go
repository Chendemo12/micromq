package test

import (
	"context"
	"fmt"
	"github.com/Chendemo12/micromq/sdk"
	"testing"
	"time"
)

type DnsConsumer struct {
	sdk.CHandler
	Host   string `json:"host"`
	Port   string `json:"port"`
	Token  string `json:"token"`
	topics []string
	ctx    context.Context
	t      *testing.T
	c      *sdk.Consumer
}

func (c *DnsConsumer) Topics() []string { return c.topics }

func (c *DnsConsumer) Handler(record *sdk.ConsumerMessage) {
	dns := &DnsForm{}
	_ = record.ShouldBindJSON(dns)
	fmt.Printf("receive message %s from:%s\n", record, record.Key)
	fmt.Printf("receive dns update: %s -> %s\n", dns.Domain, dns.IP)
}

func (c *DnsConsumer) OnConnected() {
	c.t.Logf("consumer connected.")
}

func (c *DnsConsumer) OnClosed() {
	c.t.Logf("connection closed, retry...")
}

func (c *DnsConsumer) Start() error {
	con, err := sdk.NewAsyncConsumer(sdk.Config{
		Host:   c.Host,
		Port:   c.Port,
		Ack:    sdk.AllConfirm,
		PCtx:   c.ctx,
		Logger: nil,
		Token:  c.Token,
	}, c)

	// 设置消息加密
	con.SetCryptoPlan("TOKEN")

	if err != nil {
		return err
	}

	c.c = con
	return nil
}

func TestSdkConsumer_Start(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)

	consumer := &DnsConsumer{
		Host:   "127.0.0.1",
		Port:   "7270",
		Token:  "123456788",
		topics: []string{"DNS_REPORT", "DNS_UPDATE"},
		ctx:    ctx,
		t:      t,
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
