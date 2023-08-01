package test

import (
	"context"
	"github.com/Chendemo12/fastapi-tool/logger"
	"github.com/Chendemo12/micromq/sdk"
	"testing"
	"time"
)

// DnsForm DNS数据表单
type DnsForm struct {
	Domain string `json:"domain"`
	IP     string `json:"ip"`
}

type DnsProducer struct {
	Host           string `json:"host"`
	Port           string `json:"port"`
	Topic          string `json:"topic"`
	ctx            context.Context
	logger         logger.Iface
	p              *sdk.Producer
	reportInterval time.Duration
	ticker         *time.Ticker
}

func (p *DnsProducer) Send(fn func(r *sdk.ProducerMessage) error) {
	err := p.p.Send(fn)
	if err != nil {
		p.logger.Warn("message send failed: %v", err)
	}
	p.logger.Info("message sent")
}

func (p *DnsProducer) tick() {
	for {
		if !p.p.IsConnected() {
			time.Sleep(p.reportInterval * 4)
			continue
		}

		select {
		case <-p.ctx.Done():
			return
		case <-p.ticker.C:
			p.Send(func(r *sdk.ProducerMessage) error {
				r.Topic = p.Topic
				r.Key = time.Now().String()
				err2 := r.BindFromJSON(&DnsForm{
					Domain: "pi.ifile.fun",
					IP:     "10.64.73.28",
				})
				p.logger.Info("sending msg ...")
				return err2
			})
		}
	}
}

func (p *DnsProducer) Start() error {
	p.ticker = time.NewTicker(p.reportInterval)
	pd, err := sdk.NewAsyncProducer(sdk.Config{
		Host:   p.Host,
		Port:   p.Port,
		Ack:    sdk.AllConfirm,
		Ctx:    p.ctx,
		Logger: p.logger,
	})
	if err != nil {
		return err
	}

	p.p = pd

	if err != nil {
		return err
	}

	go p.tick()

	p.logger.Info("dns producer started.")
	return nil
}

func (p *DnsProducer) Done() <-chan struct{} { return p.ctx.Done() }

func (p *DnsProducer) Stop() { p.p.Stop() }

func TestSdkProducer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)

	producer := &DnsProducer{
		Host:           "127.0.0.1",
		Port:           "7270",
		Topic:          "DNS_REPORT",
		ctx:            ctx,
		logger:         logger.NewDefaultLogger(),
		reportInterval: 1000 * time.Millisecond,
	}
	err := producer.Start()

	if err != nil {
		cancel()
		t.Errorf("producer connect failed: %s", err)
	} else {
		t.Logf("producer started.")

		<-producer.Done()
		cancel()
		t.Logf("producer finished.")
	}
}
