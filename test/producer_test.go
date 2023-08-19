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

func TestSdkProducer_Start(t *testing.T) {
	ticker := time.NewTicker(1000 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)

	producer, err := sdk.NewAsyncProducer(sdk.Config{
		Host:   "127.0.0.1",
		Port:   "7270",
		Token:  "123456788",
		Ack:    sdk.AllConfirm,
		PCtx:   ctx,
		Logger: logger.NewDefaultLogger(),
	})

	if err != nil {
		cancel()
		t.Errorf("producer connect failed: %s", err)
		return
	}
	go func() {
		for {
			select {
			case <-producer.Done():
				return
			case <-ticker.C:
				_ = producer.Send(func(r *sdk.ProducerMessage) error {
					r.Topic = "DNS_REPORT"
					r.Key = "test.test.com"
					err2 := r.BindFromJSON(&DnsForm{
						Domain: "test.test.com",
						IP:     "10.64.73.28",
					})
					producer.Logger().Info("sending msg ...")
					return err2
				})
			}
		}
	}()

	<-ctx.Done()
	cancel()
	producer.Stop()
}
