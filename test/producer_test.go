package test

import (
	"context"
	"github.com/Chendemo12/functools/logger"
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
	token := "123456788"
	producer, err := sdk.NewAsyncProducer(sdk.Config{
		Host:   "127.0.0.1",
		Port:   "7270",
		Token:  token,
		Ack:    sdk.AllConfirm,
		PCtx:   ctx,
		Logger: logger.NewDefaultLogger(),
	})
	// 设置消息加密
	producer.SetCryptoPlan("TOKEN")

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
				err = producer.Send(func(r *sdk.ProducerMessage) error {
					r.Topic = "DNS_REPORT"
					r.Key = "test.test.com"
					err2 := r.BindFromJSON(&DnsForm{
						Domain: "test.test.com",
						IP:     "10.64.73.28",
					})

					return err2
				})
				if err != nil {
					producer.Logger().Warn("msg sent failed:  ", err)
				} else {
					producer.Logger().Info("msg sent")
				}
			}
		}
	}()

	<-ctx.Done()
	cancel()
	producer.Stop()
}
