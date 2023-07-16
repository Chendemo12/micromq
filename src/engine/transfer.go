package engine

import (
	"errors"
	"fmt"
	"github.com/Chendemo12/fastapi-tool/helper"
	"github.com/Chendemo12/fastapi-tool/logger"
	"github.com/Chendemo12/functools/tcp"
)

var ErrMessageNotFull = errors.New("message is not full")

type Transfer struct {
	logger logger.Iface
	mq     *Engine
	tcps   *tcp.Server
}

func (t *Transfer) init() {
	ts := tcp.NewTcpServer(
		&tcp.TcpsConfig{
			Host:           t.mq.conf.Host,
			Port:           t.mq.conf.Port,
			MessageHandler: t,
			ByteOrder:      "big",
			Logger:         t.logger,
			MaxOpenConn:    t.mq.conf.MaxOpenConn,
		},
	)
	t.tcps = ts
}

func (t *Transfer) SetEngine(en *Engine) *Transfer {
	t.mq = en
	return t
}

func (t *Transfer) OnAccepted(r *tcp.Remote) error {
	t.logger.Info(r.Addr(), "connected.")
	// 连接成功时不关联数据, 仅在注册成功时,关联到 Engine 中
	return nil
}

func (t *Transfer) OnClosed(r *tcp.Remote) error {
	for _, cons := range t.mq.consumers {
		if cons.Addr == r.Addr() {
			t.logger.Info(r.Addr(), "consumer close connection.")
			t.mq.RemoveConsumer(r.Addr())
			t.mq.consumers[r.Index()] = nil
			return nil
		}
	}

	for _, prod := range t.mq.producers {
		if prod.Addr == r.Addr() {
			t.logger.Info(r.Addr(), "producer close connection.")
			t.mq.producers[r.Index()] = nil
		}
	}

	return nil
}

func (t *Transfer) Handler(r *tcp.Remote) error {
	if r.Len() < 2 {
		return ErrMessageNotFull
	}

	// 读取并拷贝字节流
	content := make([]byte, r.Len())
	_, err := r.Read(content)
	if err != nil {
		return fmt.Errorf("server read frame failed: %v", err)
	}

	// TODO: 计算 checksum是否正确

	frame := framePool.Get()
	err = helper.JsonUnmarshal(content, frame)
	if err != nil {
		return fmt.Errorf("server parse frame failed: %v", err)
	}

	go t.mq.Distribute(frame, r)

	return nil
}

func (t *Transfer) Start() error { return t.tcps.Serve() }

func (t *Transfer) Stop() {
	t.tcps.Stop()
	t.logger.Info("server stopped!")
}
