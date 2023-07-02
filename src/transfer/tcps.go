package transfer

import (
	"errors"
	"github.com/Chendemo12/functools/logger"
	"github.com/Chendemo12/functools/tcp"
	"github.com/Chendemo12/synshare-mq/src/engine"
	"os"
	"os/signal"
)

type Transfer struct {
	Port   string
	Logger logger.Iface
	mq     *engine.Engine
}

func (t *Transfer) SetEngine(en *engine.Engine) *Transfer {
	t.mq = en
	return t
}

func (t *Transfer) OnAccepted(r *tcp.Remote) error {
	t.Logger.Info(r.Addr(), "connected.")
	return nil
}

func (t *Transfer) OnClosed(r *tcp.Remote) error {
	t.Logger.Info(r.Addr(), "connected.")
	t.mq.RemoveConsumer(r.Addr())
	return nil
}

// Distribute 读取第一个字节,判断并分发消息
func (t *Transfer) Distribute(content []byte, r *tcp.Remote) {
	var resp []byte
	var err error
	respType := make([]byte, 1)

	switch content[0] {
	case engine.RegisterMessageType: // 注册消费者
		resp, err = t.mq.HandleRegisterMessage(content[1:], r)
		respType[0] = engine.RegisterMessageRespType
	case engine.ProductionMessageType: // 生产消息
		resp, err = t.mq.HandleProductionMessage(content[1:], r.Addr())
		respType[0] = engine.ProductionMessageRespType
	}

	// 回写返回值
	if err != nil || len(resp) == 0 {
		return
	}
	// 写入消息类别
	_, err = r.Write(respType)
	_, err = r.Write(resp)
	if err != nil {
		return
	}
	err = r.Drain()
	if err != nil {
		return
	}
}

func (t *Transfer) Handler(r *tcp.Remote) error {
	// 读取并拷贝字节流
	content := make([]byte, r.Len())
	_, err := r.Read(content)
	if err != nil {
		return err
	}

	if len(content) < 2 {
		return errors.New("message is not full")
	}

	go t.Distribute(content, r)

	return nil
}

func (t *Transfer) Run() {
	ts := tcp.NewAsyncTcpServer(
		&tcp.TcpsConfig{
			Host:           "0.0.0.0",
			Port:           t.Port,
			MessageHandler: t,
			ByteOrder:      "big",
			Logger:         t.Logger,
		},
	)

	// 关闭开关, buffered
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)
	<-quit // 阻塞进程，直到接收到停止信号,准备关闭程序

	ts.Stop()
	t.Logger.Info("server stopped!")
}
