package transfer

import (
	"github.com/Chendemo12/functools/logger"
	"github.com/Chendemo12/functools/tcp"
	"github.com/Chendemo12/synshare-mq/src/engine"
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
	return nil
}

func (t *Transfer) OnClosed(r *tcp.Remote) error {
	return nil
}

func (t *Transfer) Handler(r *tcp.Remote) error {
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
	err := ts.Serve()
	if err != nil {
		t.Logger.Error(err.Error())
		return
	}
}
