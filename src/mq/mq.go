package mq

import (
	"context"
	"github.com/Chendemo12/fastapi-tool/logger"
	"github.com/Chendemo12/micromq/src/engine"
	"os"
	"os/signal"
)

type MQ struct {
	conf     *Config
	ctx      context.Context
	cancel   context.CancelFunc
	engine   *engine.Engine
	transfer engine.Transfer
	logger   logger.Iface
}

func (m *MQ) SetLogger(logger logger.Iface) *MQ {
	m.logger = logger
	return m
}

func (m *MQ) Logger() logger.Iface { return m.logger }

func (m *MQ) Ctx() context.Context { return m.ctx }

// Serve 阻塞启动
func (m *MQ) Serve() {
	if m.logger == nil {
		m.logger = logger.NewDefaultLogger()
	}

	m.engine = engine.New(engine.Config{
		Host:         m.conf.Host,
		Port:         m.conf.Port,
		MaxOpenConn:  m.conf.MaxOpenConn,
		BufferSize:   m.conf.BufferSize,
		Logger:       m.Logger(),
		Crypto:       m.conf.Crypto,
		Token:        m.conf.Token,
		EventHandler: m.conf.EventHandler,
	})
	m.transfer = &engine.TCPTransfer{}
	m.engine.ReplaceTransfer(m.transfer)

	quit := make(chan os.Signal, 1) // 关闭开关, buffered
	signal.Notify(quit, os.Interrupt)

	go func() {
		err := m.engine.Serve()
		if err != nil {
			m.Logger().Error("server starts failed: ", err)
			os.Exit(1)
		}
	}()

	<-quit // 阻塞进程，直到接收到停止信号,准备关闭程序
	m.Stop()
}

func (m *MQ) Stop() {
	m.cancel()
	m.engine.Stop()
}

func New(cs ...Config) *MQ {
	c := &Config{}
	if len(cs) > 0 {
		c.Host = cs[0].Host
		c.Port = cs[0].Port
		c.DashboardHost = cs[0].DashboardHost
		c.DashboardPort = cs[0].DashboardPort
		c.MaxOpenConn = cs[0].MaxOpenConn
		c.BufferSize = cs[0].BufferSize
		c.Crypto = cs[0].Crypto
		c.EventHandler = cs[0].EventHandler
	} else {
		c.Host = defaultConf.Host
		c.Port = defaultConf.Port
		c.DashboardHost = defaultConf.DashboardHost
		c.DashboardPort = defaultConf.DashboardPort
		c.MaxOpenConn = defaultConf.MaxOpenConn
		c.BufferSize = defaultConf.BufferSize
		c.Crypto = defaultConf.Crypto
		c.EventHandler = defaultConf.EventHandler
	}

	mq := &MQ{conf: c}
	mq.ctx, mq.cancel = context.WithCancel(context.Background())

	return mq
}
