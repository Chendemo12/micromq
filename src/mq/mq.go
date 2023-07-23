package mq

import (
	"context"
	"github.com/Chendemo12/synshare-mq/src/engine"
)

type MQ struct {
	conf     *Config
	ctx      context.Context
	cancel   context.CancelFunc
	engine   *engine.Engine
	transfer engine.Transfer
}

// Run 阻塞启动
func (m *MQ) Run()  { m.engine.Run() }
func (m *MQ) Stop() { m.transfer.Stop() }

func New(cs ...Config) *MQ {
	c := &Config{}
	if len(cs) > 0 {
		c.Host = cs[0].Host
		c.Port = cs[0].Port
		c.DashboardHost = cs[0].DashboardHost
		c.DashboardPort = cs[0].DashboardPort
		c.MaxOpenConn = cs[0].MaxOpenConn
		c.BufferSize = cs[0].BufferSize
		c.Logger = cs[0].Logger
		c.Crypto = cs[0].Crypto
		c.EventHandler = cs[0].EventHandler
	} else {
		c.Host = defaultConf.Host
		c.Port = defaultConf.Port
		c.DashboardHost = defaultConf.DashboardHost
		c.DashboardPort = defaultConf.DashboardPort
		c.MaxOpenConn = defaultConf.MaxOpenConn
		c.BufferSize = defaultConf.BufferSize
		c.Logger = defaultConf.Logger
		c.Crypto = defaultConf.Crypto
		c.EventHandler = defaultConf.EventHandler
	}

	mq := &MQ{conf: c}
	mq.ctx, mq.cancel = context.WithCancel(context.Background())

	mq.engine = engine.New(engine.Config{
		Host:         c.Host,
		Port:         c.Port,
		MaxOpenConn:  c.MaxOpenConn,
		BufferSize:   c.BufferSize,
		Logger:       c.Logger,
		Crypto:       c.Crypto,
		EventHandler: c.EventHandler,
	})
	mq.transfer = &engine.TCPTransfer{}
	mq.engine.ReplaceTransfer(mq.transfer)

	return mq
}
