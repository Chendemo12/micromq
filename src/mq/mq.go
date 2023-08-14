package mq

import (
	"context"
	"github.com/Chendemo12/fastapi"
	"github.com/Chendemo12/fastapi-tool/logger"
	"github.com/Chendemo12/functools/python"
	"github.com/Chendemo12/micromq/src/engine"
	"github.com/Chendemo12/micromq/src/transfer"
	"os"
)

type MQ struct {
	conf     *Config
	ctx      context.Context
	cancel   context.CancelFunc
	broker   *engine.Engine
	transfer transfer.Transfer
	faster   *fastapi.FastApi
	logger   logger.Iface
}

func (m *MQ) Config() any { return m.conf }

func (m *MQ) Logger() logger.Iface { return m.logger }

func (m *MQ) Ctx() context.Context { return m.ctx }

func (m *MQ) initBroker() *MQ {
	// broker 各种事件触发器
	m.conf.Broker.EventHandler = &CoreEventHandler{}
	m.conf.Broker.Logger = m.Logger()
	m.conf.Broker.Ctx = m.ctx

	m.transfer = &transfer.TCPTransfer{}
	m.broker = engine.New(*m.conf.Broker)
	m.broker.ReplaceTransfer(m.transfer)
	m.broker.SetEventHandler(m.conf.Broker.EventHandler)
	return m
}

func (m *MQ) SetLogger(logger logger.Iface) *MQ {
	m.logger = logger
	return m
}

// Serve 阻塞启动
func (m *MQ) Serve() {
	if m.logger == nil {
		m.logger = logger.NewDefaultLogger()
	}

	// 初始化服务
	m.initBroker()

	go func() {
		err := m.broker.Serve()
		if err != nil {
			m.Logger().Error("broker starts failed: ", err)
			os.Exit(1)
		}
	}()

	m.initHttp()
	m.faster.Run(m.conf.HttpHost, m.conf.HttpPort)
}

func (m *MQ) Stop() {
	m.cancel()
	m.broker.Stop()
	m.faster.Shutdown()
}

func New(cs ...Config) *MQ {
	conf := &Config{
		AppName:     defaultConf.AppName,
		Version:     "v1.0.0",
		Debug:       false,
		HttpHost:    defaultConf.HttpHost,
		HttpPort:    defaultConf.HttpPort,
		EdgeEnabled: true,
		Broker:      defaultConf.Broker,
	}

	if len(cs) > 0 {
		conf.AppName = python.GetS(cs[0].AppName, conf.AppName)
		conf.Version = python.GetS(cs[0].Version, conf.Version)
		conf.HttpHost = python.GetS(cs[0].HttpHost, conf.HttpHost)
		conf.HttpPort = python.GetS(cs[0].HttpPort, conf.HttpPort)
		conf.Debug = cs[0].Debug
		conf.Broker.Host = cs[0].Broker.Host
		conf.Broker.Port = cs[0].Broker.Port
		conf.Broker.MaxOpenConn = cs[0].Broker.MaxOpenConn
		conf.Broker.BufferSize = cs[0].Broker.BufferSize
		conf.Broker.HeartbeatTimeout = cs[0].Broker.HeartbeatTimeout
		conf.Broker.Token = cs[0].Broker.Token

		if cs[0].EdgeEnabled {
			conf.EdgeEnabled = true
		}

		if cs[0].Broker.Crypto != nil {
			conf.Broker.Crypto = cs[0].Broker.Crypto
		}
	}

	mq := &MQ{conf: conf}
	mq.ctx, mq.cancel = context.WithCancel(context.Background())

	return mq
}
