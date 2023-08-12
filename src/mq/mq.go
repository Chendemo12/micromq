package mq

import (
	"context"
	"github.com/Chendemo12/fastapi"
	"github.com/Chendemo12/fastapi-tool/logger"
	"github.com/Chendemo12/micromq/src/engine"
	"github.com/Chendemo12/micromq/src/transfer"
	"os"
)

type MQ struct {
	conf          *Config
	ctx           context.Context
	cancel        context.CancelFunc
	broker        *engine.Engine
	transfer      transfer.Transfer
	faster        *fastapi.FastApi
	logger        logger.Iface
	brokerHandler engine.EventHandler // broker 各种事件触发器
}

func (m *MQ) Config() any { return m.conf }

func (m *MQ) Logger() logger.Iface { return m.logger }

func (m *MQ) Ctx() context.Context { return m.ctx }

func (m *MQ) initBroker() *MQ {
	m.brokerHandler = &CoreEventHandler{}
	m.broker = engine.New(engine.Config{
		Host:        m.conf.BrokerHost,
		Port:        m.conf.BrokerPort,
		MaxOpenConn: m.conf.MaxOpenConn,
		BufferSize:  m.conf.BufferSize,
		Logger:      m.Logger(),
		Crypto:      m.conf.Crypto,
		Token:       m.conf.BrokerToken,
	})
	m.transfer = &transfer.TCPTransfer{}
	m.broker.ReplaceTransfer(m.transfer)
	m.broker.SetEventHandler(m.brokerHandler)
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

func GetString(s, d string) string {
	if s != "" {
		return s
	}

	return d
}

func GetInt(s, d int) int {
	if s != 0 {
		return s
	}

	return d
}

func New(cs ...Config) *MQ {
	conf := &Config{
		AppName:     defaultConf.AppName,
		Version:     "v1.0.0",
		BrokerHost:  defaultConf.BrokerHost,
		BrokerPort:  defaultConf.BrokerPort,
		HttpHost:    defaultConf.HttpHost,
		HttpPort:    defaultConf.HttpPort,
		EdgeEnabled: true,
		MaxOpenConn: defaultConf.MaxOpenConn,
		BufferSize:  defaultConf.BufferSize,
		Debug:       false,
		Crypto:      defaultConf.Crypto,
		BrokerToken: "",
	}

	if len(cs) > 0 {
		conf.AppName = GetString(cs[0].AppName, conf.AppName)
		conf.Version = GetString(cs[0].Version, conf.Version)
		conf.BrokerHost = GetString(cs[0].BrokerHost, conf.BrokerHost)
		conf.BrokerPort = GetString(cs[0].BrokerPort, conf.BrokerPort)
		conf.HttpHost = GetString(cs[0].HttpHost, conf.HttpHost)
		conf.HttpPort = GetString(cs[0].HttpPort, conf.HttpPort)
		conf.MaxOpenConn = GetInt(cs[0].MaxOpenConn, conf.MaxOpenConn)
		conf.BufferSize = GetInt(cs[0].BufferSize, conf.BufferSize)
		conf.BrokerToken = GetString(cs[0].BrokerToken, conf.BrokerToken)
		conf.Debug = cs[0].Debug

		if cs[0].EdgeEnabled {
			conf.EdgeEnabled = true
		}

		if cs[0].Crypto != nil {
			conf.Crypto = cs[0].Crypto
		}
	}

	mq := &MQ{conf: conf, brokerHandler: &CoreEventHandler{}}
	mq.ctx, mq.cancel = context.WithCancel(context.Background())

	return mq
}
