package mq

import (
	"context"
	"github.com/Chendemo12/fastapi"
	"github.com/Chendemo12/fastapi-tool/logger"
	"github.com/Chendemo12/functools/python"
	"github.com/Chendemo12/micromq/src/engine"
	"github.com/Chendemo12/micromq/src/proto"
	"github.com/Chendemo12/micromq/src/transfer"
	"github.com/gofiber/fiber/v2"
	"os"
)

var mq *MQ

type MQ struct {
	conf     *Config
	ctx      context.Context
	cancel   context.CancelFunc
	broker   *engine.Engine
	transfer transfer.Transfer
	faster   *fastapi.FastApi
	logger   logger.Iface
}

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

func (m *MQ) initHttp() *MQ {
	m.faster = newEdge(&fastapi.Config{
		Title:                   m.conf.AppName,
		Version:                 m.conf.Version,
		Description:             m.conf.AppName + " Api Service",
		Logger:                  m.logger,
		Debug:                   m.conf.Debug,
		UserSvc:                 m,
		ShutdownTimeout:         5,
		DisableSwagAutoCreate:   !python.Any(!m.conf.StatisticDisabled, m.conf.Debug),
		EnableDumpPID:           false,
		DisableResponseValidate: false,
		DisableRequestValidate:  false,
		DisableBaseRoutes:       false,
	})

	if python.Any(m.conf.EdgeEnabled, m.conf.Debug) {
		m.faster.IncludeRouter(EdgeRouter())
	}

	if python.Any(!m.conf.StatisticDisabled, m.conf.Debug) {
		m.faster.IncludeRouter(StatRouter())
	}

	return m
}

func (m *MQ) Config() any { return m.conf }

func (m *MQ) Logger() logger.Iface { return m.logger }

// Ctx 获取根context
func (m *MQ) Ctx() context.Context { return m.ctx }

// Stat 获取统计信息类
func (m *MQ) Stat() *engine.Statistic { return m.broker.Stat() }

func (m *MQ) SetLogger(logger logger.Iface) *MQ {
	m.logger = logger
	return m
}

// SetCrypto 设置加解密器
func (m *MQ) SetCrypto(crypto proto.Crypto) *MQ {
	m.conf.crypto = crypto

	return m
}

// SetCryptoPlan 修改加密方案
func (m *MQ) SetCryptoPlan(option string, key ...string) *MQ {
	m.conf.cryptoPlan = append([]string{option}, key...)

	return m
}

// Serve 阻塞启动
func (m *MQ) Serve() {
	if m.logger == nil {
		m.logger = logger.NewDefaultLogger()
	}

	// 初始化服务
	m.initBroker()
	m.broker.SetCrypto(m.conf.crypto)
	if len(m.conf.cryptoPlan) > 0 {
		m.broker.SetCryptoPlan(m.conf.cryptoPlan[0], m.conf.cryptoPlan[1:]...)
	}

	go func() {
		err := m.broker.Serve()
		if err != nil {
			m.Logger().Error("broker starts failed: ", err)
			os.Exit(1)
		}
	}()

	m.initHttp()
	m.faster.Run(m.conf.EdgeHttpHost, m.conf.EdgeHttpPort)
}

func (m *MQ) Stop() {
	m.cancel()
	m.broker.Stop()
	m.faster.Shutdown()
}

func New(cs ...Config) *MQ {
	conf := &Config{
		AppName:      defaultConf.AppName,
		Version:      "v1.0.0",
		Debug:        false,
		EdgeHttpHost: defaultConf.EdgeHttpHost,
		EdgeHttpPort: defaultConf.EdgeHttpPort,
		EdgeEnabled:  true,
		Broker:       defaultConf.Broker,
	}

	if len(cs) > 0 {
		conf.AppName = python.GetS(cs[0].AppName, conf.AppName)
		conf.Version = python.GetS(cs[0].Version, conf.Version)
		conf.EdgeHttpHost = python.GetS(cs[0].EdgeHttpHost, conf.EdgeHttpHost)
		conf.EdgeHttpPort = python.GetS(cs[0].EdgeHttpPort, conf.EdgeHttpPort)
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
	}

	mq = &MQ{conf: conf}
	mq.ctx, mq.cancel = context.WithCancel(context.Background())

	return mq
}

func newEdge(conf *fastapi.Config) *fastapi.FastApi {
	conf.DisableResponseValidate = true
	conf.EnableMultipleProcess = false

	app := fastapi.New(*conf)
	// Enable CORS
	app.Use(func(c *fiber.Ctx) error {
		c.Set("Access-Control-Allow-Origin", "*")
		c.Set("Access-Control-Allow-Headers", "*")
		c.Set("Access-Control-Allow-Credentials", "false")
		c.Set("Access-Control-Allow-Methods", "GET,POST,OPTIONS,DELETE,PATCH")

		if c.Method() == fiber.MethodOptions {
			c.Status(fiber.StatusOK)
			return nil
		}
		return c.Next()
	})

	return app
}
