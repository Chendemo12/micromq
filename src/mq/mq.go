package mq

import (
	"context"
	"os"

	"github.com/Chendemo12/fastapi"
	"github.com/Chendemo12/fastapi/middleware/fiberWrapper"
	"github.com/Chendemo12/fastapi/utils"
	"github.com/Chendemo12/functools/python"
	logger "github.com/Chendemo12/functools/zaplog"
	"github.com/Chendemo12/micromq/src/engine"
	"github.com/Chendemo12/micromq/src/proto"
	"github.com/Chendemo12/micromq/src/transfer"
	"github.com/gofiber/fiber/v2"
)

var mq *MQ

type MQ struct {
	conf     *Config
	ctx      context.Context
	cancel   context.CancelFunc
	broker   *engine.Engine
	transfer transfer.Transfer
	faster   *fastapi.FastApi
}

func (m *MQ) initBroker() *MQ {
	// broker 各种事件触发器
	m.conf.Broker.EventHandler = &CoreEventHandler{}
	m.conf.Broker.Ctx = m.ctx

	m.transfer = &transfer.TCPTransfer{}
	m.broker = engine.New(*m.conf.Broker)
	m.broker.ReplaceTransfer(m.transfer)
	m.broker.SetEventHandler(m.conf.Broker.EventHandler)

	return m
}

func (m *MQ) initHttp() *MQ {
	app := fastapi.New(fastapi.Config{
		Title:                 m.conf.AppName,
		Version:               m.conf.Version,
		Description:           m.conf.AppName + " Api Service",
		DisableSwagAutoCreate: !python.Any(!m.conf.StatisticDisabled, m.conf.Debug),
	})
	eng := fiberWrapper.Default(fiber.Config{
		Prefork:       false,               // 多进程模式
		CaseSensitive: true,                // 区分路由大小写
		StrictRouting: true,                // 严格路由
		ServerHeader:  m.conf.AppName,      // 服务器头
		AppName:       m.conf.AppName,      // 设置为 Response.Header.Server 属性
		ColorScheme:   fiber.DefaultColors, // 彩色输出
		JSONEncoder:   utils.JsonMarshal,   // json序列化器
		JSONDecoder:   utils.JsonUnmarshal, // json解码器
		BodyLimit:     100 * 1024 * 1024,   // 设置请求体最大为 100MB
	})
	eng.App().Use(fiberWrapper.DefaultCORS)

	app.SetMux(eng)
	if python.Any(m.conf.EdgeEnabled, m.conf.Debug) {
		app.IncludeRouter(&EdgeRouter{})
	}

	if python.Any(!m.conf.StatisticDisabled, m.conf.Debug) {
		app.IncludeRouter(&StatRouter{})
	}

	m.faster = app

	return m
}

func (m *MQ) Config() any { return m.conf }

// Ctx 获取根context
func (m *MQ) Ctx() context.Context { return m.ctx }

// Stat 获取统计信息类
func (m *MQ) Stat() *engine.Statistic { return m.broker.Stat() }

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
	// 初始化服务
	m.initBroker()
	m.broker.SetCrypto(m.conf.crypto)
	if len(m.conf.cryptoPlan) > 0 {
		m.broker.SetCryptoPlan(m.conf.cryptoPlan[0], m.conf.cryptoPlan[1:]...)
	}

	go func() {
		err := m.broker.Serve()
		if err != nil {
			logger.Error("broker starts failed: ", err)
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
