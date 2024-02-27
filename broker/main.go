package main

import (
	"github.com/Chendemo12/fastapi-tool/logger"
	"github.com/Chendemo12/functools/environ"
	"github.com/Chendemo12/micromq/src/mq"
	"github.com/Chendemo12/micromq/src/proto"
)

const VERSION = "v0.3.8"
const NAME = "micromq"

func main() {
	conf := mq.DefaultConf()

	conf.AppName = environ.GetString("APP_NAME", NAME)
	conf.Version = VERSION
	conf.Debug = environ.GetBool("DEBUG", false)

	conf.Broker.Host = "0.0.0.0"
	conf.Broker.Port = environ.GetString("BROKER_CORE_LISTEN_PORT", "7270")
	conf.Broker.BufferSize = environ.GetInt("BROKER_BUFFER_SIZE", 100)
	conf.Broker.MaxOpenConn = environ.GetInt("BROKER_MAX_OPEN_SIZE", 50)
	conf.Broker.HeartbeatTimeout = float64(environ.GetInt("BROKER_HEARTBEAT_TIMEOUT", 60))
	conf.Broker.Token = proto.CalcSHA(environ.GetString("BROKER_TOKEN", ""))
	// 是否开启消息加密
	msgEncrypt := environ.GetBool("BROKER_MESSAGE_ENCRYPT", false)
	// 消息加密方案, 目前仅支持基于 Token 的加密
	msgEncryptPlan := environ.GetString("BROKER_MESSAGE_ENCRYPT_OPTION", "TOKEN")

	conf.EdgeHttpPort = environ.GetString("BROKER_EDGE_LISTEN_PORT", "7271")
	conf.EdgeEnabled = environ.GetBool("EDGE_ENABLED", false)

	handler := mq.New(conf)
	handler.SetLogger(logger.NewDefaultLogger())
	if msgEncrypt { // 设置消息加密
		handler.SetCryptoPlan(msgEncryptPlan)
	}

	handler.Serve()
}
