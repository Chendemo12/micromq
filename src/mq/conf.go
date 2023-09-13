package mq

import (
	"github.com/Chendemo12/fastapi-tool/logger"
	"github.com/Chendemo12/micromq/src/engine"
	"github.com/Chendemo12/micromq/src/proto"
)

type Config struct {
	AppName      string         `json:"app_name"`
	Version      string         `json:"version"`
	EdgeHttpHost string         `json:"edge_http_host"` // http api 接口服务
	EdgeHttpPort string         `json:"edge_http_port"` //
	EdgeEnabled  bool           `json:"edge_enabled"`   // 是否开启基于Http的消息publisher功能
	Broker       *engine.Config `json:"broker"`         //
	Debug        bool           `json:"debug"`          // 调试模式开关
	crypto       proto.Crypto
	cryptoPlan   []string
}

var defaultConf = Config{
	AppName:      "micromq",
	Version:      "1.0.0",
	EdgeHttpHost: "0.0.0.0",
	EdgeHttpPort: "7280",
	Broker: &engine.Config{
		Host:             "0.0.0.0",
		Port:             "7270",
		MaxOpenConn:      50,
		BufferSize:       100,
		HeartbeatTimeout: 60,
		Logger:           logger.NewDefaultLogger(),
		Token:            "",
		EventHandler:     &CoreEventHandler{},
		Ctx:              nil,
	},
}

func DefaultConf() Config { return defaultConf }
