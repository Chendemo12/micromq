package mq

import (
	"github.com/Chendemo12/fastapi-tool/logger"
	"github.com/Chendemo12/micromq/src/engine"
	"github.com/Chendemo12/micromq/src/proto"
)

type Config struct {
	AppName     string         `json:"app_name"`
	Version     string         `json:"version"`
	Debug       bool           `json:"debug"`        // 调试模式开关
	HttpHost    string         `json:"http_host"`    // http api 接口服务
	HttpPort    string         `json:"http_port"`    //
	EdgeEnabled bool           `json:"edge_enabled"` // 是否开启基于Http的消息publisher功能
	Broker      *engine.Config `json:"broker"`
	crypto      proto.Crypto
	cryptoPlan  []string
}

var defaultConf = Config{
	AppName:  "micromq",
	Version:  "1.0.0",
	HttpHost: "0.0.0.0",
	HttpPort: "7280",
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
