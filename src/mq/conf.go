package mq

import (
	"github.com/Chendemo12/micromq/src/engine"
	"github.com/Chendemo12/micromq/src/proto"
)

type Config struct {
	AppName           string         `json:"app_name"`
	Version           string         `json:"version"`
	EdgeHttpHost      string         `json:"edge_http_host"`     // http api 接口服务
	EdgeHttpPort      string         `json:"edge_http_port"`     //
	EdgeEnabled       bool           `json:"edge_enabled"`       // 是否开启基于Http的消息publisher功能
	Debug             bool           `json:"debug"`              // 调试模式开关
	SwaggerDisabled   bool           `json:"swagger_disabled"`   // 禁用调试文档
	StatisticDisabled bool           `json:"statistic_disabled"` // 禁用统计功能
	Broker            *engine.Config `json:"broker"`             //
	crypto            proto.Crypto
	cryptoPlan        []string
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
		Token:            "",
		EventHandler:     &CoreEventHandler{},
		Ctx:              nil,
	},
}

func DefaultConf() Config { return defaultConf }
