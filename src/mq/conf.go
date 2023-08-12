package mq

import (
	"github.com/Chendemo12/micromq/src/proto"
)

type Config struct {
	AppName                string       `json:"app_name"`
	Version                string       `json:"version"`
	BrokerHost             string       `json:"broker_host"` // mq 服务
	BrokerPort             string       `json:"broker_port"` //
	BrokerToken            string       `json:"-"`           // 注册认证密钥
	BrokerHeartbeatTimeout float64      `json:"broker_heartbeat_timeout"`
	HttpHost               string       `json:"http_host"`     // http api 接口服务
	HttpPort               string       `json:"http_port"`     //
	EdgeEnabled            bool         `json:"edge_enabled"`  // 是否开启基于Http的消息publisher功能
	MaxOpenConn            int          `json:"max_open_conn"` // 允许的最大连接数, 即 生产者+消费者最多有 MaxOpenConn 个
	BufferSize             int          `json:"buffer_size"`   // 生产者消息历史记录最大数量
	Debug                  bool         `json:"debug"`         // 调试模式开关
	Crypto                 proto.Crypto `json:"-"`             // 加密器
}

var defaultConf = Config{
	AppName:                "micromq",
	Version:                "1.0.0",
	BrokerHost:             "0.0.0.0",
	BrokerPort:             "7270",
	HttpHost:               "0.0.0.0",
	HttpPort:               "7280",
	MaxOpenConn:            50,
	BufferSize:             100,
	Crypto:                 proto.DefaultCrypto(),
	BrokerHeartbeatTimeout: 60,
}

func DefaultConf() Config { return defaultConf }
