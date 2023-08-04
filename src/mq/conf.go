package mq

import (
	"github.com/Chendemo12/micromq/src/engine"
	"github.com/Chendemo12/micromq/src/proto"
)

type Config struct {
	AppName       string              `json:"app_name"`
	Host          string              `json:"host"`
	Port          string              `json:"port"`
	Debug         bool                `json:"debug"`
	DashboardHost string              `json:"dashboard_host"`
	DashboardPort string              `json:"dashboard_port"`
	MaxOpenConn   int                 `json:"max_open_conn"` // 允许的最大连接数, 即 生产者+消费者最多有 MaxOpenConn 个
	BufferSize    int                 `json:"buffer_size"`   // 生产者消息历史记录最大数量
	Crypto        proto.Crypto        `json:"-"`             // 加密器
	Token         string              `json:"-"`             // 注册认证密钥
	EventHandler  engine.EventHandler // 事件触发器
}

var defaultConf = Config{
	AppName:       "micromq",
	Host:          "0.0.0.0",
	Port:          "7270",
	DashboardHost: "0.0.0.0",
	DashboardPort: "7280",
	MaxOpenConn:   50,
	BufferSize:    100,
	Crypto:        proto.DefaultCrypto(),
	EventHandler:  nil,
}

func DefaultConf() Config { return defaultConf }
