package main

import (
	"github.com/Chendemo12/functools/environ"
	"github.com/Chendemo12/functools/zaplog"
	"github.com/Chendemo12/micromq/src/mq"
	"github.com/Chendemo12/micromq/src/proto"
)

func main() {
	conf := mq.DefaultConf()

	conf.AppName = environ.GetString("APP_NAME", "micromq")
	conf.Version = VERSION
	conf.Debug = environ.GetBool("DEBUG", false)

	conf.Broker.Host = environ.GetString("BROKER_LISTEN_HOST", "0.0.0.0")
	conf.Broker.Port = environ.GetString("BROKER_LISTEN_PORT", "7270")
	conf.Broker.BufferSize = environ.GetInt("BROKER_BUFFER_SIZE", 100)
	conf.Broker.MaxOpenConn = environ.GetInt("BROKER_MAX_OPEN_SIZE", 50)
	conf.Broker.HeartbeatTimeout = float64(environ.GetInt("BROKER_HEARTBEAT_TIMEOUT", 60))
	conf.Broker.Token = proto.CalcSHA(environ.GetString("BROKER_TOKEN", ""))

	conf.HttpPort = environ.GetString("HTTP_LISTEN_PORT", "7280")

	zapConf := &zaplog.Config{
		Filename:   conf.AppName,
		Level:      zaplog.WARNING,
		Rotation:   10,
		Retention:  5,
		MaxBackups: 10,
		Compress:   false,
	}

	if conf.Debug {
		zapConf.Level = zaplog.DEBUG
	}

	handler := mq.New(conf)
	handler.SetLogger(zaplog.NewLogger(zapConf).Sugar())

	handler.Serve()
}
