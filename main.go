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
	conf.Port = environ.GetString("LISTEN_PORT", "7270")
	conf.DashboardPort = environ.GetString("DASHBOARD_LISTEN_PORT", "7280")
	conf.MaxOpenConn = environ.GetInt("MAX_OPEN_SIZE", 50)
	conf.Debug = environ.GetBool("DEBUG", false)
	conf.Token = proto.CalcSHA(environ.GetString("TOKEN", ""))

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
