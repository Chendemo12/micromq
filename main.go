package main

import (
	"github.com/Chendemo12/functools/environ"
	"github.com/Chendemo12/functools/zaplog"
	"github.com/Chendemo12/micromq/src/mq"
)

func main() {
	listenPort := environ.GetString("LISTEN_PORT", "7270")
	dashboardPort := environ.GetString("DASHBOARD_LISTEN_PORT", "7280")
	size := environ.GetInt("MAX_OPEN_SIZE", 50)
	debug := environ.GetBool("DEBUG", false)

	conf := &zaplog.Config{
		Filename:   "micromq",
		Level:      zaplog.WARNING,
		Rotation:   10,
		Retention:  5,
		MaxBackups: 10,
		Compress:   false,
	}

	if debug {
		conf.Level = zaplog.DEBUG
	}

	mqConf := mq.DefaultConf()
	mqConf.Port = listenPort
	mqConf.DashboardPort = dashboardPort
	mqConf.MaxOpenConn = size
	mqConf.Logger = zaplog.NewLogger(conf).Sugar()

	handler := mq.New(mqConf)
	handler.Run()
}
