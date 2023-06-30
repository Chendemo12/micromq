package main

import (
	"github.com/Chendemo12/functools/environ"
	"github.com/Chendemo12/functools/zaplog"
	"github.com/Chendemo12/synshare-mq/src/engine"
	"github.com/Chendemo12/synshare-mq/src/transfer"
)

func main() {
	listenPort := environ.GetString("LISTEN_PORT", "8090")
	size := environ.GetInt("HISTORY_SIZE", 200)
	debug := environ.GetBool("DEBUG", false)

	conf := &zaplog.Config{
		Filename:   "synshare-mq",
		Level:      zaplog.WARNING,
		Rotation:   10,
		Retention:  5,
		MaxBackups: 10,
		Compress:   false,
	}

	if debug {
		conf.Level = zaplog.DEBUG
	}

	handler := transfer.Transfer{
		Port:   listenPort,
		Logger: zaplog.NewLogger(conf).Sugar(),
	}

	handler.SetEngine(engine.New(size)).Run()
}
