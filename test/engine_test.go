package test

import (
	"github.com/Chendemo12/functools/environ"
	"github.com/Chendemo12/functools/zaplog"
	"github.com/Chendemo12/synshare-mq/src/engine"
	"testing"
)

func TestEngine(t *testing.T) {
	listenPort := environ.GetString("LISTEN_PORT", "8090")
	size := environ.GetInt("MAX_OPEN_SIZE", 50)
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

	handler := engine.New(engine.Config{
		Host:        "0.0.0.0",
		Port:        listenPort,
		MaxOpenConn: size,
		BufferSize:  100,
		Logger:      zaplog.NewLogger(conf).Sugar(),
	})
	handler.GetTopic([]byte("DDNS"))

	//handler.Run()
}
