package mq

import (
	"github.com/Chendemo12/fastapi"
	"github.com/Chendemo12/micromq/src/edge"
)

func (m *MQ) initHttp() *MQ {
	m.faster = fastapi.New(m.conf.AppName, m.conf.Version, m.conf.Debug, m)

	m.faster.SetLogger(m.logger)
	m.faster.SetDescription(m.conf.AppName + " Api Service")
	m.faster.SetShutdownTimeout(5)
	m.faster.DisableBaseRoutes()
	m.faster.DisableRequestValidate()
	m.faster.DisableResponseValidate()
	m.faster.DisableMultipleProcess()

	if m.conf.EdgeEnabled {
		m.faster.IncludeRouter(edge.Router())
	}

	return m
}
