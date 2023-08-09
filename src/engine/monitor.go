package engine

import (
	"context"
	"github.com/Chendemo12/fastapi-tool/cronjob"
	"github.com/Chendemo12/micromq/src/proto"
	"time"
)

const (
	monitorPollingInterval   time.Duration = time.Second * 10
	keepalivePollingInterval time.Duration = time.Second * 10
)

type TimeoutEventType string

const (
	HeartbeatEvent       TimeoutEventType = "HEARTBEAT_TIMEOUT"
	RegisterTimeoutEvent TimeoutEventType = "REGISTER_TIMEOUT"
)

// TimeoutEvent 注册超时事件
type TimeoutEvent struct {
	Addr            string           `json:"addr,omitempty"`
	EventType       TimeoutEventType `json:"event_type,omitempty"`
	LinkType        proto.LinkType   `json:"link_type,omitempty"`
	ConnectedAt     time.Time        `json:"connected_at"`
	TimeoutInterval time.Duration    `json:"timeout_interval,omitempty"`
	TimeoutAt       time.Time        `json:"timeout_at"`
}

// KeepaliveMonitor 心跳监视器,当心跳超时时主动断开连接
type KeepaliveMonitor struct {
	cronjob.Job
	e     *Engine
	links []*TimeoutEvent
}

func (k KeepaliveMonitor) String() string { return "engine-keepalive-monitor" }

func (k KeepaliveMonitor) Interval() time.Duration {
	return keepalivePollingInterval
}

func (k KeepaliveMonitor) OnStartup() {
	k.links = make([]*TimeoutEvent, 0)
}

func (k KeepaliveMonitor) OnShutdown() {
	k.links = make([]*TimeoutEvent, 0)
}

func (k KeepaliveMonitor) Do(ctx context.Context) error {
	//TODO implement me
	return nil
}

// RegisterMonitor 检测哪些连接成功但不注册的客户端
type RegisterMonitor struct {
	cronjob.Job
	e     *Engine
	links []*TimeoutEvent
}

func (t *RegisterMonitor) String() string { return "engine-register-monitor" }

func (t *RegisterMonitor) Interval() time.Duration {
	return monitorPollingInterval
}

func (t *RegisterMonitor) OnStartup() {
	t.links = make([]*TimeoutEvent, 0)
}

func (t *RegisterMonitor) OnShutdown() {
	t.links = make([]*TimeoutEvent, 0)
}

func (t *RegisterMonitor) Do(ctx context.Context) error {
	//TODO implement me
	return nil
}
