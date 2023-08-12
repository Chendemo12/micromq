package engine

import (
	"context"
	"github.com/Chendemo12/fastapi-tool/cronjob"
	"github.com/Chendemo12/micromq/src/proto"
	"time"
)

const (
	monitorPollingInterval time.Duration = time.Second * 10
	registerTimeoutRate                  = 1
	keepaliveTimeoutRate                 = 3
)

type TimeoutEventType string

const (
	HeartbeatTimeoutEvent TimeoutEventType = "HEARTBEAT_TIMEOUT"
	RegisterTimeoutEvent  TimeoutEventType = "REGISTER_TIMEOUT"
)

// TimeoutEvent 超时事件
type TimeoutEvent struct {
	Addr            string           `json:"addr,omitempty"`
	EventType       TimeoutEventType `json:"event_type,omitempty"`
	LinkType        proto.LinkType   `json:"link_type,omitempty"`
	TimeoutInterval float64          `json:"timeout_interval,omitempty"`
	ConnectedAt     int64            `json:"connected_at"`
	TimeoutAt       int64            `json:"timeout_at"`
}

// Monitor 监视器
// 1. 检测连接成功但不注册的客户端
// 2. 检测心跳超时的客户端
// 超时时主动断开连接
type Monitor struct {
	cronjob.Job
	e *Engine
}

func (k Monitor) String() string { return "engine-keepalive-monitor" }

func (k Monitor) Interval() time.Duration {
	return monitorPollingInterval
}

func (k Monitor) findTimeout() ([]TimeoutEvent, []TimeoutEvent, []TimeoutEvent, []TimeoutEvent) {
	t := time.Now().Unix()
	hInterval := k.e.HeartbeatInterval() * keepaliveTimeoutRate
	rInterval := k.e.HeartbeatInterval() * registerTimeoutRate

	rTimeoutConsumers := make([]TimeoutEvent, 0)
	rTimeoutProducers := make([]TimeoutEvent, 0)
	hTimeoutConsumers := make([]TimeoutEvent, 0)
	hTimeoutProducers := make([]TimeoutEvent, 0)

	for _, c := range k.e.consumers {
		info := k.e.findTimeInfo(c.Addr)

		if c.IsFree() || info.ConnectedAt == 0 {
			continue
		}

		// 连接成功 -> 注册成功 -> 心跳超时
		if info.RegisteredAt == 0 { // 连接成功，但尚未注册，判断是否注册超时
			if t-info.ConnectedAt > int64(rInterval) {
				rTimeoutConsumers = append(rTimeoutConsumers, TimeoutEvent{
					Addr:            c.Addr,
					EventType:       RegisterTimeoutEvent,
					LinkType:        proto.ConsumerLinkType,
					ConnectedAt:     info.ConnectedAt,
					TimeoutInterval: rInterval,
					TimeoutAt:       time.Now().Unix(),
				})
			}
		} else { // 注册成功，判断心跳是否超时
			if t-info.ConnectedAt > int64(hInterval) {
				hTimeoutConsumers = append(hTimeoutConsumers, TimeoutEvent{
					Addr:            c.Addr,
					EventType:       HeartbeatTimeoutEvent,
					LinkType:        proto.ConsumerLinkType,
					ConnectedAt:     info.ConnectedAt,
					TimeoutInterval: hInterval,
					TimeoutAt:       time.Now().Unix(),
				})
			}
		}

	}

	for _, c := range k.e.producers {
		info := k.e.findTimeInfo(c.Addr)
		if c.IsFree() || info.ConnectedAt == 0 {
			continue
		}

		if info.RegisteredAt == 0 { // 连接成功，但尚未注册，判断是否注册超时
			if t-info.ConnectedAt > int64(rInterval) { // 注册超时
				rTimeoutProducers = append(rTimeoutProducers, TimeoutEvent{
					Addr:            c.Addr,
					EventType:       RegisterTimeoutEvent,
					LinkType:        proto.ProducerLinkType,
					ConnectedAt:     info.ConnectedAt,
					TimeoutInterval: rInterval,
					TimeoutAt:       time.Now().Unix(),
				})
			}
		} else {
			if t-info.ConnectedAt > int64(hInterval) {
				hTimeoutProducers = append(hTimeoutProducers, TimeoutEvent{
					Addr:            c.Addr,
					EventType:       HeartbeatTimeoutEvent,
					LinkType:        proto.ProducerLinkType,
					ConnectedAt:     info.ConnectedAt,
					TimeoutInterval: hInterval,
					TimeoutAt:       time.Now().Unix(),
				})
			}
		}
	}

	return rTimeoutConsumers, rTimeoutProducers, hTimeoutConsumers, hTimeoutProducers
}

func (k Monitor) closeRegisterTimeout(consumers, producers []TimeoutEvent) {
	for _, c := range consumers {
		con := c
		go func() {
			k.e.Logger().Info("consumer register timeout: " + con.Addr)
			_ = k.e.transfer.Close(con.Addr) // 关闭过期连接
			//k.e.RemoveConsumer(con.Addr)
			k.e.EventHandler().OnConsumerRegisterTimeout(con)
		}()
	}

	for _, c := range producers {
		con := c
		go func() {
			k.e.Logger().Info("producer register timeout: " + con.Addr)
			_ = k.e.transfer.Close(con.Addr)
			//k.e.RemoveProducer(con.Addr)
			k.e.EventHandler().OnProducerRegisterTimeout(con)
		}()
	}
}

func (k Monitor) closeHeartbeatTimeout(consumers, producers []TimeoutEvent) {
	// 关闭过期连接
	for _, c := range consumers {
		con := c
		go func() {
			k.e.Logger().Info("consumer heartbeat timeout: " + con.Addr)
			_ = k.e.transfer.Close(con.Addr)
			//k.e.RemoveConsumer(con.Addr)
			k.e.EventHandler().OnConsumerHeartbeatTimeout(con)
		}()
	}
	for _, c := range producers {
		con := c
		go func() {
			k.e.Logger().Info("producer heartbeat timeout: " + con.Addr)
			_ = k.e.transfer.Close(con.Addr)
			//k.e.RemoveProducer(con.Addr)
			k.e.EventHandler().OnProducerHeartbeatTimeout(con)
		}()
	}
}

func (k Monitor) Do(ctx context.Context) error {
	k.e.cpLock.RLock()
	rTimeoutConsumers, rTimeoutProducers, hTimeoutConsumers, hTimeoutProducers := k.findTimeout()
	k.e.cpLock.RUnlock()

	// 必须先释放锁才能继续清除连接
	k.closeRegisterTimeout(rTimeoutConsumers, rTimeoutProducers)
	k.closeHeartbeatTimeout(hTimeoutConsumers, hTimeoutProducers)

	return nil
}
