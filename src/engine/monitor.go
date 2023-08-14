package engine

import (
	"context"
	"github.com/Chendemo12/fastapi-tool/cronjob"
	"github.com/Chendemo12/micromq/src/proto"
	"sync"
	"time"
)

const (
	registerTimeoutRate  = 1
	keepaliveTimeoutRate = 3
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

// TimeInfo 关于监视器有关的时间信息
type TimeInfo struct {
	Addr         string         `json:"addr"`
	LinkType     proto.LinkType `json:"link_type"`
	ConnectedAt  int64          `json:"connected_at"`  // 连接成功时间戳
	RegisteredAt int64          `json:"registered_at"` // 注册成功时间戳
	HeartbeatAt  int64          `json:"heartbeat_at"`  // 最近的一个心跳时间戳
}

func (t *TimeInfo) IsFree() bool { return t.Addr == "" }

func (t *TimeInfo) Reset() {
	t.Addr = ""
	t.LinkType = ""
	t.ConnectedAt = 0
	t.RegisteredAt = 0
	t.HeartbeatAt = 0
}

// Monitor 监视器
// 1. 检测连接成功但不注册的客户端
// 2. 检测心跳超时的客户端
// 超时时主动断开连接
type Monitor struct {
	cronjob.Job
	broker    *Engine
	timeInfos []*TimeInfo
	lock      *sync.RWMutex
}

func (k *Monitor) findTimeout() ([]TimeoutEvent, []TimeoutEvent, []TimeoutEvent, []TimeoutEvent) {
	t := time.Now().Unix()
	hInterval := k.broker.HeartbeatInterval() * keepaliveTimeoutRate
	rInterval := k.broker.HeartbeatInterval() * registerTimeoutRate

	rTimeoutConsumers := make([]TimeoutEvent, 0)
	rTimeoutProducers := make([]TimeoutEvent, 0)
	hTimeoutConsumers := make([]TimeoutEvent, 0)
	hTimeoutProducers := make([]TimeoutEvent, 0)

	for _, c := range k.timeInfos {
		info := k.ReadClientTimeInfo(c.Addr, proto.ConsumerLinkType)

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

	for _, c := range k.timeInfos {
		info := k.ReadClientTimeInfo(c.Addr, proto.ConsumerLinkType)
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

func (k *Monitor) closeRegisterTimeout(consumers, producers []TimeoutEvent) {
	for _, c := range consumers {
		con := c
		go func() {
			k.broker.Logger().Info("consumer register timeout, actively close the connection with: " + con.Addr)
			k.broker.closeConnection(con.Addr) // 关闭过期连接
			// 当连接被关闭时，会触发 OnClosed 回调，因此无需主动删除记录
			//k.broker.RemoveConsumer(con.Addr)
			k.broker.EventHandler().OnConsumerRegisterTimeout(con)
		}()
	}

	for _, c := range producers {
		con := c
		go func() {
			k.broker.Logger().Info("producer register timeout, actively close the connection with: " + con.Addr)
			k.broker.closeConnection(con.Addr)
			k.broker.EventHandler().OnProducerRegisterTimeout(con)
		}()
	}
}

func (k *Monitor) closeHeartbeatTimeout(consumers, producers []TimeoutEvent) {
	// 关闭过期连接
	for _, c := range consumers {
		con := c
		go func() {
			k.broker.Logger().Info("consumer heartbeat timeout, actively close the connection with: " + con.Addr)
			k.broker.closeConnection(con.Addr)
			k.broker.EventHandler().OnConsumerHeartbeatTimeout(con)
		}()
	}
	for _, c := range producers {
		con := c
		go func() {
			k.broker.Logger().Info("producer heartbeat timeout, actively close the connection with: " + con.Addr)
			k.broker.closeConnection(con.Addr)
			k.broker.EventHandler().OnProducerHeartbeatTimeout(con)
		}()
	}
}

// ============================= TimeInfo methods =============================

func (k *Monitor) OnClientConnected(addr string) {
	k.lock.Lock()
	defer k.lock.Unlock()

	for i := 0; i < len(k.timeInfos); i++ {
		if k.timeInfos[i].IsFree() {
			k.timeInfos[i].Addr = addr
			k.timeInfos[i].ConnectedAt = time.Now().Unix()
			return
		}
	}
	// 扩容
	k.timeInfos = append(k.timeInfos, &TimeInfo{
		Addr:         addr,
		LinkType:     "",
		ConnectedAt:  time.Now().Unix(),
		RegisteredAt: 0,
		HeartbeatAt:  0,
	})
}

// OnClientClosed 连接关闭，清空时间信息
func (k *Monitor) OnClientClosed(addr string) {
	k.lock.Lock()
	defer k.lock.Unlock()

	for i := 0; i < len(k.timeInfos); i++ {
		if k.timeInfos[i].Addr == addr {
			k.timeInfos[i].Reset() // 重置数据，而非删除对象
			return
		}
	}
}

func (k *Monitor) OnClientRegistered(addr string, linkType proto.LinkType) {
	// TODO: 不加锁，是否OK
	for i := 0; i < len(k.timeInfos); i++ {
		if k.timeInfos[i].Addr == addr {
			k.timeInfos[i].LinkType = linkType
			k.timeInfos[i].RegisteredAt = time.Now().Unix()
			return
		}
	}
}

func (k *Monitor) OnClientHeartbeat(addr string) {
	for i := 0; i < len(k.timeInfos); i++ {
		if k.timeInfos[i].Addr == addr {
			k.timeInfos[i].HeartbeatAt = time.Now().Unix()
			return
		}
	}
}

func (k *Monitor) ReadClientTimeInfo(addr string, linkType proto.LinkType) *TimeInfo {
	for i := 0; i < len(k.timeInfos); i++ {
		if k.timeInfos[i].Addr == addr && k.timeInfos[i].LinkType == linkType {
			return k.timeInfos[i]
		}
	}

	// 很难触发
	return &TimeInfo{Addr: addr}
}

// ============================= Schedule handler =============================

func (k *Monitor) String() string { return "broker-monitor" }

func (k *Monitor) Interval() time.Duration {
	return time.Duration(k.broker.HeartbeatInterval()/2) * time.Second
}

func (k *Monitor) OnStartup() {
	k.lock = &sync.RWMutex{}
	k.timeInfos = make([]*TimeInfo, k.broker.conf.MaxOpenConn)

	for i := 0; i < k.broker.conf.MaxOpenConn; i++ {
		k.timeInfos[i] = &TimeInfo{}
	}
}

func (k *Monitor) Do(ctx context.Context) error {
	k.lock.RLock()
	rTimeoutConsumers, rTimeoutProducers, hTimeoutConsumers, hTimeoutProducers := k.findTimeout()
	k.lock.RUnlock()

	// 必须先释放锁才能继续清除连接
	k.closeRegisterTimeout(rTimeoutConsumers, rTimeoutProducers)
	k.closeHeartbeatTimeout(hTimeoutConsumers, hTimeoutProducers)

	return nil
}
