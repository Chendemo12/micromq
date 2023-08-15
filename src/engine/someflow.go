package engine

import (
	"bytes"
	"fmt"
	"github.com/Chendemo12/micromq/src/proto"
	"time"
)

//
// ================================= register =================================
//

func (e *Engine) registerParser(args *ChainArgs) (stop bool) {
	args.rm = &proto.RegisterMessage{}
	args.frame.Type = proto.RegisterMessageRespType

	err := args.frame.Unmarshal(args.rm)
	if err != nil {
		// 注册消息帧解析失败，令重新发起注册
		args.frame.Type = proto.ReRegisterMessageType
		args.frame.Data = []byte{} // 重新发起注册暂无消息体
		args.err = fmt.Errorf("register message parse failed, %v, let re-register: %s", err, args.con.Addr())
	}

	return err != nil
}

func (e *Engine) registerAuth(args *ChainArgs) (stop bool) {
	// 无论注册成功与否都需要构建返回值
	args.resp = &proto.MessageResponse{
		Status:         proto.RefusedStatus,
		Offset:         0,
		ReceiveTime:    time.Now().Unix(),
		TickerInterval: int(e.ProducerSendInterval()),
		Keepalive:      e.HeartbeatInterval(),
	}

	e.Logger().Info(fmt.Sprintf("receive '%s' from  %s", args.rm, args.con.Addr()))

	if !e.IsTokenCorrect(args.rm.Token) {
		// 需要认证，但是密钥不正确
		args.resp.Status = proto.TokenIncorrectStatus
		e.Logger().Info(args.con.Addr(), " has wrong token, refused.")

		stop = true
	}

	return
}

// 密钥验证通过, 寻找空闲空间
func (e *Engine) registerAllow(args *ChainArgs) (stop bool) {
	// 记录注册时间戳
	e.monitor.OnClientRegistered(args.con.Addr(), args.rm.Type)

	switch args.rm.Type {
	case proto.ProducerLinkType:
		e.cpLock.Lock() // 上个锁, 防止刚注册就断开
		if i := e.findProducerSlot(); i != -1 {
			// 记录生产者, 用于判断其后是否要返回消息投递后的确认消息
			producer := e.producers[i]
			producer.SetConn(args.con)
			producer.Conf = &ProducerConfig{
				Ack:            args.rm.Ack,
				TickerInterval: e.ProducerSendInterval(),
			}

			args.resp.Status = proto.AcceptedStatus
			args.producer = producer
		}

		e.cpLock.Unlock()

	case proto.ConsumerLinkType: // 注册消费者
		e.cpLock.Lock()
		if i := e.findConsumerSlot(); i != -1 {
			c := e.consumers[i]
			c.SetConn(args.con)
			c.Conf = &ConsumerConfig{Topics: args.rm.Topics, Ack: args.rm.Ack}

			for _, name := range args.rm.Topics {
				e.GetTopic([]byte(name)).AddConsumer(c)
			}
			args.resp.Status = proto.AcceptedStatus
		}

		e.cpLock.Unlock()
	}

	// 输出日志
	if args.resp.Status == proto.AcceptedStatus {
		e.Logger().Info(args.con.Addr() + " register successfully")
	} else {
		e.Logger().Warn(fmt.Sprintf(
			"%s register failed, because of: %s, actively close the connection",
			args.con.Addr(), proto.GetMessageResponseStatusText(args.resp.Status),
		))
		// 注册失败，主动断开连接
		e.closeConnection(args.con.Addr())
	}

	return
}

// 触发回调
func (e *Engine) registerCallback(args *ChainArgs) (stop bool) {
	if args.resp.Accepted() {
		switch args.rm.Type {
		case proto.ProducerLinkType:
			go e.EventHandler().OnProducerRegister(args.con.Addr())
		case proto.ConsumerLinkType:
			go e.EventHandler().OnConsumerRegister(args.con.Addr())
		}
	}

	return
}

//
// ============================= producer message =============================
//

// 处理生产者消息帧，此处需要判断生产者是否已注册
func (e *Engine) producerNotFound(args *ChainArgs) (stop bool) {
	producer, exist := e.QueryProducer(args.con.Addr())

	if !exist {
		e.Logger().Debug("found unregister producer, let re-register: ", args.con.Addr())

		// 重新发起注册暂无消息体
		args.frame.Type = proto.ReRegisterMessageType
		args.frame.Data = []byte{}
		//args.err = fmt.Errorf("found unregister producer, let re-register: %s", args.con.Addr())

		// 返回令客户端重新注册命令
		return true
	}

	args.producer = producer

	return
}

func (e *Engine) pmParser(args *ChainArgs) (stop bool) {
	// 存在多个消息封装为一个帧
	args.pms = make([]*proto.PMessage, 0)
	stream := bytes.NewReader(args.frame.Data)

	// 循环解析生产者消息
	args.err = nil
	for args.err == nil && stream.Len() > 0 {
		pm := cpmp.GetPM()
		args.err = pm.ParseFrom(stream)
		if args.err == nil {
			args.pms = append(args.pms, pm)
		} else {
			cpmp.PutPM(pm)
		}
	}

	if len(args.pms) < 1 {
		args.err = ErrPMNotFound
		// 无需向客户端返回解析失败响应，客户端在收不到FIN时会自行处理
		return true
	}

	return
}

func (e *Engine) pmPublisher(args *ChainArgs) (stop bool) {
	// 若是批量发送数据,则取最后一条消息的偏移量
	var offset uint64 = 0
	for _, pm := range args.pms {
		offset = e.Publisher(pm)
	}

	if args.producer.NeedConfirm() {
		// 需要返回确认消息给客户端
		args.frame.Type = proto.MessageRespType
		args.resp = &proto.MessageResponse{
			Status:      proto.AcceptedStatus,
			Offset:      offset,
			ReceiveTime: time.Now().Unix(),
		}
	}

	return
}

//
// ============================= heartbeat message =============================
//

func (e *Engine) receiveHeartbeat(args *ChainArgs) (stop bool) {
	e.monitor.OnClientHeartbeat(args.con.Addr())

	return
}
