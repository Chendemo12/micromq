package engine

import (
	"fmt"

	"github.com/Chendemo12/micromq/src/proto"
)

// ================================= register =================================

func (e *Engine) registerParser(args *ChainArgs) (stop bool) {
	args.rm = &proto.RegisterMessage{}
	// 设置为注册响应
	args.resp.Type = proto.RegisterMessageRespType
	// 默认拒绝注册
	args.resp.Status = proto.ReRegisterStatus
	args.resp.TickerInterval = int(e.ProducerSendInterval())
	args.resp.Keepalive = e.HeartbeatInterval()

	// 消息解密并反序列化
	err := args.frame.Unmarshal(args.rm, e.tokenCrypto.Decrypt)
	stop = err != nil

	if err != nil { // 解密或反序列化失败
		args.resp.Status = proto.ReRegisterStatus
		e.Logger().Info(args.con.Addr(), " register message decrypt failed: ", err.Error())
	} else {
		e.Logger().Info(args.con.Addr(), " register message decrypt successfully.")
	}

	return
}

func (e *Engine) registerAuth(args *ChainArgs) (stop bool) {
	e.Logger().Info(fmt.Sprintf("receive '%s' from  %s", args.rm, args.con.Addr()))
	// 此处已解密成功
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
		e.Logger().Info(fmt.Sprintf("%s::%s register successfully", args.con.Addr(), args.rm.Type))
	} else {
		stop = true
		e.Logger().Warn(fmt.Sprintf(
			"%s register failed, because of: %s, actively close the connection",
			args.con.Addr(), proto.GetMessageResponseStatusText(args.resp.Status),
		))
		// 注册失败，主动断开连接, 不再回复响应和触发回调
		args.SetError(ErrNoNeedToReply) // 连接已经被主动关闭了,无法再回复响应
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

// ============================= producer message =============================

// 处理生产者消息帧，此处需要判断生产者是否已注册
func (e *Engine) producerNotFound(args *ChainArgs) (stop bool) {
	producer, exist := e.QueryProducer(args.con.Addr())

	if !exist {
		e.Logger().Debug("found unregister producer, let re-register: ", args.con.Addr())
		// 未注册, 令客户端重新注册
		args.resp.Status = proto.ReRegisterStatus
		stop = true
	} else {
		args.producer = producer
	}

	return
}

func (e *Engine) pmParser(args *ChainArgs) (stop bool) {
	// 存在多个消息封装为一个帧
	args.pms = make([]*proto.PMessage, 0)
	err := proto.FrameSplit[*proto.PMessage](args.frame, &args.pms, e.Crypto().Decrypt)

	if err != nil {
		// 消息解析错误
		args.resp.Status = proto.TokenIncorrectStatus
		args.SetError(fmt.Errorf("frame decrypt failed: %v", err))

		return true
	}

	if len(args.pms) < 1 {
		args.resp.Status = proto.ReRegisterStatus
		// 无需向客户端返回解析失败响应，客户端在收不到FIN时会自行处理
		args.SetError(ErrPMNotFound)
		stop = true
	}

	return
}

func (e *Engine) pmPublisher(args *ChainArgs) (stop bool) {
	// 若是批量发送数据,则取最后一条消息的偏移量
	var offset uint64 = 0
	for _, pm := range args.pms {
		offset = e.Publisher(pm)
	}

	args.resp.Offset = offset

	if !args.producer.NeedConfirm() {
		// 不需要返回确认消息给客户端
		args.SetError(ErrNoNeedToReply)
		stop = true
	}

	return
}

// ============================= heartbeat message =============================

func (e *Engine) receiveHeartbeat(args *ChainArgs) (stop bool) {
	args.SetError(ErrNoNeedToReply) // 不需要回复
	e.monitor.OnClientHeartbeat(args.con.Addr())

	return
}
