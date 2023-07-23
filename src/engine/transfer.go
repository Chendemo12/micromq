package engine

import (
	"fmt"
	"github.com/Chendemo12/fastapi-tool/logger"
	"github.com/Chendemo12/functools/tcp"
	"github.com/Chendemo12/synshare-mq/src/proto"
)

// Transfer Engine 传输层实现
type Transfer interface {
	SetHost(host string)
	SetPort(port string)           // 设置绑定端口
	SetMaxOpenConn(num int)        // 设置最大连接数量
	SetLogger(logger logger.Iface) // logger
	// SetOnConnectedHandler 设置当客户端连接成功时的事件
	SetOnConnectedHandler(fn func(r *tcp.Remote))
	// SetOnClosedHandler 设置当客户端断开连接时的事件
	SetOnClosedHandler(fn func(addr string))
	// SetOnReceivedHandler 设置当收到客户端数据帧时的事件
	SetOnReceivedHandler(fn func(frame *proto.TransferFrame, r *tcp.Remote))
	// SetOnFrameParseErrorHandler 设置当客户端数据帧解析出错时的事件
	SetOnFrameParseErrorHandler(fn func(frame *proto.TransferFrame, r *tcp.Remote))
	Serve() error // 阻塞式启动TCP服务
	Stop()
}

// TCPTransfer TCP传输层实现
type TCPTransfer struct {
	host              string
	port              string
	maxOpenConn       int // 允许的最大连接数, 即 生产者+消费者最多有 maxOpenConn 个
	tcps              *tcp.Server
	logger            logger.Iface
	onConnected       func(r *tcp.Remote)
	onClosed          func(addr string)
	onReceived        func(frame *proto.TransferFrame, r *tcp.Remote)
	onFrameParseError func(frame *proto.TransferFrame, r *tcp.Remote)
}

func (t *TCPTransfer) init() *TCPTransfer {
	ts := tcp.NewTcpServer(
		&tcp.TcpsConfig{
			Host:           t.host,
			Port:           t.port,
			MessageHandler: t,
			ByteOrder:      "big",
			Logger:         t.logger,
			MaxOpenConn:    t.maxOpenConn,
		},
	)
	t.tcps = ts
	return t
}

func (t *TCPTransfer) SetHost(host string) {
	t.host = host
}

func (t *TCPTransfer) SetPort(port string) {
	t.port = port
}

func (t *TCPTransfer) SetMaxOpenConn(num int) {
	t.maxOpenConn = num
}

func (t *TCPTransfer) SetLogger(logger logger.Iface) {
	t.logger = logger
}

// SetOnConnectedHandler 设置当客户端连接成功时的事件
func (t *TCPTransfer) SetOnConnectedHandler(fn func(r *tcp.Remote)) {
	t.onConnected = fn
}

// SetOnClosedHandler 设置当客户端断开连接时的事件
func (t *TCPTransfer) SetOnClosedHandler(fn func(addr string)) {
	t.onClosed = fn
}

// SetOnReceivedHandler 设置当收到客户端数据帧时的事件
func (t *TCPTransfer) SetOnReceivedHandler(fn func(frame *proto.TransferFrame, r *tcp.Remote)) {
	t.onReceived = fn
}

// SetOnFrameParseErrorHandler 设置当客户端数据帧解析出错时的事件
func (t *TCPTransfer) SetOnFrameParseErrorHandler(fn func(frame *proto.TransferFrame, r *tcp.Remote)) {
	t.onFrameParseError = fn
}

func (t *TCPTransfer) OnAccepted(r *tcp.Remote) error {
	t.logger.Info(r.Addr(), " connected.")
	t.onConnected(r)

	return nil
}

// OnClosed 连接关闭, 删除此连接的消费者记录或生产者记录
func (t *TCPTransfer) OnClosed(r *tcp.Remote) error {
	addr := r.Addr()
	t.logger.Info(addr, " close connection.")

	t.onClosed(addr)
	return nil
}

func (t *TCPTransfer) Handler(r *tcp.Remote) error {
	if r.Len() < proto.FrameMinLength {
		return proto.ErrMessageNotFull
	}

	frame := framePool.Get()
	err := frame.ParseFrom(r) // 此操作不应并发读取，避免消息2覆盖消息1的缓冲区

	// 异步执行，立刻读取下一条消息
	// 对于生产者来说, 其不关心是否会有响应, 只需一条接一条的生产消息即可
	go func(f *proto.TransferFrame, c *tcp.Remote, err error) { // 处理消息帧
		defer framePool.Put(f)

		if err != nil {
			t.logger.Warn(fmt.Errorf("server parse frame failed: %v", err))
			t.onFrameParseError(f, c)
		} else {
			t.onReceived(f, c)
		}
	}(frame, r, err)

	return nil
}

// Serve 阻塞式启动TCP服务
func (t *TCPTransfer) Serve() error { return t.init().tcps.Serve() }

func (t *TCPTransfer) Stop() {
	if t.tcps != nil {
		t.tcps.Stop()
	}
	t.logger.Info("tcp server stopped!")
}
