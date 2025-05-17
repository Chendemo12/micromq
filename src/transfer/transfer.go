package transfer

import (
	"github.com/Chendemo12/micromq/src/proto"
)

// Conn 客户端连接实现
type Conn interface {
	Addr() string                // 获取远端地址
	IsConnected() bool           // 是否已连接
	Close() error                // 关闭与对端的连接
	Read(p []byte) (int, error)  // 将缓冲区的数据读取到切片buf内，并返回实际读取的数据长度
	ReadN(n int) []byte          // 读取N个字节的数据
	Len() int                    // 获取接收数据的总长度
	Copy(p []byte) (int, error)  // 将缓冲区的数据拷贝到切片p内，并返回实际读取的数据长度
	Write(p []byte) (int, error) // 将切片buf中的内容追加到发数据缓冲区内，并返回写入的数据长度
	Seek(offset int64, whence int) (int64, error)
	Drain() error // 将缓冲区的数据发生到客户端
}

// Transfer Engine 传输层实现
type Transfer interface {
	SetHost(host string)
	SetPort(port string)    // 设置绑定端口
	SetMaxOpenConn(num int) // 设置最大连接数量
	// SetOnConnectedHandler 设置当客户端连接成功时的事件
	SetOnConnectedHandler(fn func(c Conn))
	// SetOnClosedHandler 设置当客户端断开连接时的事件
	SetOnClosedHandler(fn func(addr string))
	// SetOnReceivedHandler 设置当收到客户端数据帧时的事件
	SetOnReceivedHandler(fn func(frame *proto.TransferFrame, c Conn))
	// SetOnFrameParseErrorHandler 设置当客户端数据帧解析出错时的事件
	SetOnFrameParseErrorHandler(fn func(frame *proto.TransferFrame, c Conn))
	Close(addr string) error // 主动关闭一个客户端连接
	Serve() error            // 阻塞式启动服务
	Stop()
}

var framePool = proto.NewFramePool()
