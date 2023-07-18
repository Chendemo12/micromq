package proto

import "sync"

// 临时切片缓存
var bcPool = &bytesCachePool{
	pool: &sync.Pool{
		New: func() any {
			v := &bytesCache{
				oneByte: make([]byte, 1),
				twoByte: make([]byte, 2),
			}
			return v
		},
	},
}

func NewFramePool() *FramePool {
	p := &FramePool{pool: &sync.Pool{}}

	p.pool.New = func() any {
		v := &TransferFrame{}
		v.Reset()
		return v
	}

	return p
}

type FramePool struct {
	pool *sync.Pool
}

func (p *FramePool) Get() *TransferFrame {
	v := p.pool.Get().(*TransferFrame)
	return v
}

func (p *FramePool) Put(v *TransferFrame) {
	v.Reset()
	p.pool.Put(v)
}

func NewCMPool() *CMPool {
	p := &CMPool{pool: &sync.Pool{}}

	p.pool.New = func() any {
		cm := CMessage{Pm: &PMessage{}}
		cm.Reset()
		return cm
	}

	return p
}

type CMPool struct {
	pool *sync.Pool
}

func (p *CMPool) Get() *CMessage {
	v := p.pool.Get()
	return v.(*CMessage)
}

func (p *CMPool) Put(v *CMessage) {
	v.Reset()
	p.pool.Put(v)
}

type bytesCache struct {
	i       int
	err     error
	oneByte []byte
	twoByte []byte
}

type bytesCachePool struct {
	pool *sync.Pool
}

func (p *bytesCachePool) Get() *bytesCache {
	v := p.pool.Get().(*bytesCache)
	v.oneByte[0] = 0
	v.twoByte[0] = 0
	v.twoByte[1] = 0
	v.i = 0
	v.err = nil
	return v
}

func (p *bytesCachePool) Put(v *bytesCache) {
	v.i = 0
	v.err = nil
	p.pool.Put(v)
}

func NewHCPMPool() *HCPMessagePool {
	p := &HCPMessagePool{cpool: &sync.Pool{}, ppool: &sync.Pool{}}

	p.cpool.New = func() any {
		cm := &ConsumerMessage{}
		return cm
	}
	p.ppool.New = func() any {
		pm := &ProducerMessage{}
		return pm
	}

	return p
}

type HCPMessagePool struct {
	cpool *sync.Pool
	ppool *sync.Pool
}

func (m *HCPMessagePool) GetCM() *ConsumerMessage {
	return m.cpool.Get().(*ConsumerMessage)
}

func (m *HCPMessagePool) GetPM() *ProducerMessage {
	return m.ppool.Get().(*ProducerMessage)
}

func (m *HCPMessagePool) PutPM(v *ProducerMessage) {
	m.ppool.Put(v)
}

func (m *HCPMessagePool) PutCM(v *ConsumerMessage) {
	m.cpool.Put(v)
}
