package proto

import "sync"

func NewFramePool() *FramePool {
	p := &FramePool{pool: &sync.Pool{}, counter: NewCounter()}

	p.pool.New = func() any {
		v := &TransferFrame{}
		// 当池中存在空余对象时会直接返回，若无空余对象则通过New函数构造
		// 故此处和Get处都需要Reset
		v.Reset()
		return v
	}

	return p
}

func NewCPMPool() *CPMPool {
	p := &CPMPool{cpool: &sync.Pool{}, ppool: &sync.Pool{}}

	p.cpool.New = func() any {
		cm := &CMessage{Pm: &PMessage{}}
		cm.Reset()
		return cm
	}
	p.ppool.New = func() any {
		cm := &PMessage{}
		cm.Reset()
		return cm
	}

	return p
}

func NewHCPMPool() *HCPMessagePool {
	p := &HCPMessagePool{
		cpool:    &sync.Pool{},
		ppool:    &sync.Pool{},
		cCounter: NewCounter(),
		pCounter: NewCounter(),
	}

	p.cpool.New = func() any {
		cm := &ConsumerMessage{}
		cm.Reset()
		return cm
	}
	p.ppool.New = func() any {
		pm := &ProducerMessage{}
		pm.Reset()
		return pm
	}

	return p
}

// TODO：Put时检验counter是否正确

type FramePool struct {
	pool    *sync.Pool
	counter *Counter
}

func (p *FramePool) Get() *TransferFrame {
	v := p.pool.Get().(*TransferFrame)
	v.Reset()
	v.counter = p.counter.ValueBeforeIncrement()
	return v
}

func (p *FramePool) Put(v *TransferFrame) {
	v.Reset() // 此处在于释放内存
	p.pool.Put(v)
}

type CPMPool struct {
	cpool *sync.Pool
	ppool *sync.Pool
}

func (p *CPMPool) GetCM() *CMessage {
	v := p.cpool.Get().(*CMessage)
	v.Reset()
	return v
}

func (p *CPMPool) PutCM(v *CMessage) {
	v.Reset()
	p.cpool.Put(v)
}

func (p *CPMPool) GetPM() *PMessage {
	v := p.ppool.Get().(*PMessage)
	v.Reset()
	return v
}

func (p *CPMPool) PutPM(v *PMessage) {
	v.Reset()
	p.ppool.Put(v)
}

type HCPMessagePool struct {
	cpool    *sync.Pool
	ppool    *sync.Pool
	cCounter *Counter
	pCounter *Counter
}

func (m *HCPMessagePool) GetCM() *ConsumerMessage {
	v := m.cpool.Get().(*ConsumerMessage)
	v.Reset()
	v.counter = m.cCounter.ValueBeforeIncrement()

	return v
}

func (m *HCPMessagePool) PutCM(v *ConsumerMessage) {
	v.Reset()
	m.cpool.Put(v)
}

func (m *HCPMessagePool) GetPM() *ProducerMessage {
	v := m.ppool.Get().(*ProducerMessage)
	v.Reset()
	v.counter = m.pCounter.ValueBeforeIncrement()

	return v
}

func (m *HCPMessagePool) PutPM(v *ProducerMessage) {
	v.Reset()
	m.ppool.Put(v)
}

// -------------------------------------------------------------------

type bytesCache struct {
	i       int
	err     error
	oneByte []byte
	twoByte []byte
}

func (m *bytesCache) Reset() {
	m.oneByte = make([]byte, 1)
	m.twoByte = make([]byte, 2)
	m.i = 0
	m.err = nil
}

type bytesCachePool struct {
	pool *sync.Pool
}

func (p *bytesCachePool) Get() *bytesCache {
	v := p.pool.Get().(*bytesCache)
	v.Reset()

	return v
}

func (p *bytesCachePool) Put(v *bytesCache) {
	v.i = 0
	v.err = nil
	p.pool.Put(v)
}

// 临时切片缓存
var bcPool = &bytesCachePool{
	pool: &sync.Pool{
		New: func() any {
			v := &bytesCache{}
			v.Reset()
			return v
		},
	},
}
