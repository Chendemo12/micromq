package proto

import (
	"encoding/binary"
	"sync"
)

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

	p.ppool.New = func() any {
		pm := &PMessage{}
		pm.Reset()
		return pm
	}
	p.cpool.New = func() any {
		cm := &CMessage{}
		cm.Reset()
		cm.PM = p.GetPM()
		return cm
	}

	return p
}

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
	cv := p.counter.Value()
	if cv == 0 || v.counter <= cv {
		v.Reset() // 此处在于释放内存
		p.pool.Put(v)
	}
}

// HistoryNum 历史数量
func (p *FramePool) HistoryNum() uint64 { return p.counter.Value() }

type CPMPool struct {
	cpool *sync.Pool
	ppool *sync.Pool
}

// GetCM Attention: PM is nil
func (p *CPMPool) GetCM() *CMessage {
	v := p.cpool.Get().(*CMessage)
	v.Reset()

	return v
}

func (p *CPMPool) PutCM(v *CMessage) {
	if v.PM != nil {
		pm := v.PM
		v.PM = nil // release PM
		p.PutPM(pm)
	}

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

// -------------------------------------------------------------------

type bytesCache struct {
	i         int // 一个临时标号
	err       error
	oneByte   []byte
	twoByte   []byte
	byteOrder string
}

func (m *bytesCache) Reset() {
	m.oneByte = make([]byte, 1)
	m.twoByte = make([]byte, 2)
	m.i = 0
	m.err = nil
}

func (m *bytesCache) OneValue() int { return int(m.oneByte[0]) }

func (m *bytesCache) TwoValue() int {
	if m.byteOrder == "little" { // 显式设置为小端解析
		return int(binary.LittleEndian.Uint16(m.twoByte))
	}
	return int(binary.BigEndian.Uint16(m.twoByte))
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
