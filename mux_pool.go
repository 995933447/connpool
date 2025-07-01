package connpool

import (
	"fmt"
	"golang.org/x/sync/singleflight"
	"sync"
	"sync/atomic"
	"time"
)

type MuxPool struct {
	New                func() (interface{}, error)
	Ping               func(interface{}) bool
	Close              func(interface{})
	store              []*muxItem
	Idle               time.Duration
	sfl                singleflight.Group
	poolMu             sync.RWMutex
	storeMus           []*sync.RWMutex
	maxCap             int
	maxPos             int
	cap                atomic.Int32
	doGetCountMoreThan atomic.Int32
}

type muxItem struct {
	data       interface{}
	refCount   atomic.Int32
	isBlocking bool
	heartbeat  time.Time
}

func NewMuxPool(initCap, maxCap int, newFunc func() (interface{}, error)) (*MuxPool, error) {
	if maxCap == 0 || initCap > maxCap {
		return nil, fmt.Errorf("invalid capacity settings")
	}
	p := new(MuxPool)
	p.doGetCountMoreThan.Store(-1)
	p.maxCap = maxCap
	p.store = make([]*muxItem, maxCap)
	if newFunc != nil {
		p.New = newFunc
	}
	p.storeMus = make([]*sync.RWMutex, maxCap)
	for i := 0; i < maxCap; i++ {
		p.storeMus[i] = &sync.RWMutex{}
	}
	for i := 0; i < initCap; i++ {
		conn, err := p.create()
		if err != nil {
			return nil, err
		}
		p.store[i] = &muxItem{
			data:      conn,
			heartbeat: time.Now(),
		}
	}
	p.cap.Store(int32(initCap))
	p.maxPos = initCap - 1
	return p, nil
}

func (p *MuxPool) create() (interface{}, error) {
	if p.New == nil {
		return nil, fmt.Errorf("Pool.New is nil, can not create connection")
	}
	return p.New()
}

func (p *MuxPool) Get() (interface{}, bool, error) {
	p.poolMu.RLock()
	defer p.poolMu.RUnlock()

	var selected *muxItem

	capInt32 := p.cap.Load()
	if capInt32 > 0 {
		idx := p.doGetCountMoreThan.Add(1) / int32(p.maxPos+1)
		for i := 0; i <= p.maxPos; i++ {
			it := p.store[idx]

			idx++
			if idx > int32(p.maxPos) {
				idx = 0
			}

			if it == nil {
				continue
			}

			if p.Ping != nil && !p.Ping(it.data) {
				p.remove(i)
				continue
			}

			// 空闲链接
			if it.refCount.Load() <= 0 {
				selected = it
				break
			}

			// 连接是否阻塞，如果已经阻塞（如何写入缓冲区满了），表示单连接负载高了，取其他连接
			if !it.isBlocking {
				selected = it
				break
			}
		}
	}

	if selected != nil {
		selected.refCount.Add(1)
		return selected.data, false, nil
	}

	var isNew bool
	newItemAny, err, _ := p.sfl.Do("create", func() (interface{}, error) {
		isNew = true

		newConn, err := p.create()
		if err != nil {
			return nil, err
		}

		i := &muxItem{
			data:      newConn,
			heartbeat: time.Now(),
		}

		if p.cap.Load() < int32(p.maxCap) {
			incrMaxPos := p.maxPos + 1
			if incrMaxPos < p.maxCap {
				p.maxPos = incrMaxPos
				p.store[incrMaxPos] = i
				p.cap.Add(1)
			} else {
				for idx, it := range p.store {
					if it == nil {
						mu := p.storeMus[idx]
						mu.Lock()
						if it == nil {
							p.store[idx] = i
							p.cap.Add(1)
							if idx > p.maxPos {
								p.maxPos = idx
							}
							mu.Unlock()
							break
						}
						mu.Unlock()
					}
				}
			}
		}

		return i, nil
	})
	if err != nil {
		return nil, false, err
	}

	p.sfl.Forget("create")

	newItem := newItemAny.(*muxItem)
	newItem.refCount.Add(1)

	return newItem.data, isNew, nil
}

func (p *MuxPool) Len() int {
	return int(p.cap.Load())
}

func (p *MuxPool) Put(v interface{}) {
	p.poolMu.RLock()
	defer p.poolMu.RUnlock()

	var selected *muxItem
	for _, it := range p.store {
		if it == nil {
			continue
		}
		if it.data == v {
			selected = it
			break
		}
	}
	if selected == nil {
		return
	}
	selected.refCount.Add(-1)
	selected.heartbeat = time.Now()
	if selected.refCount.Load() <= 0 && selected.isBlocking {
		selected.isBlocking = false
	}
}

func (p *MuxPool) Block(v interface{}) {
	p.poolMu.RLock()
	defer p.poolMu.RUnlock()

	var selected *muxItem
	for _, it := range p.store {
		if it == nil {
			continue
		}
		if it.data == v {
			selected = it
			break
		}
	}
	if selected != nil && selected.refCount.Load() > 0 && !selected.isBlocking {
		selected.isBlocking = true
	}
}

func (p *MuxPool) Clear() {
	p.poolMu.Lock()
	defer p.poolMu.Unlock()

	for _, it := range p.store {
		if it == nil {
			continue
		}
		if p.Close != nil {
			p.Close(it.data)
		}
	}

	p.store = make([]*muxItem, p.maxCap)
}

func (p *MuxPool) Destroy() {
	p.poolMu.Lock()
	defer p.poolMu.Unlock()
	if p.store == nil {
		// pool already destroyed
		return
	}

	for _, it := range p.store {
		if it == nil {
			continue
		}
		if p.Close != nil {
			p.Close(it.data)
		}
	}
	p.store = nil
}

func (p *MuxPool) RegisterChecker(interval time.Duration, check func(interface{}) bool) {
	if interval > 0 && check != nil {
		go func() {
			for {
				time.Sleep(interval)
				p.poolMu.RLock()
				if p.store == nil {
					p.poolMu.RUnlock()
					return
				}
				for i, it := range p.store {
					if i > p.maxPos {
						break
					}

					if it == nil {
						continue
					}

					if p.remove(i) {
						continue
					}

					if p.Idle > 0 && time.Now().Sub(it.heartbeat) > p.Idle {
						if p.Close != nil {
							p.Close(it.data)
						}
						continue
					}

					if !check(it.data) {
						if p.Close != nil {
							p.Close(it.data)
						}
					}

					mu := p.storeMus[i]
					mu.Lock()
					if p.store[i] == nil {
						p.store[i] = it
						mu.Unlock()
						continue
					}
					mu.Unlock()

					if p.Close != nil {
						p.Close(it.data)
					}
				}
				p.poolMu.RUnlock()
			}
		}()
	}
}

func (p *MuxPool) remove(i int) bool {
	var succ bool
	mu := p.storeMus[i]
	mu.Lock()
	if p.store[i] != nil {
		p.store[i] = nil
		p.cap.Add(-1)
		succ = true
	}
	mu.Unlock()
	return succ
}
