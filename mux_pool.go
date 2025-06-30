package connpool

import (
	"fmt"
	"github.com/995933447/elemutil"
	"golang.org/x/sync/singleflight"
	"sync"
	"sync/atomic"
	"time"
)

type MuxPool struct {
	New    func() (interface{}, error)
	Ping   func(interface{}) bool
	Close  func(interface{})
	Idle   time.Duration
	store  *elemutil.LinkedList
	mu     sync.Mutex
	maxCap int
	sfl    singleflight.Group
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
	p.store = &elemutil.LinkedList{}
	p.maxCap = maxCap
	if newFunc != nil {
		p.New = newFunc
	}
	for i := 0; i < initCap; i++ {
		conn, err := p.create()
		if err != nil {
			return nil, err
		}
		p.store.Append(&muxItem{
			data:      conn,
			heartbeat: time.Now(),
		})
	}
	return p, nil
}

func (p *MuxPool) create() (interface{}, error) {
	if p.New == nil {
		return nil, fmt.Errorf("Pool.New is nil, can not create connection")
	}
	return p.New()
}

func (p *MuxPool) Get() (interface{}, bool, error) {
	var selected *muxItem

	err := p.store.Walk(func(node *elemutil.LinkedNode) (bool, error) {
		i := node.Payload.(*muxItem)

		if p.Ping != nil && !p.Ping(i) {
			p.mu.Lock()
			defer p.mu.Unlock()
			p.store.DeleteNode(node)
			return true, nil
		}

		// 空闲链接
		if i.refCount.Load() <= 0 {
			selected = i
			return false, nil
		}

		// 连接是否阻塞，如果已经阻塞（如何写入缓冲区满了），表示单连接负载高了，取其他连接
		if !i.isBlocking {
			selected = i
			return false, nil
		}

		return true, nil
	})
	if err != nil {
		return nil, false, err
	}

	if selected != nil {
		selected.refCount.Add(1)
		return selected.data, false, nil
	}

	defer p.sfl.Forget("newConn")

	var isNew bool

	newItemAny, err, _ := p.sfl.Do("newConn", func() (interface{}, error) {
		newConn, err := p.create()
		if err != nil {
			return nil, err
		}

		i := &muxItem{
			data:      newConn,
			heartbeat: time.Now(),
		}

		p.mu.Lock()
		if p.store.Len() < uint32(p.maxCap) {
			p.store.Append(i)
		}
		p.mu.Unlock()

		isNew = true

		return i, nil
	})

	newItem := newItemAny.(*muxItem)
	newItem.refCount.Add(1)

	return newItem.data, isNew, nil
}

func (p *MuxPool) Len() int {
	if p.store == nil {
		return 0
	}
	return int(p.store.Len())
}

func (p *MuxPool) Put(v interface{}) {
	var selected *muxItem
	_ = p.store.Walk(func(node *elemutil.LinkedNode) (bool, error) {
		i := node.Payload.(*muxItem)
		if i.data == v {
			selected = i
			return false, nil
		}
		return true, nil
	})
	if selected == nil {
		return
	}

	selected.refCount.Add(-1)
	selected.heartbeat = time.Now()
	if selected.refCount.Load() <= 0 && selected.isBlocking {
		selected.isBlocking = false
	}
}

func (p *MuxPool) Destroy() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.store == nil || p.store.Len() == 0 {
		// pool already destroyed
		return
	}
	_ = p.store.Walk(func(node *elemutil.LinkedNode) (bool, error) {
		i := node.Payload.(*muxItem)
		if p.Close != nil {
			p.Close(i.data)
		}
		return true, nil
	})
	p.store = nil
}

func (p *MuxPool) RegisterChecker(interval time.Duration, check func(interface{}) bool) {
	if interval > 0 && check != nil {
		go func() {
			for {
				time.Sleep(interval)
				p.mu.Lock()
				if p.store == nil {
					p.mu.Unlock()
					return
				}
				p.mu.Unlock()
				_ = p.store.Walk(func(node *elemutil.LinkedNode) (bool, error) {
					i := node.Payload.(*muxItem)
					if p.Idle > 0 && time.Now().Sub(i.heartbeat) > p.Idle {
						p.mu.Lock()
						p.store.DeleteNode(node)
						p.mu.Unlock()
						if p.Close != nil {
							p.Close(i.data)
						}
						return true, nil
					}
					if !check(i.data) {
						p.mu.Lock()
						p.store.DeleteNode(node)
						p.mu.Unlock()
						if p.Close != nil {
							p.Close(i.data)
						}
					}
					return true, nil
				})
			}
		}()
	}
}

func (p *MuxPool) Clear() {
	if p.store == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	_ = p.store.Walk(func(node *elemutil.LinkedNode) (bool, error) {
		i := node.Payload.(*muxItem)
		p.store.DeleteNode(node)
		if p.Close != nil {
			p.Close(i.data)
		}
		return true, nil
	})
}

func (p *MuxPool) Block(v interface{}) {
	var selected *muxItem
	_ = p.store.Walk(func(node *elemutil.LinkedNode) (bool, error) {
		i := node.Payload.(*muxItem)
		if i.data == v {
			selected = i
			return false, nil
		}
		return true, nil
	})
	if selected != nil && selected.refCount.Load() > 0 && !selected.isBlocking {
		selected.isBlocking = true
	}
}
