package runtime

import (
	"fmt"
	"sync"
	"time"
)

type Pool struct {
	New   func() (interface{}, error)
	Ping  func(interface{}) bool
	Close func(interface{})
	Idle  time.Duration
	store chan *item
	mu    sync.Mutex
}

type item struct {
	data      interface{}
	heartbeat time.Time
}

func NewPool(initCap, maxCap int, newFunc func() (interface{}, error)) (*Pool, error) {
	if maxCap == 0 || initCap > maxCap {
		return nil, fmt.Errorf("invalid capacity settings")
	}
	p := new(Pool)
	p.store = make(chan *item, maxCap)
	if newFunc != nil {
		p.New = newFunc
	}
	for i := 0; i < initCap; i++ {
		v, err := p.create()
		if err != nil {
			return p, err
		}
		p.store <- &item{data: v, heartbeat: time.Now()}
	}
	return p, nil
}

func (p *Pool) Len() int {
	return len(p.store)
}

func (p *Pool) RegisterChecker(interval time.Duration, check func(interface{}) bool) {
	if interval > 0 && check != nil {
		go func() error {
			for {
				time.Sleep(interval)
				p.mu.Lock()
				if p.store == nil {
					p.mu.Unlock()
					return nil
				}
				l := p.Len()
				p.mu.Unlock()
				for idx := 0; idx < l; idx++ {
					select {
					case i := <-p.store:
						v := i.data
						if p.Idle > 0 && time.Now().Sub(i.heartbeat) > p.Idle {
							if p.Close != nil {
								p.Close(v)
							}
							continue
						}
						if !check(v) {
							if p.Close != nil {
								p.Close(v)
							}
							continue
						} else {
							select {
							case p.store <- i:
								continue
							default:
								if p.Close != nil {
									p.Close(v)
								}
							}
						}
					default:
						break
					}
				}
			}
		}()
	}
}

func (p *Pool) Get() (interface{}, bool, error) {
	for {
		select {
		case i := <-p.store:
			v := i.data
			if p.Idle > 0 && time.Now().Sub(i.heartbeat) > p.Idle {
				if p.Close != nil {
					p.Close(v)
				}
				continue
			}
			if p.Ping != nil && !p.Ping(v) {
				continue
			}
			return v, true, nil
		default:
			x, y := p.create()
			return x, false, y
		}
	}
}

func (p *Pool) Put(v interface{}) {
	select {
	case p.store <- &item{data: v, heartbeat: time.Now()}:
		return
	default:
		if p.Close != nil {
			p.Close(v)
		}
		return
	}
}

func (p *Pool) Destroy() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.store == nil {
		// pool already destroyed
		return
	}
	close(p.store)
	for i := range p.store {
		if p.Close != nil {
			p.Close(i.data)
		}
	}
	p.store = nil
}

func (p *Pool) create() (interface{}, error) {
	if p.New == nil {
		return nil, fmt.Errorf("Pool.New is nil, can not create connection")
	}
	return p.New()
}

func (p *Pool) Clear() {
	var e = false
	for !e {
		select {
		case i := <-p.store:
			if p.Close != nil {
				p.Close(i.data)
			}
		default:
			e = true
		}
	}
}
