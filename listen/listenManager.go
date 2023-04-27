package listen

import (
	"strings"
	"sync"

	"github.com/go-redis/redis"
)

type Listener struct {
	C chan struct{}
}

type Listens map[string]*Listener
type ListensCh struct {
	L        Listens
	key      string
	Mux      sync.Mutex
	sub      *redis.PubSub
	isListen bool
}

func NewListensCh(name string, s *redis.PubSub) *ListensCh {
	return &ListensCh{
		L:   make(Listens),
		key: name,
		sub: s,
	}
}

func (l *ListensCh) Subscribe(token string) *Listener {
	l.Mux.Lock()
	defer l.Mux.Unlock()

	if l.L[token] != nil {
		return l.L[token]
	}

	l.L[token] = &Listener{
		C: make(chan struct{}, 1),
	}

	if !l.isListen {
		l.runOnce()
	}

	return l.L[token]
}

func (l *ListensCh) UnSubscribe(token string) bool {
	l.Mux.Lock()
	defer l.Mux.Unlock()
	if l.L[token] != nil {
		close(l.L[token].C)
		delete(l.L, token)
	}

	if len(l.L) <= 0 {
		l.sub.Close()
		return false
	}
	return true
}

func (l *ListensCh) runOnce() {
	l.isListen = true
	go func() {
	LOOP:
		for {
			select {
			case msg, ok := <-l.sub.Channel():
				if !ok {
					break LOOP
				}
				if strings.EqualFold(msg.Channel, l.key) {
					l.Mux.Lock()
					for _, listen := range l.L {
						if len(listen.C) <= 0 {
							listen.C <- struct{}{}
						}
					}
					l.Mux.Unlock()
				}
			}
		}
		l.isListen = false
	}()
}

type ListenManager struct {
	mux    sync.Mutex
	store  map[string]*ListensCh
	client *redis.Client
}

func (lm *ListenManager) Subscribe(key string, token string) *Listener {
	lm.mux.Lock()
	defer lm.mux.Unlock()
	listens := lm.store[key]
	if listens == nil {
		if listens = lm.store[key]; listens == nil {
			listens = NewListensCh(key, lm.client.Subscribe(key))
			lm.store[key] = listens
		}
	}

	return listens.Subscribe(token)
}

func (lm *ListenManager) UnSubscribe(key string, token string) {
	lm.mux.Lock()
	defer lm.mux.Unlock()
	listens := lm.store[key]
	if listens == nil {
		return
	}

	if !listens.UnSubscribe(token) {
		delete(lm.store, key)
	}
}

var instance *ListenManager = nil

func GetListerManager(c *redis.Client) *ListenManager {
	if instance == nil {
		instance = &ListenManager{
			store:  make(map[string]*ListensCh),
			client: c,
		}
	}
	return instance
}
