package redission

import (
	"context"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis"
	uuid "github.com/google/uuid"
	"github.com/lvnz555/go-redission/internal"
	"github.com/lvnz555/go-redission/listen"
)

func init() {
	SetLogger(log.New(os.Stderr, "go-redision: ", log.LstdFlags|log.Lshortfile))
}

func SetLogger(logger *log.Logger) {
	internal.Logger = logger
}

var lockScript string = strings.Join([]string{
	"if (redis.call('exists', KEYS[1]) == 0) then ",
	"redis.call('hset', KEYS[1], ARGV[2], 1); ",
	"redis.call('pexpire', KEYS[1], ARGV[1]); ",
	"return nil; ",
	"end; ",
	"if (redis.call('hexists', KEYS[1], ARGV[2]) == 1) then ",
	"redis.call('hincrby', KEYS[1], ARGV[2], 1); ",
	"redis.call('pexpire', KEYS[1], ARGV[1]); ",
	"return nil; ",
	"end; ",
	"return redis.call('pttl', KEYS[1]);",
}, "")

var refreshLockScript string = strings.Join([]string{
	"if (redis.call('hexists', KEYS[1], ARGV[2]) == 1) then ",
	"redis.call('pexpire', KEYS[1], ARGV[1]); ",
	"return 1; ",
	"end; ",
	"return 0;",
}, "")

var unlockScript string = strings.Join([]string{
	"if (redis.call('exists', KEYS[1]) == 0) then ",
	"redis.call('publish', KEYS[2], ARGV[1]); ",
	"return 1; ",
	"end;",
	"if (redis.call('hexists', KEYS[1], ARGV[3]) == 0) then ",
	"return nil;",
	"end; ",
	"local counter = redis.call('hincrby', KEYS[1], ARGV[3], -1); ",
	"if (counter > 0) then ",
	"redis.call('pexpire', KEYS[1], ARGV[2]); ",
	"return 0; ",
	"else ",
	"redis.call('del', KEYS[1]); ",
	"redis.call('publish', KEYS[2], ARGV[1]); ",
	"return 1; ",
	"end; ",
	"return nil;",
}, "")

const internalLockLeaseTime = uint64(30) * 1000
const unlockMessage = 0

type RedissionLockConfig struct {
	LockLeaseTime time.Duration
	Prefix        string
	ChanPrefix    string
	Key           string
}

type redissionLocker struct {
	token         string
	key           string
	chankey       string
	exit          chan struct{}
	lockLeaseTime uint64
	client        *redis.Client
	listenManager *listen.ListenManager
	once          *sync.Once
}

func (rl *redissionLocker) Lock(ctx context.Context, timeout ...time.Duration) {
	if rl.exit == nil {
		rl.exit = make(chan struct{})
	}
	ttl, err := rl.tryLock()
	if err != nil {
		panic(err)
	}

	if ttl <= 0 {
		rl.once.Do(func() {
			go rl.refreshLockTimeout()
		})
		return
	}

	submsg := make(chan struct{}, 1)
	defer close(submsg)
	sub := rl.client.Subscribe(rl.chankey)
	defer sub.Close()
	go rl.subscribeLock(sub, submsg)
	// listen := rl.listenManager.Subscribe(rl.key, rl.token)
	// defer rl.listenManager.UnSubscribe(rl.key, rl.token)

	timer := time.NewTimer(ttl)
	defer timer.Stop()
	var outimer *time.Timer
	if len(timeout) > 0 && timeout[0] > 0 {
		outimer = time.NewTimer(timeout[0])
	}
LOOP:
	for {
		ttl, err = rl.tryLock()
		if err != nil {
			panic(err)
		}

		if ttl <= 0 {
			rl.once.Do(func() {
				go rl.refreshLockTimeout()
			})
			return
		}
		if outimer != nil {
			select {
			case _, ok := <-submsg:
				if !timer.Stop() {
					<-timer.C
				}

				if !ok {
					panic("lock listen release")
				}

				timer.Reset(ttl)
			case <-ctx.Done():
				// break LOOP
				panic("lock context already release")
			case <-timer.C:
				timer.Reset(ttl)
			case <-outimer.C:
				if !timer.Stop() {
					<-timer.C
				}
				break LOOP
			}
		} else {
			select {
			case _, ok := <-submsg:
				if !timer.Stop() {
					<-timer.C
				}

				if !ok {
					panic("lock listen release")
				}

				timer.Reset(ttl)
			case <-ctx.Done():
				// break LOOP
				panic("lock context already release")
			case <-timer.C:
				timer.Reset(ttl)
			}
		}
	}
}

func (rl *redissionLocker) subscribeLock(sub *redis.PubSub, out chan struct{}) {
	defer func() {
		if err := recover(); err != nil {
			internal.Errorf("subscribeLock catch err: %v\n", err)
		}
	}()
	if sub == nil || out == nil {
		return
	}
	internal.Debugf("lock:%s enter sub routine\n", rl.token)
LOOP:
	for {
		msg, err := sub.Receive()
		if err != nil {
			internal.Infof("sub receive message %v\n", err)
			break LOOP
		}

		select {
		case <-rl.exit:
			break LOOP
		default:
			if len(out) > 0 {
				// if channel hava msg. drop it
				internal.Debugf("drop message when channel if full\n")
				continue
			}

			switch msg.(type) {
			case *redis.Subscription:
				// Ignore.
			case *redis.Pong:
				// Ignore.
			case *redis.Message:
				out <- struct{}{}
			default:
			}
		}
	}
	internal.Debugf("lock:%s sub routine release\n", rl.token)
}

func (rl *redissionLocker) refreshLockTimeout() {
	internal.Debugf("lock: %s lock %s\n", rl.token, rl.key)
	lockTime := time.Duration(rl.lockLeaseTime/3) * time.Millisecond
	timer := time.NewTimer(lockTime)
	defer timer.Stop()
LOOP:
	for {
		select {
		case <-timer.C:
			timer.Reset(lockTime)
			// update key expire time
			res := rl.client.Eval(refreshLockScript, []string{rl.key}, rl.lockLeaseTime, rl.token)
			val, err := res.Int()
			if err != nil {
				panic(err)
			}
			if val == 0 {
				internal.Debugf("not find the lock key of self\n")
				break LOOP
			}
		case <-rl.exit:
			break LOOP

		}
	}
	internal.Debugf("lock: %s refresh routine release\n", rl.token)
}

func (rl *redissionLocker) cancelRefreshLockTime() {
	if rl.exit != nil {
		close(rl.exit)
		rl.exit = nil
		rl.once = &sync.Once{}
	}
}

func (rl *redissionLocker) tryLock() (time.Duration, error) {
	res := rl.client.Eval(lockScript, []string{rl.key}, rl.lockLeaseTime, rl.token)
	v, err := res.Result()
	if err != redis.Nil && err != nil {
		return 0, err
	}

	if v == nil {
		return 0, nil
	}

	return time.Duration(v.(int64)), nil
}

func (rl *redissionLocker) UnLock() {
	res := rl.client.Eval(unlockScript, []string{rl.key, rl.chankey}, unlockMessage, rl.lockLeaseTime, rl.token)
	val, err := res.Result()
	if err != redis.Nil && err != nil {
		panic(err)
	}
	if val == nil {
		panic("attempt to unlock lock, not locked by current routine by lock id:" + rl.token)
	}
	internal.Debugf("lock: %s unlock %s\n", rl.token, rl.key)
	if val.(int64) == 1 {
		rl.cancelRefreshLockTime()
	}
}

func GetLocker(client *redis.Client, ops *RedissionLockConfig) *redissionLocker {
	r := &redissionLocker{
		token:         uuid.New().String(),
		client:        client,
		exit:          make(chan struct{}),
		listenManager: listen.GetListerManager(client),
		once:          &sync.Once{},
	}

	if len(ops.Prefix) <= 0 {
		ops.Prefix = "redission-lock"
	}
	if len(ops.ChanPrefix) <= 0 {
		ops.ChanPrefix = "redission-lock-channel"
	}
	if ops.LockLeaseTime == 0 {
		r.lockLeaseTime = internalLockLeaseTime
	}
	r.key = strings.Join([]string{ops.Prefix, ops.Key}, ":")
	r.chankey = strings.Join([]string{ops.ChanPrefix, ops.Key}, ":")
	return r
}
