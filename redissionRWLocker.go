package redission

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis"
	uuid "github.com/google/uuid"
	"github.com/lvnz555/go-redission/internal"
	"github.com/lvnz555/go-redission/listen"
)

var rlockScript string = strings.Join([]string{
	"local mode = redis.call('hget', KEYS[1], 'mode'); ",
	"if (mode == false) then ",
	"redis.call('hset', KEYS[1], 'mode', 'read'); ",
	"redis.call('hset', KEYS[1], ARGV[2], 1); ",
	"redis.call('set', KEYS[2] .. ':1', 1); ",
	"redis.call('pexpire', KEYS[2] .. ':1', ARGV[1]); ",
	"redis.call('pexpire', KEYS[1], ARGV[1]); ",
	"return nil; ",
	"end; ",
	"if (mode == 'read') or (mode == 'write' and redis.call('hexists', KEYS[1], ARGV[3]) == 1) then ",
	"local ind = redis.call('hincrby', KEYS[1], ARGV[2], 1); ",
	"local key = KEYS[2] .. ':' .. ind;",
	"redis.call('set', key, 1); ",
	"redis.call('pexpire', key, ARGV[1]); ",
	"redis.call('pexpire', KEYS[1], ARGV[1]); ",
	"return nil; ",
	"end;",
	"return redis.call('pttl', KEYS[1]);",
}, "")

var runlockScript string = strings.Join([]string{
	"local mode = redis.call('hget', KEYS[1], 'mode'); ",
	"if (mode == false) then ",
	"redis.call('publish', KEYS[2], ARGV[1]); ",
	"return 1; ",
	"end; ",
	"local lockExists = redis.call('hexists', KEYS[1], ARGV[2]); ",
	"if (lockExists == 0) then ",
	"return nil;",
	"end; ",

	"local counter = redis.call('hincrby', KEYS[1], ARGV[2], -1); ",
	"if (counter == 0) then ",
	"redis.call('hdel', KEYS[1], ARGV[2]); ",
	"end;",
	"redis.call('del', KEYS[3] .. ':' .. (counter+1)); ",
	"if (redis.call('hlen', KEYS[1]) > 1) then ",
	"local maxRemainTime = -3; ",
	"local keys = redis.call('hkeys', KEYS[1]); ",
	"for n, key in ipairs(keys) do ",
	"counter = tonumber(redis.call('hget', KEYS[1], key)); ",
	"if type(counter) == 'number' then ",
	"for i=counter, 1, -1 do ",
	"local remainTime = redis.call('pttl', KEYS[4] .. ':' .. key .. ':rwlock_timeout:' .. i); ",
	"maxRemainTime = math.max(remainTime, maxRemainTime);",
	"end; ",
	"end; ",
	"end; ",

	"if maxRemainTime > 0 then ",
	"redis.call('pexpire', KEYS[1], maxRemainTime); ",
	"return 0; ",
	"end;",

	"if mode == 'write' then ",
	"return 0;",
	"end; ",
	"end; ",

	"redis.call('del', KEYS[1]); ",
	"redis.call('publish', KEYS[2], ARGV[1]); ",
	"return 1; ",
}, "")

var wlockScript string = strings.Join([]string{
	"local mode = redis.call('hget', KEYS[1], 'mode'); ",
	"if (mode == false) then ",
	"redis.call('hset', KEYS[1], 'mode', 'write'); ",
	"redis.call('hset', KEYS[1], ARGV[2], 1); ",
	"redis.call('pexpire', KEYS[1], ARGV[1]); ",
	"return nil; ",
	"end; ",
	"if (mode == 'write') then ",
	"if (redis.call('hexists', KEYS[1], ARGV[2]) == 1) then ",
	"redis.call('hincrby', KEYS[1], ARGV[2], 1); ",
	"local currentExpire = redis.call('pttl', KEYS[1]); ",
	"redis.call('pexpire', KEYS[1], currentExpire + ARGV[1]); ",
	"return nil; ",
	"end; ",
	"end;",
	"return redis.call('pttl', KEYS[1]);",
}, "")

var wunlockScript string = strings.Join([]string{
	"local mode = redis.call('hget', KEYS[1], 'mode'); ",
	"if (mode == false) then ",
	"redis.call('publish', KEYS[2], ARGV[1]); ",
	"return 1; ",
	"end;",
	"if (mode == 'write') then ",
	"local lockExists = redis.call('hexists', KEYS[1], ARGV[3]); ",
	"if (lockExists == 0) then ",
	"return nil;",
	"else ",
	"local counter = redis.call('hincrby', KEYS[1], ARGV[3], -1); ",
	"if (counter > 0) then ",
	"redis.call('pexpire', KEYS[1], ARGV[2]); ",
	"return 0; ",
	"else ",
	"redis.call('hdel', KEYS[1], ARGV[3]); ",
	"if (redis.call('hlen', KEYS[1]) == 1) then ",
	"redis.call('del', KEYS[1]); ",
	"redis.call('publish', KEYS[2], ARGV[1]); ",
	"else ",
	// has unlocked read-locks
	"redis.call('hset', KEYS[1], 'mode', 'read'); ",
	"end; ",
	"return 1; ",
	"end; ",
	"end; ",
	"end; ",
	"return nil;",
}, "")

type redissionRWLocker struct {
	redissionLocker
	rwTimeoutTokenPrefix string
}

func (rl *redissionRWLocker) RLock(ctx context.Context, timeout ...time.Duration) {
	if rl.exit == nil {
		rl.exit = make(chan struct{})
	}
	ttl, err := rl.tryRLock(rl.key)
	if err != nil {
		panic(err)
	}

	if ttl <= 0 {
		rl.once.Do(func() {
			go rl.refreshLockTimeout(ctx, rl.key)
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
		ttl, err = rl.tryLock(rl.key)
		if err != nil {
			panic(err)
		}

		if ttl <= 0 {
			rl.once.Do(func() {
				go rl.refreshLockTimeout(ctx, rl.key)
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

func (rl *redissionRWLocker) Lock(ctx context.Context, timeout ...time.Duration) {
	if rl.exit == nil {
		rl.exit = make(chan struct{})
	}
	ttl, err := rl.tryLock(rl.key)
	if err != nil {
		panic(err)
	}

	if ttl <= 0 {
		rl.once.Do(func() {
			go rl.refreshLockTimeout(ctx, rl.key)
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
		ttl, err = rl.tryLock(rl.key)
		if err != nil {
			panic(err)
		}

		if ttl <= 0 {
			rl.once.Do(func() {
				go rl.refreshLockTimeout(ctx, rl.key)
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

func (rl *redissionRWLocker) tryRLock(key string) (time.Duration, error) {
	writeLockToken := strings.Join([]string{rl.token, "write"}, ":")
	res := rl.client.Eval(rlockScript, []string{key, rl.rwTimeoutTokenPrefix}, rl.lockLeaseTime, rl.token, writeLockToken)
	v, err := res.Result()
	if err != redis.Nil && err != nil {
		return 0, err
	}

	if v == nil {
		return 0, nil
	}

	return time.Duration(v.(int64)), nil
}

func (rl *redissionRWLocker) tryLock(key string) (time.Duration, error) {
	res := rl.client.Eval(wlockScript, []string{key}, rl.lockLeaseTime, rl.token)
	v, err := res.Result()
	if err != redis.Nil && err != nil {
		return 0, err
	}

	if v == nil {
		return 0, nil
	}

	return time.Duration(v.(int64)), nil
}

func (rl *redissionRWLocker) RUnLock() {
	res := rl.client.Eval(runlockScript, []string{rl.key, rl.chankey, rl.rwTimeoutTokenPrefix, "{" + rl.key + "}"}, unlockMessage, rl.token)
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

func (rl *redissionRWLocker) UnLock() {
	res := rl.client.Eval(wunlockScript, []string{rl.key, rl.chankey}, unlockMessage, rl.lockLeaseTime, rl.token)
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

func GetRWLocker(client *redis.Client, ops *RedissionLockConfig) *redissionRWLocker {
	r := &redissionLocker{
		token:         uuid.New().String(),
		client:        client,
		exit:          make(chan struct{}),
		listenManager: listen.GetListerManager(client),
		once:          &sync.Once{},
	}

	if len(ops.Prefix) <= 0 {
		ops.Prefix = "redission-rwlock"
	}
	if len(ops.ChanPrefix) <= 0 {
		ops.ChanPrefix = "redission-rwlock-channel"
	}
	if ops.LockLeaseTime == 0 {
		r.lockLeaseTime = internalLockLeaseTime
	}
	r.key = strings.Join([]string{ops.Prefix, ops.Key}, ":")
	r.chankey = strings.Join([]string{ops.ChanPrefix, ops.Key}, ":")
	tkey := "{" + r.key + "}"
	return &redissionRWLocker{redissionLocker: *r, rwTimeoutTokenPrefix: strings.Join([]string{tkey, r.token, "rwlock_timeout"}, ":")}
}
