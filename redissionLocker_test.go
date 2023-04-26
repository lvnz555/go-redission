package redission

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/go-redis/redis"
)

var rdb *redis.Client

func init() {
	rdb = redis.NewClient(&redis.Options{
		Network:     "tcp",
		Addr:        "127.0.0.1:6379",
		PoolSize:    50,
		DialTimeout: 10 * time.Second,
	})
}

func TestRedissionLocker(t *testing.T) {
	locker := GetLocker(rdb, &RedissionLockConfig{
		Key: "test",
	})

	locker.Lock(context.Background())
	t.Logf("分布式锁运用")
	time.Sleep(3 * time.Second)
	locker.UnLock()
	t.Logf("分布式锁释放")
}

func BenchmarkXxx(b *testing.B) {
	b.StartTimer()
	wg := new(sync.WaitGroup)
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func(wg *sync.WaitGroup, ii int) {
			locker := GetLocker(rdb, &RedissionLockConfig{
				Key: "test",
			})

			locker.Lock(context.Background())
			b.Logf("任务:%d\n", ii)
			locker.UnLock()
			wg.Done()
		}(wg, i)
	}

	wg.Wait()
	b.StopTimer()
}
