# go-reidssion


## Quickstart
```go
package main

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis"
	"github.com/lvnz555/go-redission"
)

func main() {
	rdb := redis.NewClient(&redis.Options{
		Network:     "tcp",
		Addr:        "127.0.0.1:6379",
		PoolSize:    50,
		DialTimeout: 10 * time.Second,
	})

	locker := redission.GetLocker(rdb, &redission.RedissionLockConfig{
			Key: "pro:001",
		})
	locker.Lock(c.Request.Context())
    // ---
    locker.UnLock()
}
```