package rediscache

import (
	"time"

	"github.com/garyburd/redigo/redis"
)

var C = func() redis.Conn {
	panic("the redis connection is not impl")
}

func Now() int64 {
	return time.Now().Local().UnixNano() / 1e6
}
