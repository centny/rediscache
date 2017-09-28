package rediscache

import (
	"time"

	"github.com/garyburd/redigo/redis"
)

//C is the redis connection getter
var C = func() redis.Conn {
	panic("the redis connection is not impl")
}

//Now will return current timestamp.
func Now() int64 {
	return time.Now().Local().UnixNano() / 1e6
}

//Pool is the redis pool.
var Pool *redis.Pool

//InitRedisPool will initial the redis pool by uri.
func InitRedisPool(uri string) {
	Pool = redis.NewPool(func() (conn redis.Conn, err error) {
		conn, err = redis.Dial("tcp", uri)
		return
	}, 100)
	Pool.MaxActive = 200
	Pool.Wait = true
	C = Pool.Get
}
