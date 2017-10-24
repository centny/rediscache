package rediscache

import (
	"strconv"
	"strings"
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
	var options []redis.DialOption
	parts := strings.SplitN(uri, "?", 2)
	if len(parts) > 1 {
		args := strings.Split(parts[1], "&")
		for _, arg := range args {
			if strings.HasPrefix(arg, "db=") {
				db, err := strconv.Atoi(strings.TrimPrefix(arg, "db="))
				if err != nil {
					panic(err)
				}
				options = append(options, redis.DialDatabase(db))
			} else if strings.HasPrefix(arg, "password=") {
				options = append(options, redis.DialPassword(strings.TrimPrefix(arg, "password=")))
			}
		}
	}
	// fmt.Println(uri, options)
	Pool = redis.NewPool(func() (conn redis.Conn, err error) {
		conn, err = redis.Dial("tcp", parts[0], options...)
		return
	}, 100)
	Pool.MaxActive = 200
	Pool.Wait = true
	C = Pool.Get
}
