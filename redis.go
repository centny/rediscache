package rediscache

import (
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/gomodule/redigo/redis"
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
var Pool *ConnPool

//InitRedisPool will initial the redis pool by uri.
func InitRedisPool(uri string) {
	Pool = NewConnPoolByURI(uri)
	// Pool.MaxActive = 200
	// Pool.Wait = true
	C = Pool.Get
}

type ConnPool struct {
	connQueue chan *poolConn
	maxQueue  chan int
	Max       int
	Newer     func() (conn redis.Conn, err error)
}

func NewConnPool(max int, newer func() (conn redis.Conn, err error)) (pool *ConnPool) {
	pool = &ConnPool{
		connQueue: make(chan *poolConn, max),
		maxQueue:  make(chan int, max),
		Max:       max,
		Newer:     newer,
	}
	for i := 0; i < max; i++ {
		pool.maxQueue <- 1
	}
	return
}

func NewConnPoolByURI(uri string) (pool *ConnPool) {
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
	pool = NewConnPool(100, func() (conn redis.Conn, err error) {
		conn, err = redis.Dial("tcp", parts[0], options...)
		return
	})
	return
}

func (c *ConnPool) Get() (conn redis.Conn) {
	for {
		var pc *poolConn
		select {
		case pc = <-c.connQueue:
			if !pc.IsGood() {
				pc = nil
			}
		case <-c.maxQueue:
			raw, err := c.Newer()
			if err != nil {
				log.Printf("[Warn]ConnPool new connection fail with %v", err)
			} else {
				pc = &poolConn{Conn: raw, pool: c}
			}
		}
		if pc != nil {
			conn = pc
			break
		}
	}
	return
}

type poolConn struct {
	redis.Conn
	pool *ConnPool
	last int64
}

// Close closes the connection.
func (p *poolConn) Close() (err error) {
	okErr := p.Conn.Err()
	if okErr != nil {
		p.Conn.Close()
		p.pool.maxQueue <- 1
	} else {
		p.pool.connQueue <- p
	}
	return
}

func (p *poolConn) IsGood() (ok bool) {
	now := time.Now().Local().UnixNano() / 1e6
	if now-p.last > 3000 {
		p.Conn.Do("ping")
		p.last = now
	}
	ok = p.Conn.Err() == nil
	return
}

func (p *poolConn) Do(commandName string, args ...interface{}) (reply interface{}, err error) {
	if err := mockerCheck("Conn.Do"); err != nil {
		return nil, err
	}
	return p.Conn.Do(commandName, args...)
}

func (p *poolConn) Send(commandName string, args ...interface{}) error {
	if err := mockerCheck("Conn.Send"); err != nil {
		return err
	}
	return p.Conn.Send(commandName, args...)
}

func (p *poolConn) Flush() error {
	if err := mockerCheck("Conn.Flush"); err != nil {
		return err
	}
	return p.Conn.Flush()
}

func (p *poolConn) Receive() (reply interface{}, err error) {
	if err := mockerCheck("Conn.Receive"); err != nil {
		return nil, err
	}
	return p.Conn.Receive()
}
