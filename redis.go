package rediscache

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"log"
	"net/url"
	"strconv"
	"strings"
	"sync"
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
func InitRedisPool(uri string) (err error) {
	Pool, err = NewConnPoolByURI(uri)
	// Pool.MaxActive = 200
	// Pool.Wait = true
	C = Pool.Get
	return
}

type ConnPool struct {
	connQueue chan *poolConn
	maxQueue  chan int
	connAll   map[string]*poolConn
	connLock  sync.RWMutex
	Max       int
	Newer     func() (conn redis.Conn, err error)
}

func NewConnPool(max int, newer func() (conn redis.Conn, err error)) (pool *ConnPool) {
	pool = &ConnPool{
		connQueue: make(chan *poolConn, max),
		maxQueue:  make(chan int, max),
		connAll:   map[string]*poolConn{},
		connLock:  sync.RWMutex{},
		Max:       max,
		Newer:     newer,
	}
	for i := 0; i < max; i++ {
		pool.maxQueue <- 1
	}
	return
}

func NewConnPoolByURI(uri string) (pool *ConnPool, err error) {
	if !strings.HasPrefix(uri, "redis://") {
		uri = "redis://" + uri
	}
	poolURI, err := url.Parse(uri)
	if err != nil {
		return
	}
	poolQuery := poolURI.Query()
	var options []redis.DialOption
	for key := range poolURI.Query() {
		switch key {
		case "username":
			options = append(options, redis.DialUsername(poolQuery.Get(key)))
		case "password":
			options = append(options, redis.DialPassword(poolQuery.Get(key)))
		case "db", "database":
			database, xerr := strconv.ParseInt(poolQuery.Get(key), 10, 32)
			if xerr != nil {
				err = xerr
				return
			}
			options = append(options, redis.DialDatabase(int(database)))
		case "keepAlive":
			keepAlive, xerr := strconv.ParseInt(poolQuery.Get(key), 10, 32)
			if xerr != nil {
				err = xerr
				return
			}
			options = append(options, redis.DialKeepAlive(time.Duration(keepAlive)*time.Millisecond))
		case "readTimeout":
			readTimeout, xerr := strconv.ParseInt(poolQuery.Get(key), 10, 32)
			if xerr != nil {
				err = xerr
				return
			}
			options = append(options, redis.DialReadTimeout(time.Duration(readTimeout)*time.Millisecond))
		case "writeTimeout":
			writeTimeout, xerr := strconv.ParseInt(poolQuery.Get(key), 10, 32)
			if xerr != nil {
				err = xerr
				return
			}
			options = append(options, redis.DialWriteTimeout(time.Duration(writeTimeout)*time.Millisecond))
		case "tlsUse":
			if poolQuery.Get(key) != "1" {
				break
			}
			tlsCert := poolQuery.Get("tlsCert")
			tlsKey := poolQuery.Get("tlsKey")
			tlsCA := poolQuery.Get("tlsCA")
			tlsVerify := poolQuery.Get("tlsVerify") != "0"
			// Load client cert
			cert, xerr := tls.LoadX509KeyPair(tlsCert, tlsKey)
			if xerr != nil {
				err = xerr
				break
			}
			// Load CA cert
			ca, xerr := ioutil.ReadFile(tlsCA)
			if xerr != nil {
				err = xerr
				break
			}
			certPool := x509.NewCertPool()
			certPool.AppendCertsFromPEM(ca)
			// tls config
			tlsConf := &tls.Config{
				Certificates:       []tls.Certificate{cert},
				RootCAs:            certPool,
				InsecureSkipVerify: !tlsVerify,
			}
			options = append(options, redis.DialUseTLS(true), redis.DialTLSConfig(tlsConf))
		}
	}
	pool = NewConnPool(100, func() (conn redis.Conn, err error) {
		conn, err = redis.Dial("tcp", poolURI.Host, options...)
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
				c.connLock.Lock()
				c.connAll[fmt.Sprintf("%p", pc)] = pc
				c.connLock.Unlock()
			}
		}
		if pc != nil {
			conn = pc
			break
		}
	}
	return
}

func (c *ConnPool) Close() {
	conns := []*poolConn{}
	c.connLock.Lock()
	for _, conn := range c.connAll {
		conns = append(conns, conn)
	}
	c.connLock.Unlock()
	for _, conn := range conns {
		conn.close(true)
	}
}

type poolConn struct {
	redis.Conn
	pool   *ConnPool
	last   int64
	closed int
}

// Close closes the connection.
func (p *poolConn) Close() (err error) {
	err = p.close(false)
	return
}

func (p *poolConn) close(force bool) (err error) {
	okErr := p.Conn.Err()
	if okErr != nil || force {
		p.Conn.Close()
		p.pool.connLock.Lock()
		delete(p.pool.connAll, fmt.Sprintf("%p", p))
		closed := p.closed
		if closed < 1 {
			p.closed = 1
		}
		p.pool.connLock.Unlock()
		if closed < 1 {
			p.pool.maxQueue <- 1
		}
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
