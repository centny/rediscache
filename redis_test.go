package rediscache

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
)

func TestNewConnPoolByURI(t *testing.T) {
	_, err := NewConnPoolByURI("redis.loc:6379?tlsUse=1&tlsCA=ca.crt&tlsCert=client.crt&tlsKey=client.key&tlsVerify=0&keepAlive=3000&readTimeout=5000&writeTimeout=5000&username=abc&password=123&database=1")
	if err != nil {
		t.Error(err)
		return
	}
	_, err = NewConnPoolByURI("redis.loc:6379?tlsUse=0")
	if err != nil {
		t.Error(err)
		return
	}
	_, err = NewConnPoolByURI("%x%x")
	if err == nil {
		t.Error(err)
		return
	}
	_, err = NewConnPoolByURI("redis.loc:6379?database=xx")
	if err == nil {
		t.Error(err)
		return
	}
	_, err = NewConnPoolByURI("redis.loc:6379?keepAlive=xx")
	if err == nil {
		t.Error(err)
		return
	}
	_, err = NewConnPoolByURI("redis.loc:6379?readTimeout=xx")
	if err == nil {
		t.Error(err)
		return
	}
	_, err = NewConnPoolByURI("redis.loc:6379?writeTimeout=xx")
	if err == nil {
		t.Error(err)
		return
	}
	_, err = NewConnPoolByURI("redis.loc:6379?tlsUse=1&tlsCA=ca.crt&tlsCert=clientx.crt&tlsKey=clientx.key")
	if err == nil {
		t.Error(err)
		return
	}
	_, err = NewConnPoolByURI("redis.loc:6379?tlsUse=1&tlsCA=cax.crt&tlsCert=client.crt&tlsKey=client.key")
	if err == nil {
		t.Error(err)
		return
	}
}

func TestRedis(t *testing.T) {
	pool, err := NewConnPoolByURI("redis.loc:6379")
	if err != nil {
		t.Error(err)
		return
	}
	testPool(t, pool)
}

func TestRedisTLS(t *testing.T) {
	pool, err := NewConnPoolByURI("redis.loc:6379?tlsUse=1&tlsCA=ca.crt&tlsCert=client.crt&tlsKey=client.key&tlsVerify=0&keepAlive=3000&readTimeout=5000&writeTimeout=5000")
	if err != nil {
		t.Error(err)
		return
	}
	testPool(t, pool)
}

func testPool(t *testing.T, pool *ConnPool) {
	// conn, _ := redis.Dial("tcp", "redis.loc:6379")
	conn := pool.Get()
	//
	psc := redis.PubSubConn{Conn: conn}
	err := psc.Subscribe("abc")
	if err != nil {
		t.Error(err)
		return
	}
	waiter := sync.WaitGroup{}
	waiter.Add(1)
	go func() {
		running := true
		for running {
			switch v := psc.Receive().(type) {
			case redis.Message:
				fmt.Println("Message")
			case redis.Subscription:
				fmt.Println("Subscription")
			case error:
				fmt.Printf("%v\n", v)
				running = false
			}
		}
		waiter.Done()
	}()
	time.Sleep(time.Second)
	pool.Close()
	conn.Close()
	waiter.Wait()
}
