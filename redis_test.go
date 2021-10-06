package rediscache

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
)

func TestRedis(t *testing.T) {
	pool := NewConnPoolByURI("redis.loc:6379")
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
