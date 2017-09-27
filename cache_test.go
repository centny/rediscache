package rediscache

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/garyburd/redigo/redis"
)

func init() {
	pool := redis.NewPool(func() (conn redis.Conn, err error) {
		conn, err = redis.Dial("tcp", "loc.m:6379")
		return
	}, 100)
	pool.MaxActive = 200
	pool.Wait = true
	C = pool.Get
	C().Do("del", "res-ver", "res-size", "res-val")
}

type cacheTest struct {
	added map[string]bool
	alck  sync.RWMutex
	cache *Cache
	ver   int64
	wait  bool
}

func newCacheTest() *cacheTest {
	return &cacheTest{
		added: map[string]bool{},
		cache: NewCache(10240),
		ver:   10000,
		alck:  sync.RWMutex{},
	}
}

func (c *cacheTest) doAdd(key string) {
	//
	c.alck.Lock()
	c.added[key] = true
	c.ver++
	ver := c.ver
	if c.wait {
		time.Sleep(10 * time.Millisecond)
	}
	c.alck.Unlock()
	//
	// fmt.Printf("start expired->key:%v,ver:%v\n", key, ver)
	err := c.cache.Expire("res", ver)
	if err != nil {
		panic(err)
	}
	// fmt.Printf("%v:expired->ver:%v\n", key, ver)
}

func (c *cacheTest) list(u string) (xval interface{}, res map[string]bool) {
	err := c.cache.Try("res", &res)
	// fmt.Printf("cache->%v\n", err)
	if err == nil {
		return
	}
	if err != NoFound {
		panic(err)
	}
	//
	c.alck.Lock()
	res = map[string]bool{}
	for k, v := range c.added {
		res[k] = v
	}
	ver := c.ver
	if c.wait {
		time.Sleep(10 * time.Millisecond)
	}
	c.alck.Unlock()
	//
	// fmt.Printf("start update to->%v\n", ver)
	err = c.cache.Update("res", ver, res)
	if err != nil {
		panic(err)
	}
	// fmt.Printf("updated to->%v\n", ver)
	return
}

func TestCache(t *testing.T) {
	ctest := newCacheTest()
	ctest.cache.ShowLog = true
	key := fmt.Sprintf("test-%v", 0)
	ctest.doAdd(key)
	_, res := ctest.list("")
	if !res[key] {
		panic("list not found")
	}
	for i := 0; i < 10; i++ {
		_, res = ctest.list("")
		if !res[key] {
			panic("list not found")
		}
	}
	//
	ctest.cache.removeLocal("res")
	for i := 0; i < 10; i++ {
		_, res = ctest.list("")
		if !res[key] {
			panic("list not found")
		}
	}
	if ctest.cache.RemoteHited != 1 || ctest.cache.LocalHited != 19 {
		fmt.Println(ctest.cache.RemoteHited, ctest.cache.LocalHited)
		t.Error("error")
		return
	}
}

func BenchmarkDisable(b *testing.B) {
	ctest := newCacheTest()
	ctest.cache.Disable = true
	ctest.wait = true
	var sequence uint64
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			idx := atomic.AddUint64(&sequence, 1)
			key := fmt.Sprintf("x-%v", idx)
			if idx%10 == 0 {
				ctest.doAdd(key)
				_, res := ctest.list(key)
				if !res[key] {
					panic("list not found")
				}
			} else {
				ctest.list(key)
			}
		}
	})
}

func BenchmarkEnable(b *testing.B) {
	ctest := newCacheTest()
	ctest.wait = true
	// ctest.cache.Disable = true
	var sequence uint64
	var done func()
	runned := 0
	done = func() {
		if runned > 0 {
			return
		}
		runned = 1
		fmt.Println()
		fmt.Println("sequence->", sequence)
		// fmt.Println("cached->", util.S2Json(ctest.list()))
		fmt.Println(" added->", len(ctest.added), ctest.ver)
		fmt.Println("ver->", ctest.cache.mcache["res"].Value.(*Item).Ver)
		fmt.Printf("cache hited->local:%v,remote:%v\n", ctest.cache.LocalHited, ctest.cache.RemoteHited)
		fmt.Printf("<--- all done --->\n\n\n")
	}
	b.RunParallel(func(pb *testing.PB) {
		defer func() {
			err := recover()
			if err != nil {
				done()
				panic(err)
			}
		}()
		for pb.Next() {
			idx := atomic.AddUint64(&sequence, 1)
			// fmt.Println("running->", idx)
			key := fmt.Sprintf("x-%v", idx)
			if idx%10 == 0 {
				ctest.doAdd(key)
				xval, res := ctest.list(key)
				if !res[key] {
					panic(fmt.Sprintf("list not found by key %v ->%v-->%v", key, len(res), xval))
				}
			} else {
				ctest.list(key)
			}
			// fmt.Println("done->", idx)
		}
	})
}
