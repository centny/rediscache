package rediscache

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func init() {
	InitRedisPool("localhost:6379?db=1")
	for _, key := range []string{"cres", "cres1"} {
		C().Do("del", key+"-ver", key+"-size", key+"-val")
	}
}

type cacheTest struct {
	added         map[string]bool
	alck          sync.RWMutex
	cache         *Cache
	wait          bool
	adding        int64
	listing       int64
	listing2      int64
	add2c, list2c int64
}

func newCacheTest() *cacheTest {
	return &cacheTest{
		added: map[string]bool{},
		cache: NewCache(10240),
		alck:  sync.RWMutex{},
	}
}

func (c *cacheTest) doAdd1(key string) {
	err := c.cache.WillModify("cres1", func() error {
		//your code.
		c.alck.Lock()
		c.added[key] = true
		if c.wait {
			time.Sleep(10 * time.Millisecond)
		}
		c.alck.Unlock()
		return nil
	})
	if err != nil {
		panic(err)
	}
}

func (c *cacheTest) doAdd2(key string) {
	err := c.cache.WillModify("cres", func() error {
		//your code.
		c.alck.Lock()
		c.added[key] = true
		if c.wait {
			time.Sleep(10 * time.Millisecond)
		}
		c.alck.Unlock()
		return nil
	})
	if err != nil {
		panic(err)
	}
}

func (c *cacheTest) list2(u string) (xval interface{}, res map[string]bool) {
	c.cache.WillQuery("cres", &res, func() (val interface{}, err error) {
		c.alck.Lock()
		xres := map[string]bool{}
		for k, v := range c.added {
			xres[k] = v
		}
		if c.wait {
			time.Sleep(10 * time.Millisecond)
		}
		c.alck.Unlock()
		return xres, nil
	}, "cres1")
	return
}

func TestCache(t *testing.T) {
	ctest := newCacheTest()
	ctest.cache.ShowLog = true
	key := fmt.Sprintf("test-%v", 0)
	ctest.doAdd2(key)
	_, res := ctest.list2("")
	if !res[key] {
		panic("list not found")
	}
	fmt.Println("--->")
	for i := 0; i < 10; i++ {
		_, res = ctest.list2("")
		if !res[key] {
			panic("list not found")
		}
	}
	//
	ctest.cache.removeLocal("cres")
	for i := 0; i < 10; i++ {
		_, res = ctest.list2("")
		if !res[key] {
			panic("list not found")
		}
	}
	if ctest.cache.RemoteHited != 1 || ctest.cache.LocalHited != 19 {
		fmt.Println(ctest.cache.RemoteHited, ctest.cache.LocalHited)
		t.Error("error")
		return
	}
	fmt.Println("--->")
	//watch change.
	key2 := fmt.Sprintf("test-%v", 0)
	ctest.doAdd1(key2)
	_, res = ctest.list2("")
	if !res[key2] {
		panic("list not found")
	}
	if ctest.cache.RemoteHited != 1 || ctest.cache.LocalHited != 19 {
		fmt.Println(ctest.cache.RemoteHited, ctest.cache.LocalHited)
		t.Error("error")
		return
	}
	fmt.Println(ctest.cache.size)
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
				ctest.doAdd2(key)
				_, res := ctest.list2(key)
				if !res[key] {
					panic("list not found")
				}
			} else {
				ctest.list2(key)
			}
		}
	})
}

func BenchmarkEnable(b *testing.B) {
	ctest := newCacheTest()
	// ctest.cache.ShowLog = true
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
		// fmt.Println(" added->", len(ctest.added), ctest.ver)
		fmt.Println("ver->", ctest.cache.mcache["res"].Value.(*Item).Ver)
		fmt.Printf("cache hited->local:%v,remote:%v\n", ctest.cache.LocalHited, ctest.cache.RemoteHited)
		fmt.Printf("<--- all done --->\n\n\n")
	}
	// var running int64
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
			// fmt.Println("start->", idx, atomic.AddInt64(&running, 1),
			// 	ctest.adding, ctest.listing, ctest.listing2, ctest.list2c, ctest.add2c)
			// fmt.Println("running->", idx)
			key := fmt.Sprintf("x-%v", idx)
			if idx%10 == 0 {
				ctest.doAdd2(key)
				xval, res := ctest.list2(key)
				if !res[key] {
					panic(fmt.Sprintf("list not found by key %v ->%v-->%v", key, len(res), xval))
				}
			} else {
				ctest.list2(key)
			}
			// atomic.AddInt64(&running, -1)
			// fmt.Println("done->", idx, atomic.AddInt64(&running, -1), ctest.adding, ctest.listing)
		}
	})
	state, _ := ctest.cache.State()
	fmt.Println("don with size:", state)
}
