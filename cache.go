package rediscache

import (
	"container/list"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/Centny/gwf/log"
	"github.com/garyburd/redigo/redis"
)

var NoFound = fmt.Errorf("cache not found")

type Item struct {
	Key  string
	Ver  int64
	Data []byte
	Last int64
}

func (i *Item) Size() uint64 {
	return uint64(len(i.Data)+len(i.Key)) + 64
}

func (i *Item) Unmarshal(v interface{}) error {
	return json.Unmarshal(i.Data, v)
}

type Cache struct {
	MemLimit    uint64
	Disable     bool
	size        uint64
	cache       *list.List
	mcache      map[string]*list.Element
	cacheLck    sync.RWMutex
	LocalHited  uint64
	RemoteHited uint64
	ShowLog     bool
}

func NewCache(memLimit uint64) *Cache {
	return &Cache{
		cache:  list.New(),
		mcache: map[string]*list.Element{},
	}
}

func (c *Cache) Update(key string, ver int64, val interface{}) (err error) {
	if c.Disable {
		return
	}
	data, err := json.Marshal(val)
	if err != nil {
		log.E("Cache-Update marshal fail with %v", err)
		return
	}
	if len(data) < 1 {
		panic("empty data")
	}
	err = c.remoteUpdate(key, ver, data)
	if err == nil {
		c.addLocal(key, ver, data)
	}
	return
}

func (c *Cache) Expire(key string, ver int64) (err error) {
	if c.Disable {
		return
	}
	c.removeLocal(key)
	err = c.remoteUpdate(key, ver, []byte(""))
	return
}

func (c *Cache) remoteUpdate(key string, ver int64, data []byte) (err error) {
	conn := C()
	defer conn.Close()
	res, err := conn.Do("eval",
		`local oldVer=redis.call('get',KEYS[1]);if(oldVer and tonumber(oldVer)>tonumber(ARGV[1]))then return redis.status_reply("IGNORE"); else return redis.call('mset',KEYS[1],ARGV[1],KEYS[2],ARGV[2],KEYS[3],ARGV[3]);end`,
		3, key+"-ver", key+"-val", key+"-size", ver, data, len(data))
	if err != nil {
		log.E("Cache-Update remote update cache fail with %v", err)
		return
	}
	c.log("Cache update remote cache by key(%v),ver(%v),size(%v) success with %v", key, ver, len(data), res)
	return
}

func (c *Cache) removeLocal(key string) {
	c.cacheLck.Lock()
	if element, ok := c.mcache[key]; ok {
		c.cache.Remove(element)
		delete(c.mcache, element.Value.(*Item).Key)
	}
	c.cacheLck.Unlock()
}

func (c *Cache) addLocal(key string, ver int64, data []byte) (newItem *Item) {
	c.cacheLck.Lock()
	defer c.cacheLck.Unlock()
	newItem = &Item{
		Key:  key,
		Ver:  ver,
		Data: data,
		Last: Now(),
	}
	newSize := newItem.Size()
	for c.cache.Len() > 0 {
		if c.size+newSize < c.MemLimit {
			break
		}
		//remove old one
		element := c.cache.Back()
		c.cache.Remove(element)
		old := element.Value.(*Item)
		delete(c.mcache, old.Key)
		c.size -= old.Size()
	}
	c.mcache[key] = c.cache.PushFront(newItem)
	return
}

func (c *Cache) Try(key string, val interface{}) (err error) {
	if c.Disable {
		err = NoFound
		return
	}
	c.cacheLck.Lock()
	element, ok := c.mcache[key]
	c.cacheLck.Unlock()
	conn := C()
	defer conn.Close()
	if ok {
		res, execErr := redis.Values(conn.Do("MGET", key+"-ver", key+"-size"))
		if execErr != nil {
			err = execErr
			log.E("Cache try get the data verison and size by key(%v) fail with %v", key, err)
			return
		}
		remoteSize, execErr := redis.Int64(res[1], nil)
		if execErr == redis.ErrNil || remoteSize < 1 {
			//remote version not found, but local found
			//remove local and return not found
			c.removeLocal(key)
			err = NoFound
			c.log("Cache local cache foud by key(%v), but remote cache is empty, will clear local", key)
			return
		} else if execErr != nil {
			err = execErr
			log.E("Cache try get the data size by key(%v) fail with %v", key, err)
			return
		}
		remoteVer, execErr := redis.Int64(res[0], nil)
		if execErr == redis.ErrNil {
			//remote version not found, but local found
			//remove local and return not found
			c.removeLocal(key)
			err = NoFound
			c.log("Cache local cache foud by key(%v), but remote version is empty, will clear local", key)
			return
		} else if execErr != nil {
			err = execErr
			log.E("Cache try get the data verison by key(%v) fail with %v", key, err)
			return
		}

		item := element.Value.(*Item)
		if item.Ver == remoteVer { //cache hited
			atomic.AddUint64(&c.LocalHited, 1)
			c.log("Cache local cache hited by key(%v),ver(%v)", key, item.Ver)
			err = item.Unmarshal(val)
			return
		}
		//local cache is expired.
		c.removeLocal(key)
	}
	res, execErr := redis.Values(conn.Do("MGET", key+"-ver", key+"-size", key+"-val"))
	if execErr != nil {
		err = execErr
		log.E("Cache try get the data and version by key(%v) fail with %v", key, err)
		return
	}
	ver, execErr := redis.Int64(res[0], nil)
	if execErr != nil || ver < 1 {
		//remove version not found
		err = NoFound
		return
	}
	size, execErr := redis.Int64(res[1], nil)
	if execErr != nil || size < 1 {
		//remove version not found
		err = NoFound
		return
	}
	data, execErr := redis.Bytes(res[2], nil)
	if execErr != nil {
		//remove data not found
		err = NoFound
		return
	}
	item := c.addLocal(key, ver, data)
	atomic.AddUint64(&c.RemoteHited, 1)
	c.log("Cache remote cache hited by key(%v),ver(%v)", key, item.Ver)
	err = item.Unmarshal(val)
	return
}

func (c *Cache) log(format string, args ...interface{}) {
	if c.ShowLog {
		log.D_(1, format, args...)
	}
}
