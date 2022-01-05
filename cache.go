/*
Package rediscache imple the normal Try/Update/Expire cache.
it will using cache by local and remote pool.
*/
package rediscache

import (
	"container/list"
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gomodule/redigo/redis"
)

//ErrNoFound is const define for cache not found error.
var ErrNoFound = fmt.Errorf("cache not found")

func WatchVersion(allver []interface{}, xerr error) (ver int64, cacheWatch, remoteWatch string, err error) {
	if xerr != nil {
		err = xerr
		return
	}
	ver, err = redis.Int64(allver[0], nil)
	if err != nil && err != redis.ErrNil {
		return
	}
	err = nil
	cacheWatch, err = redis.String(allver[1], nil)
	if err != nil && err != redis.ErrNil {
		return
	}
	err = nil
	//
	for i := 2; i < len(allver); i++ {
		iv, xerr := redis.Int64(allver[i], nil)
		if xerr == redis.ErrNil {
			allver[i] = 0
		} else if xerr != nil {
			err = xerr
			return
		} else {
			allver[i] = iv
		}
	}
	if len(allver) > 1 {
		remoteWatch = Join(allver[2:], ",")
	}
	return
}

func Join(v interface{}, seq string) string {
	vtype := reflect.TypeOf(v)
	if vtype.Kind() != reflect.Slice {
		return ""
	}
	vval := reflect.ValueOf(v)
	if vval.Len() < 1 {
		return ""
	}
	val := fmt.Sprintf("%v", reflect.Indirect(vval.Index(0)).Interface())
	for i := 1; i < vval.Len(); i++ {
		val += fmt.Sprintf("%v%v", seq, reflect.Indirect(vval.Index(i)).Interface())
	}
	return val
}

func WatchKeys(key string, watch ...string) (keys []interface{}) {
	keys = append(keys, key+"-ver", key+"-watch")
	for _, w := range watch {
		keys = append(keys, w+"-ver")
	}
	return
}

func KeyPrefix(key string) string {
	idx := strings.LastIndex(key, "-")
	if idx < 0 {
		return key
	} else {
		return key[:idx]
	}
}

//Item is cache item.
type Item struct {
	Key   string
	Ver   int64
	Watch string
	Data  []byte
	Last  int64
}

//Size will return the memory size of cache used.
func (i *Item) Size() uint64 {
	return uint64(len(i.Data)+len(i.Key)) + 64
}

//Unmarshal will unmarshal the []byte data to struct by json.Unmarshal
func (i *Item) Unmarshal(v interface{}) error {
	return json.Unmarshal(i.Data, v)
}

//Cache is the cache pool
type Cache struct {
	MemLimit    uint64
	Disable     bool
	size        uint64
	cache       *list.List
	mcache      map[string]*list.Element
	cacheLck    sync.RWMutex
	LocalHited  uint64
	RemoteHited uint64
	hited       map[string]uint64
	hitedLck    sync.RWMutex
	ShowLog     bool
	ExpirerExpr map[string]time.Duration
	expirerLast map[string]time.Time
	expirerExit chan int
	expirerOn   bool
}

//NewCache is the creator to create one cache pool by local memory max limit.
func NewCache(memLimit uint64) *Cache {
	return &Cache{
		MemLimit:    memLimit,
		cache:       list.New(),
		mcache:      map[string]*list.Element{},
		hited:       map[string]uint64{},
		ExpirerExpr: map[string]time.Duration{},
		expirerLast: map[string]time.Time{},
		expirerExit: make(chan int, 1),
	}
}

func (c *Cache) State() (val interface{}, err error) {
	cached := map[string]uint64{}
	hited := map[string]uint64{}
	c.cacheLck.Lock()
	for key, cache := range c.mcache {
		cached[KeyPrefix(key)] += cache.Value.(*Item).Size()
	}
	c.cacheLck.Unlock()
	c.hitedLck.Lock()
	for key, h := range c.hited {
		hited[key] = h
	}
	c.hitedLck.Unlock()
	val = map[string]interface{}{
		"max":          c.MemLimit,
		"disable":      c.Disable,
		"used":         c.size,
		"local_hited":  c.LocalHited,
		"remote_hited": c.RemoteHited,
		"cached":       cached,
		"hited":        hited,
		"showlog":      c.ShowLog,
	}
	return
}

//Version will return the cache version by key.
func (c *Cache) Version(keys ...string) (ver []int64, err error) {
	if c.Disable {
		for range keys {
			ver = append(ver, 0)
		}
		return
	}
	conn := C()
	defer conn.Close()
	verKeys := []interface{}{}
	for _, key := range keys {
		verKeys = append(verKeys, key+"-ver")
	}
	allver, err := redis.Values(conn.Do("MGET", verKeys...))
	if err != nil {
		return
	}
	for _, v := range allver {
		iv, xerr := redis.Int64(v, nil)
		if err == redis.ErrNil {
			ver = append(ver, 0)
		} else if xerr != nil {
			err = xerr
			return
		} else {
			ver = append(ver, iv)
		}
	}
	return
}

func (c *Cache) WatchVersion(key string, watch ...string) (ver int64, cacheWatch string, remoteWatch string, err error) {
	if c.Disable {
		return
	}
	conn := C()
	defer conn.Close()
	ver, cacheWatch, remoteWatch, err = WatchVersion(redis.Values(conn.Do("MGET", WatchKeys(key, watch...)...)))
	return
}

func (c *Cache) LoadRemoteData(key string) (data []byte, err error) {
	conn := C()
	defer conn.Close()
	data, err = redis.Bytes(conn.Do("GET", key+"-val"))
	if err == redis.ErrNil || len(data) < 1 {
		//remote data not found
		err = ErrNoFound
	}
	return
}

//Update the cahce by key/ver and data.
//it will marshal the val to []byte by json.Marshal.
//return nil when all is done well, or return fail message.
func (c *Cache) update(key string, ver int64, wver string, val interface{}) (err error) {
	if c.Disable {
		return
	}
	data, err := json.Marshal(val)
	if err != nil {
		log.Printf("[Error]Cache-Update marshal fail with %v", err)
		return
	}
	if len(data) < 1 {
		panic("empty data")
	}
	err = c.remoteUpdate(key, ver, wver, data)
	if err == nil {
		c.addLocal(key, ver, wver, data)
	}
	return
}

//Expire the cache by key and version.
//return nil when the local and remote cache is updated, or return fail message.
func (c *Cache) Expire(keys ...string) (vers []int64, err error) {
	if c.Disable {
		for range keys {
			vers = append(vers, 0)
		}
		return
	}
	for _, key := range keys {
		c.removeLocal(key)
	}
	vers, err = c.expireRemote(keys...)
	return
}

func (c *Cache) ExpirePrefix(base []string, prefix string, keys ...string) (vers []int64, err error) {
	if c.Disable {
		for range base {
			vers = append(vers, 0)
		}
		for range keys {
			vers = append(vers, 0)
		}
		return
	}
	allkeys := base
	for _, key := range keys {
		allkeys = append(allkeys, prefix+key)
	}
	vers, err = c.Expire(allkeys...)
	return
}

func (c *Cache) ExpireMultiPrefix(base []string, prefix []string, keys ...string) (vers []int64, err error) {
	if c.Disable {
		for range base {
			vers = append(vers, 0)
		}
		for range prefix {
			for range keys {
				vers = append(vers, 0)
			}
		}
		return
	}
	allkeys := base
	for _, pre := range prefix {
		for _, key := range keys {
			allkeys = append(allkeys, pre+key)
		}
	}
	vers, err = c.Expire(allkeys...)
	return
}

//update remote cache pool
func (c *Cache) remoteUpdate(key string, ver int64, wver string, data []byte) (err error) {
	conn := C()
	defer conn.Close()
	res, err := conn.Do("eval",
		`local oldVer=redis.call('get',KEYS[1]);if(oldVer and tonumber(oldVer)>tonumber(ARGV[1]))then return redis.status_reply("IGNORE"); else return redis.call('mset',KEYS[1],ARGV[1],KEYS[2],ARGV[2],KEYS[3],ARGV[3]);end`,
		3, key+"-ver", key+"-val", key+"-watch", ver, data, wver)
	if err != nil {
		log.Printf("[Error]Cache-Update remote update cache fail with %v", err)
		return
	}
	c.log("Cache update remote cache by key(%v),ver(%v),size(%v) success with %v", key, ver, len(data), res)
	return
}

func (c *Cache) expireRemote(keys ...string) (vers []int64, err error) {
	conn := C()
	defer conn.Close()
	conn.Send("MULTI")
	for _, key := range keys {
		conn.Send("MSET", key+"-val", []byte(""))
		conn.Send("INCR", key+"-ver")
	}
	res, err := redis.Values(conn.Do("EXEC"))
	if err != nil {
		log.Printf("[Error]Cache expire remote cache fail with %v", err)
		return
	}
	for i := 1; i < len(res); i += 2 {
		ver, _ := redis.Int64(res[i], nil)
		vers = append(vers, ver)
	}
	c.log("Cache expire remote cache by keys(%v) success with version(%v)", keys, vers)
	return
}

//remote cache from local cache pool
func (c *Cache) removeLocal(key string) {
	c.cacheLck.Lock()
	if element, ok := c.mcache[key]; ok {
		c.cache.Remove(element)
		delete(c.mcache, element.Value.(*Item).Key)
		old := element.Value.(*Item)
		c.size -= old.Size()
		c.log("Cache remove local cache(%v),size(%v), current used(%v)", old.Key, old.Size(), c.size)
	}
	c.cacheLck.Unlock()
}

func (c *Cache) Clear(expr string) (localRemoved int64, remoteRemoved int64, err error) {
	localRemoved, err = c.removeLocalMatched(expr)
	if err == nil {
		remoteRemoved, err = c.removeRemoteMatched(expr)
	}
	return
}

func (c *Cache) removeRemoteMatched(expr string) (removed int64, err error) {
	conn := C()
	defer conn.Close()
	removed, err = redis.Int64(conn.Do("EVAL", `return redis.call("del", "___", unpack(redis.call("keys", ARGV[1])))`, 0, expr))
	c.log("Cache expire %v remote cache by expr %v success ", removed, expr)
	return
}

func (c *Cache) removeLocalMatched(expr string) (removed int64, err error) {
	reg, err := regexp.Compile(expr)
	if err != nil {
		return
	}
	c.cacheLck.Lock()
	for key, element := range c.mcache {
		if !reg.MatchString(key) {
			continue
		}
		c.cache.Remove(element)
		delete(c.mcache, element.Value.(*Item).Key)
		old := element.Value.(*Item)
		c.size -= old.Size()
		c.log("Cache remove local cache(%v),size(%v), current used(%v)", old.Key, old.Size(), c.size)
		removed++
	}
	c.cacheLck.Unlock()
	return
}

// add data to local cache pool
func (c *Cache) addLocal(key string, ver int64, wver string, data []byte) (newItem *Item) {
	c.cacheLck.Lock()
	defer c.cacheLck.Unlock()
	newItem = &Item{
		Key:   key,
		Ver:   ver,
		Watch: wver,
		Data:  data,
		Last:  Now(),
	}
	newSize := newItem.Size()
	for c.cache.Len() > 0 {
		if c.MemLimit < 1 || c.size+newSize < c.MemLimit {
			break
		}
		//remove old one
		element := c.cache.Back()
		c.cache.Remove(element)
		old := element.Value.(*Item)
		delete(c.mcache, old.Key)
		c.size -= old.Size()
		c.log("Cache remove local cache(%v),size(%v), current used(%v)", old.Key, old.Size(), c.size)
	}
	c.size += newSize
	c.mcache[key] = c.cache.PushFront(newItem)
	c.log("Cache add local cache(%v),size(%v), current used(%v)", newItem.Key, newSize, c.size)
	return
}

//Try get the data from cache.
//it will try find cache on local memory, if cache not found try remote.
//return NotFound when cache not exist, return nil when the cache hited, or return fail error.
func (c *Cache) Try(key string, val interface{}, watch ...string) (remoteCachVer int64, remoteNewWatch string, err error) {
	if c.Disable {
		err = ErrNoFound
		return
	}
	remoteCachVer, remoteCacheWatch, remoteNewWatch, err := c.WatchVersion(key, watch...)
	if err != nil {
		log.Printf("[Error]Cache try get the data verison by key(%v) fail with %v", key, err)
		return
	}
	if remoteCacheWatch != remoteNewWatch { //watch change.
		c.log("Cache the key(%v) cache is expired by watch expired", key)
		vers, xerr := c.Expire(key) //expire the key and get the new cache version
		if xerr != nil {
			err = xerr
			return
		}
		remoteCachVer = vers[0]
		err = ErrNoFound
		return
	}
	c.cacheLck.Lock()
	element, ok := c.mcache[key]
	c.cacheLck.Unlock()
	conn := C()
	defer conn.Close()
	if ok {
		item := element.Value.(*Item)
		if item.Ver == remoteCachVer && item.Watch == remoteCacheWatch { //cache hited
			atomic.AddUint64(&c.LocalHited, 1)
			c.hitedLck.Lock()
			c.hited[KeyPrefix(key)]++
			c.hitedLck.Unlock()
			c.log("Cache local cache hited(%v) by key(%v),ver(%v)", c.LocalHited, key, item.Ver)
			err = item.Unmarshal(val)
			return
		}
		//local cache is expired.
		c.removeLocal(key)
	}
	data, err := c.LoadRemoteData(key)
	if err != nil {
		return
	}
	item := c.addLocal(key, remoteCachVer, remoteCacheWatch, data)
	atomic.AddUint64(&c.RemoteHited, 1)
	c.log("Cache remote cache hited(%v) by key(%v),ver(%v)", c.RemoteHited, key, item.Ver)
	err = item.Unmarshal(val)
	c.hitedLck.Lock()
	c.hited[KeyPrefix(key)]++
	c.hitedLck.Unlock()
	return
}

func (c *Cache) log(format string, args ...interface{}) {
	if c.ShowLog {
		log.Output(1, fmt.Sprintf(format, args...))
	}
}

//WillModify impl modify and expire cache by redis sequece
func (c *Cache) WillModify(key string, call func() error, notify ...string) (err error) {
	err = call()
	keys := []string{key}
	keys = append(keys, notify...)
	c.Expire(keys...)
	return
}

//WillQuery impl query and update cache by redis sequece.
func (c *Cache) WillQuery(key string, val interface{}, call func() (val interface{}, err error), watch ...string) (err error) {
	remoteCachVer, remoteNewWatch, err := c.Try(key, val, watch...)
	if err != ErrNoFound {
		return
	}
	newval, err := call()
	if err != nil {
		return
	}
	reflect.Indirect(reflect.ValueOf(val)).Set(reflect.ValueOf(newval))
	c.update(key, remoteCachVer, remoteNewWatch, newval)
	return
}

func (c *Cache) StartExpirer(delay time.Duration) {
	c.expirerOn = true
	go c.loopExpirer(delay)
}

func (c *Cache) StopExpirer() {
	c.expirerExit <- 1
}

func (c *Cache) loopExpirer(delay time.Duration) {
	ticker := time.NewTicker(delay)
	running := true
	for running {
		select {
		case <-c.expirerExit:
			running = false
		case <-ticker.C:
			c.procExpirer()
		}
	}
	c.expirerOn = false
}

func (c *Cache) procExpirer() (err error) {
	defer func() {
		if perr := recover(); perr != nil {
			c.log("Cache.Expirer proc expirer is panic with %v", perr)
		}
	}()
	var localRemoved, remoteRemoved int64
	for expr, delay := range c.ExpirerExpr {
		last := c.expirerLast[expr]
		if time.Since(last) < delay {
			continue
		}
		localRemoved, remoteRemoved, err = c.Clear(expr)
		if err != nil {
			c.log("Cache.Expirer expire clear by expr %v fail with %v", expr, err)
			break
		}
		c.expirerLast[expr] = time.Now()
		c.log("Cache.Expirer expire clear by expr %v with local:%v,remote:%v keys is deleted", expr, localRemoved, remoteRemoved)
	}
	return
}
