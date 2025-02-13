package dict

import (
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// 将 key 分散到固定数量的 shard 中避免 rehash 操作
// shard 是有锁保护的 map, 当 shard 进行 rehash 时会阻塞shard内的读写，但不会对其他 shard 造成影响
type Shard struct {
	m     map[string]interface{}
	mutex sync.RWMutex
}

type ConcurrentDict struct {
	table      []*Shard
	count      int32
	shardCount int
}

// 计算容量，保证容量是2的倍数
func computeCapacity(param int) (size int) {
	if param <= 16 {
		return 16
	}
	n := param - 1
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	if n < 0 {
		return math.MaxInt32
	}
	return n + 1
}

// 创建Concurrent
func MakeConcurrent(shardCount int) *ConcurrentDict {
	if shardCount <= 0 {
		return nil
	}
	if shardCount == 1 {
		table := []*Shard{
			{
				m: make(map[string]interface{}),
			},
		}
		return &ConcurrentDict{
			table:      table,
			count:      0,
			shardCount: shardCount,
		}
	}
	shardCount = computeCapacity(shardCount)
	table := make([]*Shard, shardCount)
	for i := 0; i < shardCount; i++ {
		table[i] = &Shard{
			m: make(map[string]interface{}),
		}
	}
	return &ConcurrentDict{
		table:      table,
		count:      0,
		shardCount: shardCount,
	}
}

// FNV哈希算法
const prime32 = uint32(16777619)

func fnv32(key string) uint32 {
	hash := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		hash *= prime32
		hash ^= uint32(key[i])
	}
	return hash
}

// 根据key计算出Shard的索引
func (dict *ConcurrentDict) spread(key string) uint32 {
	if dict == nil {
		panic("dict is nil !")
	}
	// 如果只有一个Shard
	if len(dict.table) == 0 {
		return 0
	}
	hashcode := fnv32(key)
	tableSize := uint32(len(dict.table))
	// 使用位运算计算最终的索引位置：
	// tableSize - 1 计算出哈希表大小减去 1 的值。
	// & 是按位与操作符。这个操作可以确保返回的索引在 0 到 tableSize - 1 的范围内。
	// 这种方法利用了哈希表的大小是 2 的幂次方的特性，确保哈希值可以被正确地映射到表的索引。
	return hashcode & (tableSize - 1)
}

// 根据索引获得Shard
func (dict *ConcurrentDict) getShard(index uint32) *Shard {
	if dict == nil {
		panic("dict is nil !")
	}
	return dict.table[index]
}

// 根据key获得对应的value
func (dict *ConcurrentDict) Get(key string) (val interface{}, exists bool) {
	if dict == nil {
		panic("dict is nil !")
	}
	shardIndex := dict.spread(key)
	shard := dict.getShard(shardIndex)
	shard.mutex.Lock()
	defer shard.mutex.Unlock()
	val, exists = shard.m[key]
	return
}

// 在已经持有锁的情况下进行读
func (dict *ConcurrentDict) GetWithLock(key string) (val interface{}, exists bool) {
	if dict == nil {
		panic("dict is nil")
	}
	index := dict.spread(key)
	s := dict.getShard(index)
	val, exists = s.m[key]
	return
}

// 返回dict中的元素个数
func (dict *ConcurrentDict) Len() int {
	if dict == nil {
		panic("dict is nil !")
	}
	// atomic.LoadInt32(&dict.count) 使用了原子操作来读取 dict.count 的值。
	// 这种方式确保了在高并发情况下，读取 count 的值是安全的，不会因为其他线程的写入操作而导致读取到不一致的值。
	return int(atomic.LoadInt32(&dict.count))
}

// 想dict中插入元素，如果插入成功返回1，替换元素返回0
func (dict *ConcurrentDict) Put(key string, val interface{}) (result int) {
	if dict == nil {
		panic("dict is nil !")
	}
	shardIndex := dict.spread(key)
	shard := dict.getShard(shardIndex)
	shard.mutex.Lock()
	defer shard.mutex.Unlock()
	if _, ok := shard.m[key]; ok {
		shard.m[key] = val
		return 0
	}
	dict.addCount()
	shard.m[key] = val
	return 1
}

// 在持有锁的情况下直接插入元素
func (dict *ConcurrentDict) PutWithLock(key string, val interface{}) (result int) {
	if dict == nil {
		panic("dict is nil !")
	}
	shardIndex := dict.spread(key)
	shard := dict.getShard(shardIndex)
	if _, ok := shard.m[key]; ok {
		shard.m[key] = val
		return 0
	}
	dict.addCount()
	shard.m[key] = val
	return 1
}

// 只有当key不存在的时候，才会插入元素，返回插入的元素个数
func (dict *ConcurrentDict) PutIfAbsent(key string, val interface{}) (result int) {
	if dict == nil {
		panic("dict is nil !")
	}
	shardIndex := dict.spread(key)
	shard := dict.getShard(shardIndex)
	shard.mutex.Lock()
	defer shard.mutex.Unlock()
	if _, ok := shard.m[key]; ok {
		return 0
	}
	shard.m[key] = val
	dict.addCount()
	return 1
}
func (dict *ConcurrentDict) PutIfAbsentWithLock(key string, val interface{}) (result int) {
	if dict == nil {
		panic("dict is nil !")
	}
	shardIndex := dict.spread(key)
	shard := dict.getShard(shardIndex)
	if _, ok := shard.m[key]; ok {
		return 0
	}
	shard.m[key] = val
	dict.addCount()
	return 1
}

// 只有在key存在的情况下，才会向dict中插入元素，返回插入的元素的个数
func (dict *ConcurrentDict) PutIfExists(key string, val interface{}) (result int) {
	if dict == nil {
		panic("dict is nil !")
	}
	shardIndex := dict.spread(key)
	shard := dict.getShard(shardIndex)
	shard.mutex.Lock()
	defer shard.mutex.Unlock()
	if _, ok := shard.m[key]; ok {
		shard.m[key] = val
		return 1
	}
	return 0
}
func (dict *ConcurrentDict) PutIfExistsWithLock(key string, val interface{}) (result int) {
	if dict == nil {
		panic("dict is nil !")
	}
	shardIndex := dict.spread(key)
	shard := dict.getShard(shardIndex)
	if _, ok := shard.m[key]; ok {
		shard.m[key] = val
		return 1
	}
	return 0
}

// Remove 删除元素，然后返回删除的键值对的个数
func (dict *ConcurrentDict) Remove(key string) (val interface{}, result int) {
	if dict == nil {
		panic("dict is nil !")
	}
	shardIndex := dict.spread(key)
	shard := dict.getShard(shardIndex)
	shard.mutex.Lock()
	defer shard.mutex.Unlock()
	if val, ok := shard.m[key]; ok {
		delete(shard.m, key)
		return val, 1
	}
	return nil, 0
}
func (dict *ConcurrentDict) RemoveWithLock(key string) (val interface{}, result int) {
	if dict == nil {
		panic("dict is nil !")
	}
	shardIndex := dict.spread(key)
	shard := dict.getShard(shardIndex)
	if val, ok := shard.m[key]; ok {
		delete(shard.m, key)
		return val, 1
	}
	return nil, 0
}

func (dict *ConcurrentDict) addCount() {
	atomic.AddInt32(&dict.count, 1)
}

func (dict *ConcurrentDict) decreaseCount() {
	atomic.AddInt32(&dict.count, -1)
}

// 用来遍历dict，并进行消费
func (dict *ConcurrentDict) ForEach(consumer Consumer) {
	if dict == nil {
		panic("dict is nil !")
	}
	for _, shard := range dict.table {
		shard.mutex.RLock()
		// 开始访问shard的元素
		// 这里设计的很巧妙，通过把对每一个Shard的访问写进一个内部函数中，这样再defer解锁
		f := func() bool {
			defer shard.mutex.RUnlock()
			for key, val := range shard.m {
				continues := consumer(key, val)
				if !continues {
					return false
				}
			}
			return true
		}()
		if !f {
			break
		}
	}
}

// 返回所有的键
func (dict *ConcurrentDict) Keys() []string {
	if dict == nil {
		panic("dict is nil !")
	}
	result := make([]string, 0, dict.Len())
	i := 0
	dict.ForEach(func(key string, val interface{}) bool {
		if i < len(result) {
			result[i] = key
			i++
		} else {
			// 在遍历的过程中，dict的键值对个数有可能会发生改变
			// 如果超出了预分配的数目，那就采用append进行动态扩展
			result = append(result, key)
		}
		return true
	})
	return result
}

// RandomKey 随机返回一个Key
func (shard *Shard) RandomKey() string {
	if shard == nil {
		panic("shard is nil !")
	}
	shard.mutex.RLock()
	defer shard.mutex.RUnlock()
	for key, _ := range shard.m {
		return key
	}
	return ""
}

// RandomKeys 随机返回固定数目的key，可能包含重复的key
func (dict *ConcurrentDict) RandomKeys(limit int) []string {
	if dict == nil {
		panic("dict is nil !")
	}
	result := make([]string, 0, limit)
	i := 0
	dict.ForEach(func(key string, val interface{}) bool {
		if i < len(result) {
			result[i] = key
			i++
		} else {
			return false
		}
		return true
	})
	return result
}

func (dict *ConcurrentDict) RandomDistinctKeys(limit int) []string {
	size := dict.Len()
	if limit >= size {
		return dict.Keys()
	}

	shardCount := len(dict.table)
	result := make(map[string]struct{})
	nR := rand.New(rand.NewSource(time.Now().UnixNano()))
	for len(result) < limit {
		shardIndex := uint32(nR.Intn(shardCount))
		s := dict.getShard(shardIndex)
		if s == nil {
			continue
		}
		key := s.RandomKey()
		if key != "" {
			if _, exists := result[key]; !exists {
				result[key] = struct{}{}
			}
		}
	}
	arr := make([]string, limit)
	i := 0
	for k := range result {
		arr[i] = k
		i++
	}
	return arr
}

// 删除dict中所有的数据
func (dict *ConcurrentDict) Clear() {
	*dict = *MakeConcurrent(dict.shardCount)
}
