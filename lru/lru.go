package lru

import (
	"sync"
	"sync/atomic"
)

type Node struct {
	prev  *Node
	next  *Node
	Key   string
	Value uint64
}

type LRU struct {
	data    sync.Map
	head    *Node
	tail    *Node
	maxSize int
	len     int
	lock    sync.Mutex
}

func NewLRU(maxSize int) *LRU {
	return &LRU{
		head:    nil,
		tail:    nil,
		maxSize: maxSize,
		len:     0,
	}
}

func (lru *LRU) attach(node *Node) *Node {
	lru.lock.Lock()
	defer lru.lock.Unlock()

	lru.len += 1
	if lru.head != nil {
		node.next = lru.head
		lru.head.prev = node
		lru.head = node
	} else {
		lru.head = node
		lru.tail = node
	}

	if lru.len == lru.maxSize && lru.tail != nil {
		old := lru.tail
		newTail := old.prev
		newTail.next = nil
		lru.tail = newTail

		old.prev = nil
		old.next = nil
		lru.data.Delete(old.Key)
		return old
	}
	return nil
}

func (lru *LRU) detach(node *Node) {
	lru.lock.Lock()
	defer lru.lock.Unlock()

	lru.len -= 1
	if node.prev != nil {
		node.prev.next = node.next
	}
	if node.next != nil {
		node.next.prev = node.prev
	} else {
		lru.tail = node.prev
	}
}

func (lru *LRU) PushOrIncrement(key string, value uint64) (old *Node) {
	n, ok := lru.data.LoadOrStore(key, &Node{Key: key, Value: value})
	node := n.(*Node)
	if ok {
		atomic.AddUint64(&node.Value, value)
		lru.detach(node)
		old = lru.attach(node)
	} else {
		old = lru.attach(node)
	}
	return
}

func (lru *LRU) Get(key string) (*Node, bool) {
	if n, ok := lru.data.Load(key); ok {
		node := n.(*Node)
		lru.detach(node)
		lru.attach(node)
		return node, true
	} else {
		return nil, false
	}
}

func (lru *LRU) Range(f func(key string, value *Node) bool) {
	lru.data.Range(func(key, value interface{}) bool {
		keyString := key.(string)
		node := value.(*Node)
		return f(keyString, node)
	})
}
