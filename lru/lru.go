package lru

import (
	"errors"
	"sync"
)

type Node struct {
	prev  *Node
	next  *Node
	key   string
	value uint64
}

type LRU struct {
	data    map[string]*Node
	head    *Node
	tail    *Node
	maxSize int
	len     int
	lock    sync.RWMutex
}

func NewLRU(maxSize int) (*LRU, error) {
	if maxSize <= 0 {
		return nil, errors.New("lru max-size must provide a positive size")
	}

	return &LRU{
		data:    make(map[string]*Node, maxSize),
		head:    nil,
		tail:    nil,
		maxSize: maxSize,
		len:     0,
	}, nil
}

func (lru *LRU) removeOld() *Node {
	if lru.len > lru.maxSize {
		old := lru.tail
		newTail := old.prev
		newTail.next = nil
		lru.tail = newTail

		old.prev = nil
		old.next = nil
		delete(lru.data, old.key)
		lru.len -= 1
		return old
	}
	return nil
}

func (lru *LRU) attach(node *Node) {
	lru.len += 1
	if lru.head != nil {
		node.next = lru.head
		lru.head.prev = node
		lru.head = node
	} else {
		lru.head = node
		lru.tail = node
	}
}

func (lru *LRU) detach(node *Node) {
	lru.len -= 1
	if node.prev != nil {
		node.prev.next = node.next
	} else {
		lru.head = node.next
	}

	if node.next != nil {
		node.next.prev = node.prev
	} else {
		lru.tail = node.prev
	}

	node.prev = nil
	node.next = nil
}

func (lru *LRU) PushOrIncrement(key string, value uint64) (uint64, bool) {
	lru.lock.Lock()
	defer lru.lock.Unlock()

	node, ok := lru.data[key]
	if ok {
		node.value += 1
		lru.detach(node)
		lru.attach(node)
	} else {
		node = &Node{key: key, value: value}
		lru.data[key] = node
		lru.attach(node)
	}

	if old := lru.removeOld(); old != nil {
		return old.value, true
	}

	return 0, false
}

func (lru *LRU) Get(key string) (uint64, bool) {
	lru.lock.RLock()
	defer lru.lock.RUnlock()

	if node, ok := lru.data[key]; ok {
		lru.detach(node)
		lru.attach(node)
		return node.value, true
	} else {
		return 0, false
	}
}

func (lru *LRU) Range(f func(key string, value uint64)) {
	lru.lock.RLock()
	defer lru.lock.RUnlock()

	for k, v := range lru.data {
		f(k, v.value)
	}
}
