package main

import (
	"log"
	"os"
	"query-counter/btree"
	"query-counter/lru"
	"sync"
)

type QueryWorker struct {
	db       *btree.BTree
	cache    *lru.LRU
	poolSize int
	wg       *sync.WaitGroup
	workers  chan string
	results  chan query
}

func NewQueryWorker(db *btree.BTree, poolSize int, cache *lru.LRU) (*QueryWorker, error) {
	workers := make(chan string, 100)
	results := make(chan query, 100)
	var wg sync.WaitGroup

	return &QueryWorker{
		db:       db,
		poolSize: poolSize,
		cache:    cache,
		workers:  workers,
		results:  results,
		wg:       &wg,
	}, nil
}

func (qw *QueryWorker) worker(jobs <-chan string, results chan<- query) {
	defer qw.wg.Done()
	for j := range jobs {
		oldValue, hasOld := qw.cache.PushOrIncrement(j, 1)
		if hasOld {
			results <- query{key: j, vale: oldValue}
		}
	}
}

func (qw *QueryWorker) InitWorkers() {
	qw.wg.Add(qw.poolSize)
	for w := 1; w <= qw.poolSize; w++ {
		go qw.worker(qw.workers, qw.results)
	}
}

func (qw *QueryWorker) Send(query string) {
	qw.workers <- query
}

func (qw *QueryWorker) writeToDB(key string, value uint64) error {
	val, ok, err := qw.db.Get(key)
	if err != nil {
		log.Fatal(err)
	}
	if !ok {
		if err := qw.db.Insert(btree.NewPairs(key, value)); err != nil {
			return err
		}
		return nil
	}
	if _, err = qw.db.Update(key, val+value); err != nil {
		return err
	}
	return nil
}

func (qw *QueryWorker) ResultProcessing() {
	for {
		q := <-qw.results
		if err := qw.writeToDB(q.key, q.vale); err != nil {
			log.Fatal(err)
		}
	}
}

func (qw *QueryWorker) Close() {
	close(qw.workers)
}

func (qw *QueryWorker) Wait() {
	qw.wg.Wait()
	qw.cache.Range(func(key string, value uint64) {
		if err := qw.writeToDB(key, value); err != nil {
			log.Fatal(err)
		}
	})
}

func (qw *QueryWorker) ExportToFile(path string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	return qw.db.Export(file)
}
