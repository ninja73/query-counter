package main

import (
	"context"
	"log"
	"os"
	"query-counter/btree"
	"query-counter/lru"
	"sync"
)

type QueryWorker struct {
	indexes  *btree.BTree
	cache    *lru.LRU
	poolSize int
	wg       *sync.WaitGroup
	workers  chan string
	results  chan query
}

func NewQueryWorker(indexes *btree.BTree, poolSize int, cache *lru.LRU) (*QueryWorker, error) {
	workers := make(chan string, 100)
	results := make(chan query, 100)
	var wg sync.WaitGroup

	return &QueryWorker{
		indexes:  indexes,
		poolSize: poolSize,
		cache:    cache,
		workers:  workers,
		results:  results,
		wg:       &wg,
	}, nil
}

func (qw *QueryWorker) worker(jobs <-chan string, results chan<- query) {
	for j := range jobs {
		old := qw.cache.PushOrIncrement(j, 1)
		if old != nil {
			results <- query{key: old.Key, vale: old.Value}
		}
	}
	qw.wg.Done()
}

func (qw *QueryWorker) InitWorkers() {
	for w := 1; w <= qw.poolSize; w++ {
		qw.wg.Add(1)
		go qw.worker(qw.workers, qw.results)
	}
}

func (qw *QueryWorker) Send(query string) {
	qw.workers <- query
}

func (qw *QueryWorker) ResultProcessing(ctx context.Context) {
	for {
		select {
		case q := <-qw.results:
			value, ok, err := qw.indexes.Get(q.key)
			if err != nil {
				log.Fatal(err)
			}
			if !ok {
				if err := qw.indexes.Insert(btree.NewPairs(q.key, q.vale)); err != nil {
					log.Fatal(err)
				}
				continue
			}
			if _, err = qw.indexes.Update(q.key, value+q.vale); err != nil {
				log.Fatal(err)
			}
		case <-ctx.Done():
			log.Print("Result processing done")
		}
	}
}

func (qw *QueryWorker) Wait(cancel func()) {
	qw.wg.Wait()
	cancel()
}

func (qw *QueryWorker) ExportToFile(path string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	return qw.indexes.Export(file)
}
