package main

import (
	"context"
	"flag"
	"log"
	"query-counter/btree"
	"query-counter/lru"
)

func main() {
	var inputPath = flag.String("input", "./queries.txt", "Parser file")
	var outputPath = flag.String("output", "./result.txt", "Result file")
	var cacheSize = flag.Int("cache-size", 10000, "Cache size")
	var indexes = flag.String("indexes", "./indexes", "Index file")
	flag.Parse()

	bTree, err := btree.NewBTree(*indexes)
	if err != nil {
		log.Fatal(err)
	}
	cache := lru.NewLRU(*cacheSize)

	worker, err := NewQueryWorker(bTree, 10, cache)
	if err != nil {
		log.Fatal(err)
	}

	queryReader, err := NewQueryReader(*inputPath)
	if err != nil {
		log.Fatal(err)
	}
	defer queryReader.Close()

	worker.InitWorkers()

	ctx, cancel := context.WithCancel(context.Background())
	go worker.ResultProcessing(ctx)

	if err := queryReader.Run(worker.Send); err != nil {
		log.Fatal(err)
	}
	worker.Wait(cancel)
	if err := worker.ExportToFile(*outputPath); err != nil {
		log.Fatal(err)
	}
}
