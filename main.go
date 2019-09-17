package main

import (
	"flag"
	"log"
	"query-counter/btree"
	"query-counter/lru"
)

func main() {
	var inputPath = flag.String("input", "./queries.txt", "Parser file")
	var outputPath = flag.String("output", "./result.txt", "Result file")
	var cacheSize = flag.Int("cache-size", 10000, "Cache size")
	var db = flag.String("db", "./db", "Index file")
	flag.Parse()

	bTree, err := btree.NewBTree(*db)
	if err != nil {
		log.Fatal(err)
	}
	cache := lru.NewLRU(*cacheSize)

	worker, err := NewQueryWorker(bTree, 10, cache)
	if err != nil {
		log.Fatal(err)
	}

	queryReader, err := NewQueryReader(*inputPath, worker)
	if err != nil {
		log.Fatal(err)
	}
	defer queryReader.Close()

	worker.InitWorkers()

	go worker.ResultProcessing()

	if err := queryReader.Run(); err != nil {
		log.Fatal(err)
	}
	worker.Wait()

	if err := worker.ExportToFile(*outputPath); err != nil {
		log.Fatal(err)
	}
	log.Println("Query counter done")
}
