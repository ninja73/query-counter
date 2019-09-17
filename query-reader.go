package main

import (
	"bufio"
	"os"
)

type QueryReader struct {
	file   *os.File
	worker *QueryWorker
}

type query struct {
	key  string
	vale uint64
}

func NewQueryReader(inPath string, worker *QueryWorker) (*QueryReader, error) {
	file, err := os.Open(inPath)
	if err != nil {
		return nil, err
	}

	return &QueryReader{file: file, worker: worker}, nil
}

func (qr *QueryReader) Close() error {
	return qr.file.Close()
}

func (qr *QueryReader) Run() error {
	scanner := bufio.NewScanner(qr.file)
	for scanner.Scan() {
		qr.worker.Send(scanner.Text())
	}
	qr.worker.Close()

	return scanner.Err()
}
