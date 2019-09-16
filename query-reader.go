package main

import (
	"bufio"
	"os"
)

type QueryReader struct {
	file *os.File
}

type query struct {
	key  string
	vale uint64
}

func NewQueryReader(inPath string) (*QueryReader, error) {
	file, err := os.Open(inPath)
	if err != nil {
		return nil, err
	}

	return &QueryReader{file: file}, nil
}

func (qr *QueryReader) Close() error {
	return qr.file.Close()
}

func (qr *QueryReader) Run(send func(string)) error {
	scanner := bufio.NewScanner(qr.file)
	for scanner.Scan() {
		send(scanner.Text())
	}
	return scanner.Err()
}
