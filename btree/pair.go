package btree

import (
	"encoding/binary"
	"fmt"
	"strconv"
)

const (
	pairSize     = 127
	maxKeyLength = 117
)

type pairs struct {
	keyLen uint16 //2
	key    string //117
	value  uint64 //8
}

func NewPairs(key string, value uint64) *pairs {
	pairs := new(pairs)
	pairs.setKey(key)
	pairs.setValue(value)
	return pairs
}

func EmptyPairs() *pairs {
	return new(pairs)
}

func (p *pairs) setKey(key string) {
	p.key = key
	p.keyLen = uint16(len(key))
}

func (p *pairs) setValue(value uint64) {
	p.value = value
}

func (p *pairs) validate() error {
	if len(p.key) > maxKeyLength {
		return fmt.Errorf("key max lenght 117, currently it is %d", len(p.key))
	}
	return nil
}

func (p *pairs) convertToBytes() []byte {
	bPairs := make([]byte, pairSize)
	var offset uint16 = 0
	copy(bPairs[offset:], lenToBytes(p.keyLen))
	offset += 2
	keyByte := []byte(p.key)
	copy(bPairs[offset:], keyByte[:p.keyLen])
	offset += p.keyLen
	valueByte := uint64ToBytes(p.value)
	copy(bPairs[offset:], valueByte)
	return bPairs
}

func (p *pairs) convertToPair(bPairs []byte) {
	var offset uint16 = 0
	p.keyLen = lenFromBytes(bPairs[offset:])
	offset += 2
	p.key = string(bPairs[offset : offset+p.keyLen])
	offset += p.keyLen
	p.value = uint64FromBytes(bPairs[offset:])
}

func lenFromBytes(b []byte) uint16 {
	return binary.LittleEndian.Uint16(b)
}

func lenToBytes(value uint16) []byte {
	b := make([]byte, 2)
	binary.LittleEndian.PutUint16(b, value)
	return b
}

func (p *pairs) toBytes() []byte {
	valueString := strconv.FormatUint(p.value, 10)
	valueSize := uint16(len(valueString))
	keySize := uint16(len(p.key))
	bPairs := make([]byte, valueSize+keySize+2)
	var offset uint16 = 0
	copy(bPairs[offset:], p.key)
	offset += keySize
	copy(bPairs[offset:], "\t")
	offset += 1
	copy(bPairs[offset:], valueString)
	offset += valueSize
	copy(bPairs[offset:], "\n")
	return bPairs
}
