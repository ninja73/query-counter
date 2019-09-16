package btree

import (
	"encoding/binary"
	"errors"
	"os"
)

const blockSize = 8192
const maxLeafSize = 60

type bTreeBlock struct {
	id               uint64   //8
	currentLeafSize  uint64   //8
	currentChildSize uint64   //8
	childrenBlockIds []uint64 //8
	dataSet          []*pairs //127
}

func (b *bTreeBlock) setData(data []*pairs) {
	b.currentLeafSize = uint64(len(data))
	b.dataSet = data
}

func (b *bTreeBlock) setChildren(childrenBlockIds []uint64) {
	b.currentChildSize = uint64(len(childrenBlockIds))
	b.childrenBlockIds = childrenBlockIds
}

type bTreeBlockService struct {
	file           *os.File
	lastBlockIndex uint64
}

func (s *bTreeBlockService) blockFromBuffer(bufferBlock []byte) *bTreeBlock {
	blockOffset := 0
	block := new(bTreeBlock)
	block.id = uint64FromBytes(bufferBlock[blockOffset:])
	blockOffset += 8
	block.currentLeafSize = uint64FromBytes(bufferBlock[blockOffset:])
	blockOffset += 8
	block.currentChildSize = uint64FromBytes(bufferBlock[blockOffset:])
	blockOffset += 8
	block.childrenBlockIds = make([]uint64, block.currentChildSize)
	for i := 0; i < int(block.currentChildSize); i++ {
		block.childrenBlockIds[i] = uint64FromBytes(bufferBlock[blockOffset:])
		blockOffset += 8
	}
	block.dataSet = make([]*pairs, block.currentLeafSize)
	for i := 0; i < int(block.currentLeafSize); i++ {
		p := EmptyPairs()
		p.convertToPair(bufferBlock[blockOffset:])
		block.dataSet[i] = p
		blockOffset += pairSize
	}
	return block
}

func (s *bTreeBlockService) blockToBuffer(block *bTreeBlock) []byte {
	bufferBlock := make([]byte, blockSize)
	blockOffset := 0
	copy(bufferBlock[blockOffset:], uint64ToBytes(block.id))
	blockOffset += 8
	copy(bufferBlock[blockOffset:], uint64ToBytes(block.currentLeafSize))
	blockOffset += 8
	copy(bufferBlock[blockOffset:], uint64ToBytes(block.currentChildSize))
	blockOffset += 8
	for i := 0; i < int(block.currentChildSize); i++ {
		copy(bufferBlock[blockOffset:], uint64ToBytes(block.childrenBlockIds[i]))
		blockOffset += 8
	}
	for i := 0; i < int(block.currentLeafSize); i++ {
		copy(bufferBlock[blockOffset:], block.dataSet[i].convertToBytes())
		blockOffset += pairSize
	}
	return bufferBlock
}

func (s *bTreeBlockService) blockByIndex(index int64) (*bTreeBlock, error) {
	if index < 0 {
		return nil, errors.New("index less 0")
	}

	offset := index * blockSize
	if _, err := s.file.Seek(offset, 0); err != nil {
		return nil, err
	}

	blockBuffer := make([]byte, blockSize)
	if _, err := s.file.Read(blockBuffer); err != nil {
		return nil, err
	}

	block := s.blockFromBuffer(blockBuffer)
	return block, nil
}

func (s *bTreeBlockService) rootBlock() (*bTreeBlock, error) {
	if s.lastBlockIndex == 0 {
		return s.newBlock()
	}
	return s.blockByIndex(0)
}

func (s *bTreeBlockService) writeBlock(block *bTreeBlock) error {
	seekOffset := blockSize * block.id
	blockBuffer := s.blockToBuffer(block)
	if _, err := s.file.Seek(int64(seekOffset), 0); err != nil {
		return err
	}
	if _, err := s.file.Write(blockBuffer); err != nil {
		return err
	}
	return nil
}

func (s *bTreeBlockService) newBlock() (*bTreeBlock, error) {
	block := new(bTreeBlock)
	if s.lastBlockIndex == 0 {
		block.id = 0
	} else {
		block.id = s.lastBlockIndex + 1
	}
	if err := s.writeBlock(block); err != nil {
		return nil, err
	}
	s.lastBlockIndex += 1
	return block, nil
}

func (s *bTreeBlockService) nodeToBlock(node *bTreeNode) *bTreeBlock {
	block := new(bTreeBlock)
	block.id = node.id
	tmp := make([]*pairs, len(node.elements))
	for i, e := range node.elements {
		tmp[i] = e
	}
	block.setData(tmp)
	tmpChildren := make([]uint64, len(node.childrenBlockIds))
	for i, c := range node.childrenBlockIds {
		tmpChildren[i] = c
	}
	block.setChildren(tmpChildren)
	return block
}

func (s *bTreeBlockService) blockToNode(block *bTreeBlock) *bTreeNode {
	node := &bTreeNode{
		id:               block.id,
		elements:         make([]*pairs, block.currentLeafSize),
		childrenBlockIds: make([]uint64, block.currentChildSize),
		bs:               s,
	}
	for i := range block.dataSet {
		node.elements[i] = block.dataSet[i]
	}
	for i := range block.childrenBlockIds {
		node.childrenBlockIds[i] = block.childrenBlockIds[i]
	}
	return node
}

func (s *bTreeBlockService) nodeAtBlockID(blockID uint64) (*bTreeNode, error) {
	block, err := s.blockByIndex(int64(blockID))
	if err != nil {
		return nil, err
	}
	return s.blockToNode(block), nil
}

func (s *bTreeBlockService) saveNewNodeToDisk(node *bTreeNode) error {
	node.id = s.lastBlockIndex + 1
	if err := s.writeBlock(s.nodeToBlock(node)); err != nil {
		return err
	}
	s.lastBlockIndex += 1
	return nil
}

func (s *bTreeBlockService) updateNodeToDisk(node *bTreeNode) error {
	return s.writeBlock(s.nodeToBlock(node))
}

func uint64ToBytes(index uint64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, index)
	return b
}

func uint64FromBytes(b []byte) uint64 {
	return binary.LittleEndian.Uint64(b)
}
