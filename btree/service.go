package btree

import "os"

type bTreeNodeService struct {
	file *os.File
}

func newBTreeNodeService(file *os.File) *bTreeNodeService {
	return &bTreeNodeService{file: file}
}

func (ns *bTreeNodeService) getRootNode() (*bTreeNode, error) {
	bs := &bTreeBlockService{file: ns.file, lastBlockIndex: 0}
	rootBlock, err := bs.rootBlock()
	if err != nil {
		return nil, err
	}
	return bs.blockToNode(rootBlock), nil
}
