package btree

import "os"

type BTree struct {
	root *bTreeNode
	file *os.File
}

func NewBTree(path string) (*BTree, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	ns := newBTreeNodeService(file)
	rootNode, err := ns.getRootNode()
	if err != nil {
		return nil, err
	}
	return &BTree{root: rootNode, file: file}, nil
}

func (bt *BTree) Update(key string, value uint64) (bool, error) {
	return bt.root.findAndUpdate(key, value)
}

func (bt *BTree) Export(file *os.File) error {
	return bt.root.writeToFile(file)
}

func (bt *BTree) Insert(value *pairs) error {
	return bt.root.insertPair(value, bt)
}

func (bt *BTree) Get(key string) (uint64, bool, error) {
	value, err := bt.root.getValue(key)
	if err != nil {
		return 0, false, err
	}
	if value == 0 {
		return 0, false, nil
	}
	return value, true, nil
}

func (bt *BTree) SetRootNode(n *bTreeNode) {
	bt.root = n
}

func (bt *BTree) Close() error {
	return bt.file.Close()
}
