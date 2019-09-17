package btree

import (
	"os"
)

type bTreeNode struct {
	id               uint64
	elements         []*pairs
	childrenBlockIds []uint64
	bs               *bTreeBlockService
}

func (n *bTreeNode) isLeaf() bool {
	return len(n.childrenBlockIds) == 0
}

func (n *bTreeNode) addElement(element *pairs) int {
	elements := n.elements
	for i := range elements {
		if elements[i].key >= element.key {
			elements = append(elements, nil)
			copy(elements[i+1:], elements[i:])
			elements[i] = element
			n.elements = elements
			return i
		}
	}
	n.elements = append(n.elements, element)
	return len(n.elements) - 1
}

func (n *bTreeNode) hasOverFlown() bool {
	return len(n.elements) > maxLeafSize
}

func (n *bTreeNode) getChildAtIndex(index int) (*bTreeNode, error) {
	return n.bs.nodeAtBlockID(n.childrenBlockIds[index])
}

func (n *bTreeNode) shiftChildrenToRight(index int) {
	if len(n.childrenBlockIds) < index+1 {
		return
	}
	n.childrenBlockIds = append(n.childrenBlockIds, 0)
	copy(n.childrenBlockIds[index+1:], n.childrenBlockIds[index:])
	n.childrenBlockIds[index] = 0
}

func (n *bTreeNode) setChildAtIndex(index int, childNode *bTreeNode) {
	if len(n.childrenBlockIds) < index+1 {
		n.childrenBlockIds = append(n.childrenBlockIds, 0)
	}
	n.childrenBlockIds[index] = childNode.id
}

func (n *bTreeNode) lastChildNode() (*bTreeNode, error) {
	return n.getChildAtIndex(len(n.childrenBlockIds) - 1)
}

func (n *bTreeNode) childNodes() ([]*bTreeNode, error) {
	childNodes := make([]*bTreeNode, len(n.childrenBlockIds))
	for index := range n.childrenBlockIds {
		childNode, err := n.getChildAtIndex(index)
		if err != nil {
			return nil, err
		}
		childNodes[index] = childNode
	}
	return childNodes, nil
}

func (n *bTreeNode) splitLeafNode() (*pairs, *bTreeNode, *bTreeNode, error) {
	elements := n.elements
	midIndex := len(elements) / 2
	middle := elements[midIndex]

	elements1 := elements[0:midIndex]
	elements2 := elements[midIndex+1:]

	leftNode, err := newNode(elements1, n.bs, nil)
	if err != nil {
		return nil, nil, nil, err
	}
	rightNode, err := newNode(elements2, n.bs, nil)
	if err != nil {
		return nil, nil, nil, err
	}
	return middle, leftNode, rightNode, nil
}

func (n *bTreeNode) splitNonLeafNode() (*pairs, *bTreeNode, *bTreeNode, error) {
	elements := n.elements
	midIndex := len(elements) / 2
	middle := elements[midIndex]

	elements1 := elements[0:midIndex]
	elements2 := elements[midIndex+1:]

	children := n.childrenBlockIds
	children1 := children[0 : midIndex+1]
	children2 := children[midIndex+1:]

	leftNode, err := newNode(elements1, n.bs, children1)
	if err != nil {
		return nil, nil, nil, err
	}
	rightNode, err := newNode(elements2, n.bs, children2)
	if err != nil {
		return nil, nil, nil, err
	}
	return middle, leftNode, rightNode, nil
}

func (n *bTreeNode) addPoppedUpElementIntoCurrentNodeAndUpdateWithNewChildren(element *pairs, leftNode *bTreeNode, rightNode *bTreeNode) {
	insertionIndex := n.addElement(element)
	n.setChildAtIndex(insertionIndex, leftNode)

	n.shiftChildrenToRight(insertionIndex + 1)
	n.setChildAtIndex(insertionIndex+1, rightNode)
}

func newNode(elements []*pairs, bs *bTreeBlockService, childrenBlockIds []uint64) (*bTreeNode, error) {
	node := &bTreeNode{elements: elements, bs: bs, childrenBlockIds: childrenBlockIds}
	if err := bs.saveNewNodeToDisk(node); err != nil {
		return nil, err
	}
	return node, nil
}

func newRootNodeWithSingleElementAndTwoChildren(element *pairs, leftChildBlockID uint64, rightChildBlockID uint64, bs *bTreeBlockService) (*bTreeNode, error) {
	elements := []*pairs{element}
	childrenBlockIds := []uint64{leftChildBlockID, rightChildBlockID}
	node := &bTreeNode{elements: elements, childrenBlockIds: childrenBlockIds, bs: bs}
	if err := bs.updateNodeToDisk(node); err != nil {
		return nil, err
	}
	return node, nil
}

func (n *bTreeNode) getChildNodeForElement(key string) (*bTreeNode, error) {
	for i := range n.elements {
		if key < n.elements[i].key {
			return n.getChildAtIndex(i)
		}
	}

	return n.getChildAtIndex(len(n.childrenBlockIds) - 1)
}

func (n *bTreeNode) insertIfLeaf(value *pairs, bt *BTree) (*pairs, *bTreeNode, *bTreeNode, error) {
	n.addElement(value)
	if !n.hasOverFlown() {
		if err := n.bs.updateNodeToDisk(n); err != nil {
			return nil, nil, nil, err
		}
		return nil, nil, nil, nil
	}
	if bt.root == n {
		poppedMiddleElement, leftNode, rightNode, err := n.splitLeafNode()
		if err != nil {
			return nil, nil, nil, err
		}
		newRootNode, err := newRootNodeWithSingleElementAndTwoChildren(poppedMiddleElement, leftNode.id, rightNode.id, n.bs)
		if err != nil {
			return nil, nil, nil, err
		}
		bt.SetRootNode(newRootNode)
		return nil, nil, nil, nil
	}

	return n.splitLeafNode()
}

func (n *bTreeNode) insert(value *pairs, bt *BTree) (*pairs, *bTreeNode, *bTreeNode, error) {
	stack := make([]*bTreeNode, 0, 1)
	stack = append(stack, n)

	var poppedMiddleElement *pairs
	var leftNode *bTreeNode
	var rightNode *bTreeNode
	var err error
	for {
		current := stack[len(stack)-1]
		if current.isLeaf() {
			poppedMiddleElement, leftNode, rightNode, err = current.insertIfLeaf(value, bt)
			if err != nil {
				return nil, nil, nil, err
			}
			stack = stack[:len(stack)-1]
			break
		}
		childNodeToBeInserted, err := current.getChildNodeForElement(value.key)
		if err != nil {
			return nil, nil, nil, err
		}
		stack = append(stack, childNodeToBeInserted)
	}

	for len(stack) > 0 {
		current := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		if poppedMiddleElement == nil {
			continue
		}
		current.addPoppedUpElementIntoCurrentNodeAndUpdateWithNewChildren(poppedMiddleElement, leftNode, rightNode)
		if !current.hasOverFlown() {
			if err := current.bs.updateNodeToDisk(current); err != nil {
				return nil, nil, nil, err
			}
			poppedMiddleElement = nil
			leftNode = nil
			rightNode = nil
			continue
		}

		poppedMiddleElement, leftNode, rightNode, err = current.splitNonLeafNode()
		if err != nil {
			return nil, nil, nil, err
		}
		if bt.root != current {
			continue
		}
		newRootNode, err := newRootNodeWithSingleElementAndTwoChildren(poppedMiddleElement, leftNode.id, rightNode.id, current.bs)
		if err != nil {
			return nil, nil, nil, err
		}
		bt.SetRootNode(newRootNode)
	}

	return nil, nil, nil, nil
}

func (n *bTreeNode) update(key string, value uint64) (bool, error) {
	for i := range n.elements {
		if n.elements[i].key == key {
			n.elements[i].value = value
			if err := n.bs.updateNodeToDisk(n); err != nil {
				return false, nil
			}
			return true, nil
		}
	}
	return false, nil
}

func (n *bTreeNode) searchElementInNode(key string) (uint64, bool) {
	for i := range n.elements {
		if n.elements[i].key == key {
			return n.elements[i].value, true
		}
	}
	return 0, false
}

func (n *bTreeNode) search(key string) (uint64, error) {
	currentNode := n
	for {
		value, foundInCurrentNode := currentNode.searchElementInNode(key)
		if foundInCurrentNode {
			return value, nil
		}
		if currentNode.isLeaf() {
			return 0, nil
		}
		node, err := currentNode.getChildNodeForElement(key)
		if err != nil {
			return 0, err
		}
		currentNode = node
	}
}

func (n *bTreeNode) insertPair(value *pairs, bt *BTree) error {
	if _, _, _, err := n.insert(value, bt); err != nil {
		return err
	}
	return nil
}

func (n *bTreeNode) writeElements(file *os.File) {

}

func (n *bTreeNode) writeToFile(file *os.File) error {
	for _, el := range n.elements {
		if _, err := file.Write(el.toBytes()); err != nil {
			return err
		}
	}
	queue := make([]uint64, 0, 10)
	queue = append(queue, n.childrenBlockIds...)

	for len(queue) > 0 {
		blockID := queue[0]
		queue = queue[1:]
		bt, err := n.bs.nodeAtBlockID(blockID)
		if err != nil {
			return err
		}
		for _, el := range bt.elements {
			if _, err := file.Write(el.toBytes()); err != nil {
				return err
			}
		}
		queue = append(queue, bt.childrenBlockIds...)
	}
	return nil
}

func (n *bTreeNode) getValue(key string) (uint64, error) {
	return n.search(key)
}

func (n *bTreeNode) findAndUpdate(key string, value uint64) (bool, error) {
	currentNode := n
	for {
		value, foundInCurrentNode := currentNode.searchElementInNode(key)
		if foundInCurrentNode {
			return currentNode.update(key, value)
		}
		if currentNode.isLeaf() {
			return false, nil
		}
		node, err := currentNode.getChildNodeForElement(key)
		if err != nil {
			return false, err
		}
		currentNode = node
	}
}
