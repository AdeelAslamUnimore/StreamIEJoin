package com.proposed.iejoinandbplustreebased;

import com.stormiequality.BTree.BPlusTreeUpdated;

public class InMemoryModel {
    private BPlusTreeUpdated leftStreamBPlusTree;
    private BPlusTreeUpdated rightStreamBPlusTree;

    public BPlusTreeUpdated getLeftStreamBPlusTree() {
        return leftStreamBPlusTree;
    }

    public void setLeftStreamBPlusTree(BPlusTreeUpdated leftStreamBPlusTree) {
        this.leftStreamBPlusTree = leftStreamBPlusTree;
    }

    public BPlusTreeUpdated getRightStreamBPlusTree() {
        return rightStreamBPlusTree;
    }

    public void setRightStreamBPlusTree(BPlusTreeUpdated rightStreamBPlusTree) {
        this.rightStreamBPlusTree = rightStreamBPlusTree;
    }
}
