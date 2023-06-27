package com.baselinealgorithm.chainbplusandcss;

import java.util.List;

public class Node {
    private int nodeID;
    private Node next;
    private Node prev;
    private Block blockPointer;
    private boolean isleaf;
    private int childBlockIndex;
    private List<Key> keys;


    public int getChildBlockIndex() {
        return childBlockIndex;
    }

    public void setChildBlockIndex(int childBlockIndex) {
        this.childBlockIndex = childBlockIndex;
    }

    public boolean isIsleaf() {
        return isleaf;
    }

    public void setIsleaf(boolean isleaf) {
        this.isleaf = isleaf;
    }

    public Node getNext() {
        return next;
    }

    public void setNext(Node next) {
        this.next = next;
    }

    public Node getPrev() {
        return prev;
    }

    public void setPrev(Node prev) {
        this.prev = prev;
    }

    public Block getBlockPointer() {
        return blockPointer;
    }

    public void setBlockPointer(Block blockPointer) {
        this.blockPointer = blockPointer;
    }
    public List<Key> getKeys() {
        return keys;
    }

    public void setKeys(List<Key> keys) {
        this.keys = keys;
    }

    public int getNodeID() {
        return nodeID;
    }

    public void setNodeID(int nodeID) {
        this.nodeID = nodeID;
    }

    @Override
    public String toString() {
        return "Node{" +
                ", keys=" + keys +
                '}';
    }
}
