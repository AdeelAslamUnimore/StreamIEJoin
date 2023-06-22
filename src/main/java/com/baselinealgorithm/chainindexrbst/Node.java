package com.baselinealgorithm.chainindexrbst;

import java.util.List;

import static com.google.common.collect.Lists.newLinkedList;

public class Node {
    public int key; // key
    public List<Integer> vals; // associated data
    public Node left, right; // links to left and right subtrees
    public boolean color; // color of parent link
    public int size; // subtree count

    public Node(int key, int val, boolean color, int size) {
        this.key = key;
        vals = newLinkedList();
        vals.add(val);
        this.color = color;
        this.size = size;
    }

    public int getKey() {
        return key;
    }

    public void setKey(int key) {
        this.key = key;
    }

    public List<Integer> getVals() {
        return vals;
    }

    public void setVals(List<Integer> vals) {
        this.vals = vals;
    }

    public Node getLeft() {
        return left;
    }

    public void setLeft(Node left) {
        this.left = left;
    }

    public Node getRight() {
        return right;
    }

    public void setRight(Node right) {
        this.right = right;
    }

    public boolean isColor() {
        return color;
    }

    public void setColor(boolean color) {
        this.color = color;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    @Override
    public String toString() {
        return "Node{" +
                "key=" + key +
                ", vals=" + vals +
                '}';
    }
}
