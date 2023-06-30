package com.stormiequality.BTree;

import java.util.BitSet;

/*
Offset is the component for IE Join,
It is an array for finding relative positive of one data array to another
the offset Array can be defined as the array
L1=[5,9,12] and L2=[5,6,10,11]
The offset Array of L1 w.r.t to L2 [1,3,5]
key is the L1 data element and index is the position
 */
public class Offset {
    private int key;
    private int index;
    private BitSet bitSet;
    private int size;
    public Offset(int key, int index) {
        this.key = key;
        this.index = index;
    }

    public Offset(int key, int index, BitSet bitSet) {
        this.key = key;
        this.index = index;
        this.bitSet=bitSet;

    }
    public Offset(int key, int index, BitSet bitSet, int size) {
        this.key = key;
        this.index = index;
        this.bitSet=bitSet;
        this.size=size;

    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public int getKey() {
        return key;
    }

    public void setKey(int key) {
        this.key = key;
    }

    public int getIndex() {
        return index;
    }

    public BitSet getBitSet() {
        return bitSet;
    }

    public void setBitSet(BitSet bitSet) {
        this.bitSet = bitSet;
    }
    public Integer getKeyForSearch() {
        return Integer.valueOf(key);
    }

    @Override
    public String toString() {
        return "Offset{" +
                "key=" + key +
                ", index=" + index +
                ", bitSet=" + bitSet +
                '}';
    }
}
