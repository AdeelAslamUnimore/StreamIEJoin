package com.baselinealgorithm.chainbplusandcss;

import java.util.List;

public class Key implements Comparable<Key> {


    private int key;
    private List<Integer> value;
    private int id;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }



    public int getKey() {
        return key;
    }

    public void setKey(int key) {
        this.key = key;
    }

    public List<Integer> getValue() {
        return value;
    }

    public void setValue(List<Integer> value) {
        this.value = value;
    }
    @Override
    public int compareTo(Key o) {
        return Integer.compare(this.key, o.key);
    }

    @Override
    public String toString() {
        return "Key{" +
                "key=" + key +
                ", value=" + value +
                '}';
    }
}
