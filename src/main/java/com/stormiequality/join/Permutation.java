package com.stormiequality.join;

import java.util.List;

public class Permutation {
    int index;
    int value;
    int idsForTest;


    private List<Integer> listOfIDs;
    public Permutation(int index, int value) {
        this.index = index;
        this.value = value;
    }
    public Permutation(int Key, List<Integer> listOfIds){
        this.index=Key;
        this.listOfIDs=listOfIds;
    }
    public Permutation(int Key, int value, int idsForTest){
        this.index=Key;
        this.value=value;
        this.idsForTest=idsForTest;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public int getValue() {
        return value;
    }
    public List<Integer> getListOfIDs() {
        return listOfIDs;
    }

    public int getIdsForTest() {
        return idsForTest;
    }

    public void setIdsForTest(int idsForTest) {
        this.idsForTest = idsForTest;
    }

    public void setListOfIDs(List<Integer> listOfIDs) {
        this.listOfIDs = listOfIDs;
    }
    public void setValue(int value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "Permutation{" +
                "index=" + index +
                ", value=" + value +
                ", idsForTest=" + idsForTest +
                ", listOfIDs=" + listOfIDs +
                '}';
    }
}
