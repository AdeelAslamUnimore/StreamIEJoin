package com.stormiequality.join;

import java.util.List;

public class Permutation {
    int tuple;
    int id;
    int idsForTest;


    private List<Integer> listOfIDs;
    public Permutation(int tuple){
        this.tuple =tuple;
    }
    public Permutation(int tuple, int id) {
        this.tuple = tuple;
        this.id = id;
    }
//    public Permutation(int index, List<Integer> listOfIds){
//        this.index=index;
//        this.listOfIDs=listOfIds;
//    }
    public Permutation(int value, List<Integer> listOfIds){
        this.id =value;
        this.listOfIDs=listOfIds;
    }

    public Permutation(int tuple, int value, int idsForTest){
        this.tuple =tuple;
        this.id =value;
        this.idsForTest=idsForTest;
    }

    public int getTuple() {
        return tuple;
    }

    public void setTuple(int tuple) {
        this.tuple = tuple;
    }

    public int getId() {
        return id;
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
    public void setId(int id) {
        this.id = id;
    }

    @Override
    public String toString() {
        return "Permutation{" +
                "index=" + tuple +
                ", value=" + id +
                ", idsForTest=" + idsForTest +
                ", listOfIDs=" + listOfIDs +
                '}';
    }
}
