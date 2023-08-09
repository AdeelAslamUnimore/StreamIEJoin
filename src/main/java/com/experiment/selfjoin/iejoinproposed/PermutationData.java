package com.experiment.selfjoin.iejoinproposed;

import com.stormiequality.join.Permutation;

import java.util.ArrayList;

public class PermutationData {
    private ArrayList<Permutation> listLeftPermutation;
    private ArrayList<Permutation> listRightPermutation;

    public long getMergingTimeForEndTuple() {
        return mergingTimeForEndTuple;
    }

    public void setMergingTimeForEndTuple(long mergingTimeForEndTuple) {
        this.mergingTimeForEndTuple = mergingTimeForEndTuple;
    }

    private long mergingTimeForEndTuple;

    public ArrayList<Permutation> getListLeftPermutation() {
        return listLeftPermutation;
    }

    public void setListLeftPermutation(ArrayList<Permutation> listLeftPermutation) {
        this.listLeftPermutation = listLeftPermutation;
    }

    public ArrayList<Permutation> getListRightPermutation() {
        return listRightPermutation;
    }

    public void setListRightPermutation(ArrayList<Permutation> listRightPermutation) {
        this.listRightPermutation = listRightPermutation;
    }
}
