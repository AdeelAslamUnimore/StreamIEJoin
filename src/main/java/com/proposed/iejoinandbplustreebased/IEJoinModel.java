package com.proposed.iejoinandbplustreebased;

import com.stormiequality.BTree.Offset;
import com.stormiequality.join.Permutation;

import java.util.ArrayList;

public class IEJoinModel {
    private ArrayList<Permutation> leftStreamPermutation;
    private ArrayList<Permutation> rightStreamPermutation;
    private ArrayList<Offset> leftStreamOffset;
    private ArrayList<Offset> rightStreamOffset;
    private int  tupleRemovalCounter;

    public ArrayList<Permutation> getLeftStreamPermutation() {
        return leftStreamPermutation;
    }

    public void setLeftStreamPermutation(ArrayList<Permutation> leftStreamPermutation) {
        this.leftStreamPermutation = leftStreamPermutation;
    }

    public ArrayList<Permutation> getRightStreamPermutation() {
        return rightStreamPermutation;
    }

    public void setRightStreamPermutation(ArrayList<Permutation> rightStreamPermutation) {
        this.rightStreamPermutation = rightStreamPermutation;
    }

    public ArrayList<Offset> getLeftStreamOffset() {
        return leftStreamOffset;
    }

    public void setLeftStreamOffset(ArrayList<Offset> leftStreamOffset) {
        this.leftStreamOffset = leftStreamOffset;
    }

    public ArrayList<Offset> getRightStreamOffset() {
        return rightStreamOffset;
    }

    public void setRightStreamOffset(ArrayList<Offset> rightStreamOffset) {
        this.rightStreamOffset = rightStreamOffset;
    }

    public int getTupleRemovalCounter() {
        return tupleRemovalCounter;
    }

    public void setTupleRemovalCounter(int tupleRemovalCounter) {
        this.tupleRemovalCounter = tupleRemovalCounter;
    }
}
