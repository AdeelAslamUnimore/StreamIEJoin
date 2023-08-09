package com.experiment.selfjoin.iejoinproposed;

public class PermutationSelfJoin {
    private int index;
    private int leftStreamValue;
    private int rightStreamValue;

    public PermutationSelfJoin(int index,int rightStreamValue, int leftStreamValue ) {
        this.index = index;
        this.leftStreamValue = leftStreamValue;
        this.rightStreamValue = rightStreamValue;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public int getLeftStreamValue() {
        return leftStreamValue;
    }

    public void setLeftStreamValue(int leftStreamValue) {
        this.leftStreamValue = leftStreamValue;
    }

    public int getRightStreamValue() {
        return rightStreamValue;
    }

    public void setRightStreamValue(int rightStreamValue) {
        this.rightStreamValue = rightStreamValue;
    }

    @Override
    public String toString() {
        return "PermutationSelfJoin{" +
                "index=" + index +
                ", leftStreamValue=" + leftStreamValue +
                ", rightStreamValue=" + rightStreamValue +
                '}';
    }
}
