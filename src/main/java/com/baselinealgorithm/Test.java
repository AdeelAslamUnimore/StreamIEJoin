package com.baselinealgorithm;

import com.baselinealgorithm.chainbplusandcss.CSSTree;
import com.correctness.iejoin.CSSTreeUpdated;
import com.proposed.iejoinandbplustreebased.IEJoinModel;
import com.stormiequality.BTree.BPlusTree;

import java.util.*;

public class Test {
    private boolean num = false;
    ArrayList<IEJoinModel> list;

    public static void main(String[] args) {

        BPlusTree my_object = new BPlusTree(4);

        my_object.insert(2, 12);
        my_object.insert(4, 11);
        my_object.insert(7, 12);
        my_object.insert(10, 12);
        ;
        my_object.insert(17, 11);
        my_object.insert(21, 12);
        my_object.insert(28, 12);
        my_object.insert(30, 11);
        my_object.insert(33, 12);
        ;
        my_object.insert(35, 12);
        System.out.println(my_object.rangeSmaller(33));
        my_object.remove(28);

    }


}
