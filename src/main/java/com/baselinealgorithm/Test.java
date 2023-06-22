package com.baselinealgorithm;

import com.baselinealgorithm.chainindexrbst.RedBlackBST;

import java.util.List;

public class Test {
    public static void main(String[] args) {
        RedBlackBST rbst= new RedBlackBST();
        for(int i=0;i<1000000;i++){
            rbst.put(i, i);
        }
        rbst.put(19,5);
        rbst.put(12,34);
        rbst.put(16,38);
        rbst.put(13,40);
        rbst.put(14,40);
        rbst.put(16, 25);

System.out.println(rbst.lessThanKey(160).size());
    }
}
