package com.baselinealgorithm;

import com.baselinealgorithm.chainbplusandcss.CSSTree;
import com.proposed.iejoinandbplustreebased.IEJoinModel;
import com.stormiequality.BTree.BPlusTree;

import java.util.*;

public class Test {
    private boolean num = false;
    ArrayList<IEJoinModel> list;

    public static void main(String[] args) {
        BPlusTree bPlusTree= new BPlusTree(4);
        bPlusTree.insert(2,2);
        bPlusTree.insert(4,4);
        bPlusTree.insert(7, 7);
        bPlusTree.insert(10,10);
        bPlusTree.insert(17,17);
        bPlusTree.insert(21,21);
        bPlusTree.insert(28,28);
        bPlusTree.insert(30,17);
        bPlusTree.insert(33,21);
        bPlusTree.insert(35,28);
        bPlusTree.insert(39,17);
        bPlusTree.insert(40,21);
//        BPlusTree bplusTree = new BPlusTree(4);
//        CSSTree cssTree = new CSSTree(7);
//        Random random = new Random();
//
//        for (int i = 0; i < 100000; i++) {
//            int j = random.nextInt(100000);
//            bplusTree.insert(i, j);
//            cssTree.insert(i, j);
//        }
//        long time = System.currentTimeMillis();
//        for (int i = 0; i < 1000; i++) {
//            int j = random.nextInt(10000);
//            cssTree.searchGreater(j);
//        }
//        System.out.println(System.currentTimeMillis() - time);
    }
    public LinkedList<Integer> linkedList() {
        LinkedList<Integer> list = new LinkedList<>();
        list.add(0, 1);
        return list;
    }


}
