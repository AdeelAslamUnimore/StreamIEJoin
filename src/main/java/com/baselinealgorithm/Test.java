package com.baselinealgorithm;

import com.baselinealgorithm.chainbplusandcss.CSSTree;
import com.stormiequality.BTree.BPlusTree;

import java.util.*;

public class Test {
    public static void main(String[] args) {
        BPlusTree my_object= new BPlusTree(4);
        my_object.insert(7,12);
        my_object.insert(8,11);
        my_object.insert(9,12);
        my_object.insert(17,12);
        my_object.insert(12,11);
        my_object.insert(13,12);
        my_object.insert(14,12);
        my_object.insert(15,11);
        my_object.insert(16,12);

        my_object.insert(144,12);
        my_object.insert(153,11);
        my_object.insert(162,12);
        my_object.insert(173,12);
        my_object.insert(14,13);
        my_object.insert(15,16);
        my_object.insert(29,19);
        my_object.insert(30,20);
        my_object.insert(40, 60);
        my_object.insert(50, 80);
        my_object.insert(60, 100);
        my_object.insert(80, 150);
        my_object.insert(90,169);
        my_object.insert(100,170);
        my_object.insert(400, 60);
        my_object.insert(500, 80);
        my_object.insert(600, 100);
        my_object.insert(700,170);
        my_object.insert(800, 60);
        my_object.insert(1000, 80);
        my_object.insert(1200, 100);
        my_object.insert(1600, 100);;
        my_object.insert(1700,170);
        my_object.insert(1800, 60);
        my_object.insert(19000, 80);;;
        my_object.insert(2200, 100);
//        bPlusTree.insert(2,2);;;
//        bPlusTree.insert(3,5);
//        bPlusTree.insert(4,6);
//        bPlusTree.insert(5,7);
//        bPlusTree.insert(6,2);
//        bPlusTree.insert(7,5);
//        bPlusTree.insert(8,6);
//        bPlusTree.insert(9,7);
//        bPlusTree.insert(21,2);
//        bPlusTree.insert(33,5);
//        bPlusTree.insert(43,6);
//        bPlusTree.insert(53,7);
//        bPlusTree.insert(64,2);
//        bPlusTree.insert(72,5);
//        bPlusTree.insert(82,6);
//        bPlusTree.insert(92,7);


      //  System.out.println(bPlusTree.leftMostNode());
        my_object.remove(400);
        System.out.println(my_object.search(400));

    }

}
