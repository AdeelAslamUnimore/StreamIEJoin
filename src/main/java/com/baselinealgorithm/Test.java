package com.baselinealgorithm;

import com.baselinealgorithm.chainbplusandcss.CSSTree;

import java.util.Random;

public class Test {
    public static void main(String[] args) {
        CSSTree cssTree = new CSSTree(10);
        Random random = new Random();
        //    for(int i=0;i<20;i++){
        cssTree.insert(14, 1);
        cssTree.insert(11, 2);
        cssTree.insert(8, 3);
        cssTree.insert(8, 4);
        cssTree.insert(16, 5);
        cssTree.insert(1, 6);
        cssTree.insert(1, 7);
        cssTree.insert(8, 8);
        cssTree.insert(1, 9);
        cssTree.insert(1, 10);
        cssTree.insert(6, 11);
        cssTree.insert(6, 12);
        cssTree.insert(18, 13);
        cssTree.insert(15, 14);
        cssTree.insert(14, 15);
        cssTree.insert(14, 16);
        cssTree.insert(15, 17);
        cssTree.insert(15, 18);
        cssTree.insert(10, 19);
        cssTree.insert(10, 20);
        cssTree.insert(10, 20);
        cssTree.insert(25, 17);
      cssTree.insert(100, 18);
       cssTree.insert(167, 19);
        cssTree.insert(161, 20);
       cssTree.insert(170, 23);
      cssTree.insert(172, 24);
      cssTree.insert(173, 25);
       cssTree.insert(190, 30);
       cssTree.insert(200, 40);
       cssTree.insert(300, 60);
      cssTree.insert(312, 24);
     cssTree.insert(145, 25);
      cssTree.insert(309, 30);
     cssTree.insert(155, 40);
        cssTree.insert(189, 60);
//
        cssTree.insert(350, 25);
      cssTree.insert(134, 30);
       cssTree.insert(343, 40);
       cssTree.insert(320, 60);
      cssTree.insert(432, 24);
      cssTree.insert(410, 25);
      cssTree.insert(399, 30);
       cssTree.insert(299, 40);
     cssTree.insert(1199, 60);
//
//
        cssTree.insert(550, 25);
      cssTree.insert(534, 30);
       cssTree.insert(1600, 40);
        cssTree.insert(1200, 60);
        cssTree.insert(4320, 24);
       cssTree.insert(4100, 25);
       cssTree.insert(2009, 30);
        cssTree.insert(2912, 40);
        cssTree.insert(2600, 60);
//
        cssTree.insert(770, 25);
      cssTree.insert(785, 30);
       cssTree.insert(1670, 40);
        cssTree.insert(1284, 60);
       cssTree.insert(4389, 24);
       cssTree.insert(4188, 25);
       cssTree.insert(9000, 30);
      cssTree.insert(4500, 40);
        cssTree.insert(4600, 60);
        cssTree.insert(14, 1);
//        cssTree.insert(11, 2);
//        cssTree.insert(8, 3);
//        cssTree.insert(8,4);
//        cssTree.insert(16, 5);
//        cssTree.insert(1, 6);
//        cssTree.insert(1, 7);
//        cssTree.insert(8,8);
//        cssTree.insert(1, 9);
//        cssTree.insert(1, 10);
//        cssTree.insert(6, 11);
//        cssTree.insert(6,12);
//        cssTree.insert(18, 13);
//        cssTree.insert(15, 14);
//        cssTree.insert(14, 15);
//        cssTree.insert(14,16);
//        cssTree.insert(15, 17);
//        cssTree.insert(15, 18);
//        cssTree.insert(10, 19);
//        cssTree.insert(10,20);
//        cssTree.insert(10,20);
//        cssTree.insert(25, 17);
//        cssTree.insert(100, 18);

        Random random1= new Random();
        for(int i=1;i<1000;i++){


        cssTree.insert(random.nextInt(20000),i);
    }
     cssTree.searchSmaller( 2000);
    }

}
