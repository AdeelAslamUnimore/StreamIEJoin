package com.correctness.iejoin;

import java.util.Random;

public class TestCSSTree {
    public static void main(String[] args) {
        CSSTreeUpdated cssTreeUpdated= new CSSTreeUpdated(4);
        cssTreeUpdated.insert(2,2);
        cssTreeUpdated.insert(4,4);
        cssTreeUpdated.insert(7,7);
        cssTreeUpdated.insert(10,10);
        cssTreeUpdated.insert(17,17);
        cssTreeUpdated.insert(21,21);
       cssTreeUpdated.insert(28,21);
        cssTreeUpdated.insert(30,10);
        cssTreeUpdated.insert(33,17);
        cssTreeUpdated.insert(35,21);
        cssTreeUpdated.insert(39,21);
        cssTreeUpdated.insert(40,21);
        Random random = new Random();
        for(int i=0;i< 300000;i++){
           int index= random.nextInt(25000);
           cssTreeUpdated.insert(index, i);
        }

        cssTreeUpdated.searchGreaterBitSet(257);

    }
}
