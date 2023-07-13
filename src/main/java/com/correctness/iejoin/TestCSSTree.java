package com.correctness.iejoin;

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

    }
}
