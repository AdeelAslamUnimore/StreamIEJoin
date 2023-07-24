package com.testcomparatorclasses.ieJoin;

import com.correctness.iejoin.West;

import java.util.Comparator;

public class Cost implements Comparator<West> {

    public int compare(West s1, West s2) {
        if (s1.getCost() == s2.getCost())
            return 0;
        else if (s1.getCost() > s2.getCost())
            return 1;
        else
            return -1;
    }
    }

