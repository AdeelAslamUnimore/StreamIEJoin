package com.testcomparatorclasses.ieJoin;

import com.correctness.iejoin.East;

import java.util.Comparator;

public class Rev implements Comparator<East> {

    public int compare(East s1, East s2) {
        if (s1.getRevenue() == s2.getRevenue())
            return 0;
        else if (s1.getRevenue() > s2.getRevenue())
            return 1;
        else
            return -1;
    }
    }

