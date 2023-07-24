package com.testcomparatorclasses.ieJoin;

import com.correctness.iejoin.East;

import java.util.Comparator;

public class Duration implements Comparator<East> {

    public int compare(East s1, East s2) {
        if (s1.getDuration() == s2.getDuration())
            return 0;
        else if (s1.getDuration() > s2.getDuration())
            return 1;
        else
            return -1;
    }
    }

