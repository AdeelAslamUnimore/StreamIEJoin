package com.stormiequality.inputdata;

import java.util.*;

class BinarySearch {

    public static void main(String[] args) {
        boolean number=false;
      new BinarySearch().testBoolean(new boolean[]{number});
       System.out.println(number);
    }
    public static  void testBoolean(boolean[] flagWrapper ){
        flagWrapper[0] = true;
    }

    public static void computingNumber(){
        List<TestingDomain> list = new ArrayList<>();
        list.add(new TestingDomain(10));
        list.add(new TestingDomain(20));
        list.add(new TestingDomain(20));
        list.add(new TestingDomain(30));
        list.add(new TestingDomain(40));
        list.add(new TestingDomain(50));
        list.add(new TestingDomain(50));
        list.add(new TestingDomain(60));

        TestingDomain target = new TestingDomain(50);
//////////// This is for Normal
        Comparator<TestingDomain> comparator = new Comparator<TestingDomain>() {
            public int compare(TestingDomain obj1, TestingDomain obj2) {
                return Integer.compare(obj1.getId(), obj2.getId());
            }
        };

        int low = 0;
        int high = list.size() - 1;
        int result = -1;

        while (low <= high) {
            int mid = low + (high - low) / 2;

            if (comparator.compare(list.get(mid), target) >= 0) {
                result = mid;
                high = mid - 1;
            } else {
                low = mid + 1;
            }
        }

        if (result != -1) {
            System.out.println("First greater or equal object index: " + result);
        } else {
            System.out.println("No greater or equal object found.");
        }
    }
    public static void findNumber(){
        //// THis is for not West Stream case
        List<TestingDomain> l = new ArrayList<TestingDomain>();
        l.add(new TestingDomain(10, "www.geeksforgeeks.org"));
        l.add(new TestingDomain(20, "practice.geeksforgeeks.org"));
        l.add(new TestingDomain(20, "code.geeksforgeeks.org"));
        l.add(new TestingDomain(20, "www.geeksforgeeks.org"));
        l.add(new TestingDomain(80, "code.geeksforgeeks.org"));
        l.add(new TestingDomain(1000, "www.geeksforgeeks.org"));
        TestingDomain offset= new TestingDomain(20,"iii");
        Comparator<TestingDomain> comparator = new Comparator<TestingDomain>() {
            public int compare(TestingDomain u1, TestingDomain u2) {
                return Integer.compare(u1.getId(), u2.getId());
            }
        };
        int result = -1;

        int low = 0;
        int high = l.size() - 1;

        while (low <= high) {
            int mid = low + (high - low) / 2;

            if (comparator.compare(l.get(mid), offset) > 0) {
                result = mid;
                high = mid - 1;
            } else {
                low = mid + 1;
            }
        }

        if (result != -1) {
            System.out.println("First greatest object index: " + result);
        } else {
            System.out.println("No greater object found.");
        }
    }
}