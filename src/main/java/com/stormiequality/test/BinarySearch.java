package com.stormiequality.test;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

class BinarySearch {
    public static void main(String[] args) {
        // Create a list
        List<Domain> l = new ArrayList<Domain>();
        l.add(new Domain(10, "www.geeksforgeeks.org"));
        l.add(new Domain(20, "practice.geeksforgeeks.org"));
        l.add(new Domain(30, "code.geeksforgeeks.org"));
        l.add(new Domain(40, "www.geeksforgeeks.org"));
        l.add(new Domain(80, "code.geeksforgeeks.org"));
        l.add(new Domain(1000, "www.geeksforgeeks.org"));

        Comparator<Domain> c = new Comparator<Domain>() {
            public int compare(Domain u1, Domain u2) {
                return Integer.compare(u1.getId(), u2.getId());
            }
        };

        // Searching a domain with key value 10. To search
        // we create an object of domain with key 10.
        Domain searchKey1 = new Domain(200, null);
        int index = Collections.binarySearch(l, searchKey1, c);
        if (index >= 0) {
            System.out.println("Found at index " + index);
        } else {
            index = -(index + 1); // Get the insertion point
            if (index >= l.size()) {
                System.out.println("Key not found. No greater key exists.");
            } else {
                Domain next = l.get(index);
                System.out.println("Key not found. Nearest key greater than the search key is " + next.getId() + " at index " + index);
            }
        }

        // Searching an item with key 15
        Domain searchKey2 = new Domain(89, null);
        index = Collections.binarySearch(l, searchKey2, c);
        if (index >= 0) {
            System.out.println("Found at index " + index);
        } else {
            index = -(index + 1); // Get the insertion point
            if (index >= l.size()) {
                System.out.println("Key not found. No greater key exists."+(l.size()-1));
            } else {
                Domain next = l.get(index);
                System.out.println("Key not found. Nearest key greater than the search key is " + next.getId() + " at index " + index);
            }
        }
    }
}