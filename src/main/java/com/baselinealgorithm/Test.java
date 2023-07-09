package com.baselinealgorithm;

import com.proposed.iejoinandbplustreebased.IEJoinModel;
import com.stormiequality.BTree.BPlusTree;

import java.util.*;

public class Test {
    private boolean num= false;
    ArrayList<IEJoinModel> list;
    public static void main(String[] args) {
       LinkedList<Integer> list= new Test().linkedList();
      // list.add(0,2);
       System.out.println(list.getFirst());


    }

    public LinkedList<Integer> linkedList(){
        LinkedList<Integer> list= new LinkedList<>();
        list.add(0,1);
        return list;
    }


}
