package com.internodeparallelisim.iejoin;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;

public class ParallelLinkedListProcessing {
    public static void main(String[] args) {
        for(int i=1;i<10;i++)
      new ParallelLinkedListProcessing().test(i);
    }

    public synchronized void test(int num){
        LinkedList<ListNode> linkedList = new LinkedList<>();
        ListNode listNode= new ListNode(14);
        ListNode listNode1= new ListNode(15);
        ListNode listNode3= new ListNode(16);
        ListNode listNode4= new ListNode(17);
        ListNode listNode5= new ListNode(18);
        ListNode listNode6= new ListNode(19);
        ListNode listNode7= new ListNode(20);
        ListNode listNode8= new ListNode(21);
        ListNode listNode9= new ListNode(22);
        ListNode listNode10= new ListNode(23);
        linkedList.add(listNode);
        linkedList.add(listNode1);
        linkedList.add(listNode3);
        linkedList.add(listNode4);

        linkedList.add(listNode5);
        linkedList.add(listNode6);
        linkedList.add(listNode7);
        linkedList.add(listNode8);
        linkedList.add(listNode9);
        linkedList.add(listNode10);
        // Populate your linked list
        // ...
        ListNode item = linkedList.removeFirst();
        System.out.println(item.data+"=="+linkedList.size());


        int numCores = Runtime.getRuntime().availableProcessors();
        System.out.println(numCores);
        WorkerThread[] threads = new WorkerThread[numCores];
        Queue<ListNode> workQueue = new LinkedList<>(linkedList);
        CountDownLatch latch = new CountDownLatch(numCores);

        for (int i = 0; i < numCores; i++) {
            threads[i] = new WorkerThread(workQueue, latch);
            threads[i].start();
        }

        try {
            latch.await(); // Wait for all worker threads to finish
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("I am here ");

    }
}
