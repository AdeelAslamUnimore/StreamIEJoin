package com.internodeparallelisim.iejoin;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;

public class WorkerThread extends Thread{
    private LinkedList<ListNode> linkedList;

    private CountDownLatch latch;
    private Lock listLock;

    public WorkerThread(LinkedList<ListNode> linkedList, CountDownLatch latch, Lock listLock) {
        this.linkedList = linkedList;
        this.latch = latch;
        this.listLock=listLock;
    }

    @Override
    public void run() {
        while (true) {
            ListNode item;
            listLock.lock();
            try {
                if (linkedList.isEmpty()) {
                    // No more work to do
                    break;
                }
                item = linkedList.removeFirst();
            } finally {
                listLock.unlock();
            }


//            synchronized (workQueue) {
//                item = workQueue.poll();
//
//                if (item == null) {
//                    // No more work to do
//
//                    break;
//                }
//             //   break;
//
//            }
            processItem(item);
        }

        latch.countDown(); // Notify that this thread has finished
    }

    private void processItem(ListNode item) {
        // Your processing logic for a single item
        String threadName = Thread.currentThread().getName();
        System.out.println(threadName+"=="+item.data+"=="+System.currentTimeMillis());

        // ...
    }}