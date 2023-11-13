package com.internodeparallelisim.iejoin;

import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;

public class WorkerThread extends Thread{
    private Queue<ListNode> workQueue;

    private CountDownLatch latch;
    private int num;


    public WorkerThread(Queue<ListNode> workQueue, CountDownLatch latch) {
        this.workQueue = workQueue;
        this.latch = latch;
        this.num= num;

    }

    @Override
    public void run() {
        while (true) {
            ListNode item;


            synchronized (workQueue) {
                item = workQueue.poll();

                if (item == null) {
                    // No more work to do

                    break;
                }
             //   break;

            }

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