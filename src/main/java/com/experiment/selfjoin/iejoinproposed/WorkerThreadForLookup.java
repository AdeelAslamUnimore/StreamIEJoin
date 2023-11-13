package com.experiment.selfjoin.iejoinproposed;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;

public class WorkerThreadForLookup extends Thread {
    private Tuple tuple;
    private OutputCollector outputCollector;
    private  LinkedList<ArrayList<PermutationSelfJoin>> linkedListPermutationSelfJoin;
    private int taskID;
    private String hostName;
    private Lock lock;
    private AtomicInteger currentIndex;
    private CountDownLatch latch;
    private int tupleRemovalCounter;
    private BufferedWriter writer;
    private StringBuilder builder;
    private int counter;

    // Constructor to initialize the worker thread
    public WorkerThreadForLookup(Tuple tuple, OutputCollector collector, LinkedList<ArrayList<PermutationSelfJoin>> linkedListPermutationSelfJoin, CountDownLatch latch, Lock lock, AtomicInteger lockCounter, int tupleRemovalCounter, int taskID, String hostName, BufferedWriter writer) {
        this.tuple = tuple;
        this.outputCollector = collector;
        this.linkedListPermutationSelfJoin = linkedListPermutationSelfJoin;
        this.lock= lock;
        this.latch=latch;
        this.taskID = taskID;
        this.tupleRemovalCounter=tupleRemovalCounter;
        this.hostName = hostName;
        this.currentIndex=lockCounter;
        try{
          this.writer= writer;
//            this.builder= builder;
            this.counter=0;
        }catch (Exception e){

        }
    }

    // Method for performing result computation
    public void resultComputation() {
        long kafkaReadingTime = tuple.getLongByField("kafkaTime");
        long KafkaSpoutTime = tuple.getLongByField("Time");
        int tupleID = tuple.getIntegerByField("ID");
        long tupleArrivalTime = System.currentTimeMillis();

        // Call bitSetEvaluation method to perform bitset operations
//        bitSetEvaluation(permutationSelfJoins, tuple);
//
//        long tupleEvaluationTime = System.currentTimeMillis();
//
//        // Emit the processed tuple with values
//        outputCollector.emit("ieJoinResult", tuple, new Values(tupleID, kafkaReadingTime, KafkaSpoutTime, tupleArrivalTime, tupleEvaluationTime, taskID, hostName));
//        outputCollector.ack(tuple);
    }

    // Method for performing BitSet evaluation
    public void bitSetEvaluation(ArrayList<PermutationSelfJoin> listPermutationSelfJoin, Tuple tuple) {
        BitSet bitSet = new BitSet(listPermutationSelfJoin.size());

        // Perform binary search to find the index with right stream value
        int indexWithRight = binarySearchForIndexWithFlag(listPermutationSelfJoin, tuple.getInteger(1));

        // Perform binary search to find the index with left stream value
        int indexWithLeft = binarySearchWithIndex(listPermutationSelfJoin, tuple.getInteger(0));

        try {
            if (indexWithRight > -1) {
                if (indexWithRight < listPermutationSelfJoin.size()) {
                    // Set bits in BitSet for selected indices
                    for (int i = indexWithRight; i < listPermutationSelfJoin.size(); i++) {
                        bitSet.set(listPermutationSelfJoin.get(i).getIndex(), true);
                    }
                }
            }
        } catch (ArrayIndexOutOfBoundsException e) {
            // Handle any exceptions here if needed
        }

        int count = 0;
        for (int i = indexWithLeft - 1; i >= 0; i--) {
            if (bitSet.get(i)) {
                count++;
            }
        }
    }

    // Binary search for finding the index with right stream value
    public int binarySearchForIndexWithFlag(ArrayList<PermutationSelfJoin> arr, int target) {
        int left = 0;
        int right = arr.size() - 1;
        int result = -1; // Initialize the result variable to keep track of the next greatest index

        while (left <= right) {
            int mid = left + (right - left) / 2;
            int midVal = arr.get(mid).getRightStreamValue();

            if (midVal == target) {
                return mid + 1; // Target found, return its index
            } else if (midVal < target) {
                result = mid; // Update the result to the current index before moving to the right
                left = mid + 1;
            } else {
                right = mid - 1;
            }
        }

        return result;
    }

    // Binary search for finding the index with left stream value
    public int binarySearchWithIndex(ArrayList<PermutationSelfJoin> arr, int target) {
        int left = 0;
        int right = arr.size() - 1;
        int result = -1; // Initialize the result variable to keep track of the next greatest index

        while (left <= right) {
            int mid = left + (right - left) / 2;
            int midVal = arr.get(mid).getLeftStreamValue();

            if (midVal == target) {
                return mid; // Target found, return its index
            } else if (midVal < target) {
                result = mid; // Update the result to the current index before moving to the right
                left = mid + 1;
            } else {
                right = mid - 1;
            }
        }

        // If the target is not found, return the index of the nearest element
        return result + 1;
    }

    @Override
    public void run() {
        long kafkaReadingTime = tuple.getLongByField("kafkaTime");
        long KafkaSpoutTime = tuple.getLongByField("Time");
        int tupleID = tuple.getIntegerByField("ID");
        long tupleArrivalTime = System.currentTimeMillis();
        while (true) {
            ArrayList<PermutationSelfJoin> permutationArray = null;
            lock.lock();
            try {
                if (currentIndex.get() < linkedListPermutationSelfJoin.size()) {
                    permutationArray = linkedListPermutationSelfJoin.get(currentIndex.get());
                   currentIndex.getAndIncrement(); // Move to the next index
                } else {
                    // No more work to do
                    break;
                }
            } finally {
                lock.unlock();
            }

            if (permutationArray != null) {
                bitSetEvaluation(permutationArray, tuple);
                long tupleEvaluationTime = System.currentTimeMillis();
                lock.lock();
                try {
//                  this.counter=  counter+1;
//                    this.builder.append(tupleID+","+ kafkaReadingTime+","+ KafkaSpoutTime+","+tupleArrivalTime+","+ tupleEvaluationTime+","+ tupleRemovalCounter+","+ taskID+","+hostName+","+ Thread.currentThread().getName()+"\n");
//
//                   if(this.counter==1000){
//
//                       this.writer.write(builder.toString());
//                       this.writer.flush();
//                       this.builder= new StringBuilder();
//                       this.counter=0;
//                   }
                    this.writer.write(tupleID+","+ kafkaReadingTime+","+ KafkaSpoutTime+","+tupleArrivalTime+","+ tupleEvaluationTime+","+ tupleRemovalCounter+","+ taskID+","+hostName+","+ Thread.currentThread().getName()+"\n");
                    this.writer.flush();
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
                finally {
                    lock.unlock();
                }
//                        findIndex(linkedListPermutationSelfJoin, permutationArray));
                // Emit the processed tuple with values
//                outputCollector.emit("ieJoinResult", tuple, new Values(tupleID, kafkaReadingTime, KafkaSpoutTime, tupleArrivalTime, tupleEvaluationTime, tupleRemovalCounter, taskID, hostName, Thread.currentThread().getName(),
//                        findIndex(linkedListPermutationSelfJoin, permutationArray)));
//              outputCollector.ack(tuple);
            }
        }
        latch.countDown(); //

    }
    public static <T> int findIndex(LinkedList<T> list, T target) {
        int index = 0;
        for (T item : list) {
            if (item.equals(target)) {
                return index;
            }
            index++;
        }
        return -1; // Element not found in the list
    }
}
