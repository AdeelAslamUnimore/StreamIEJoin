package com.baselinealgorithm.chainindexbplustree;

import com.stormiequality.BTree.BPlusTree;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;

public class LeftStreamPredicateBplusTree extends BaseRichBolt {
    private LinkedList<BPlusTree> duration=null;
    private LinkedList<BPlusTree> time=null;
    private int treeRemovalThreshold;
    private int treeArchiveThresholdDuration;
    private int treeArchiveThresholdTime;
    private int treeArchiveUserDefined;
    private int tupleRemovalCountForLocal;
    private OutputCollector outputCollector;
    private int bPlusTreeInitilization;
    private HashSet<Integer> hashSet;
    private String operator;
    // Constructor parameter for tuples
    public LeftStreamPredicateBplusTree(int treeArchiveUserDefined, int treeRemovalThreshold, int bPlusTreeInitilization, String operator){
        this.treeRemovalThreshold=treeRemovalThreshold;
        this.treeArchiveUserDefined=treeArchiveUserDefined;
        this.bPlusTreeInitilization=bPlusTreeInitilization;
    }
    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        duration= new LinkedList<>();
        time= new LinkedList<>();
        this.outputCollector= outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        tupleRemovalCountForLocal++;

        //Left Stream Tuple means Insert in Duration and Search in Time
        if(tuple.getSourceStreamId().equals("Left")){
            leftPredicateEvaluation(tuple);
        }
        if(tuple.getSourceStreamId().equals("Right")){
            rightPredicateEvaluation(tuple);
        }
        if(tupleRemovalCountForLocal>=treeRemovalThreshold){
            time.remove(time.getFirst());
            duration.remove(duration.getFirst());
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    public void leftPredicateEvaluation(Tuple tuple){

        if(!duration.isEmpty()){
            //New insertion only active sub index structure that always exist on the right of linkedList
            BPlusTree currentBPlusTreeDuration= duration.getLast();
            currentBPlusTreeDuration.insert(tuple.getIntegerByField("Tuple"), tuple.getIntegerByField("ID"));
            treeArchiveThresholdDuration++;
            //Archive period achieve
            if(treeArchiveThresholdDuration>=treeArchiveUserDefined){
                treeArchiveThresholdDuration=0;
                // New Object of BPlus
                BPlusTree bPlusTree= new BPlusTree(bPlusTreeInitilization);
                //Added to the linked list
                duration.add(bPlusTree);
            }
        }else
        {
            // When the linkedlist is empty:
            BPlusTree bPlusTree= new BPlusTree(bPlusTreeInitilization);
            bPlusTree.insert(tuple.getIntegerByField("Tuple"), tuple.getIntegerByField("ID"));
            duration.add(bPlusTree);
        }
        //Search of inequality condition and insertion into the hashset
        for (BPlusTree bPlusTree : time) {
            hashSet.addAll(bPlusTree.greaterThenSpecificValueHashSet(tuple.getIntegerByField("Tuple")));
            //EmitLogic tomorrow
        }



    }
    public void rightPredicateEvaluation(Tuple tuple){
        if(!time.isEmpty()){
            //Only last index for insertion
            BPlusTree currentBPlusTreeDuration= time.getLast();
            currentBPlusTreeDuration.insert(tuple.getIntegerByField("Tuple"), tuple.getIntegerByField("ID"));
            treeArchiveThresholdTime++;
            //Checking Archive period
            if(treeArchiveThresholdTime>=treeArchiveUserDefined){
                treeArchiveThresholdTime=0;
                BPlusTree bPlusTree= new BPlusTree(bPlusTreeInitilization);
                time.add(bPlusTree);
            }
        }else
        {
            BPlusTree bPlusTree= new BPlusTree(bPlusTreeInitilization);
            bPlusTree.insert(tuple.getIntegerByField("Tuple"), tuple.getIntegerByField("ID"));
            time.add(bPlusTree);
        }
        for (BPlusTree bPlusTree : duration) {
            hashSet.addAll(bPlusTree.smallerThenSpecificValueHashSet(tuple.getIntegerByField("Tuple")));
            //EmitLogic tomorrow
        }

    }
}
