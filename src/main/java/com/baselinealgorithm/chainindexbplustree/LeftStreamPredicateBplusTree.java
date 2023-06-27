package com.baselinealgorithm.chainindexbplustree;

import com.baselinealgorithm.chainindexrbst.Node;
import com.baselinealgorithm.chainindexrbst.RedBlackBST;
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
    private OutputCollector outputCollector;
    private int bPlusTreeInitilization;
    private HashSet<Integer> hashSet;
    public LeftStreamPredicateBplusTree(int treeRemovalThreshold, int treeArchiveUserDefined, int bPlusTreeInitilization){
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
        if(tuple.getSourceStreamId().equals("Left")){
            if(!duration.isEmpty()){
                BPlusTree currentBplusTreeDuration= duration.getLast();
                currentBplusTreeDuration.insert(tuple.getIntegerByField("Tuple"), tuple.getIntegerByField("ID"));
                treeArchiveThresholdDuration++;
                if(treeArchiveThresholdDuration==treeArchiveUserDefined){
                    treeArchiveThresholdDuration=0;
                    BPlusTree bPlusTree= new BPlusTree(bPlusTreeInitilization);
                    duration.add(bPlusTree);
                }
            }else
            {
                BPlusTree bPlusTree= new BPlusTree(bPlusTreeInitilization);
                bPlusTree.insert(tuple.getIntegerByField("Tuple"), tuple.getIntegerByField("ID"));
                duration.add(bPlusTree);
            }
            for (BPlusTree bPlusTree : time) {
                hashSet.addAll(bPlusTree.lessThenSpecificValueHash(tuple.getIntegerByField("Tuple")));
                //EmitLogic tomorrow
            }


        }
        if(tuple.getSourceStreamId().equals("Right")){
            if(!time.isEmpty()){
                BPlusTree currentBplusTreeDuration= time.getLast();
                currentBplusTreeDuration.insert(tuple.getIntegerByField("Tuple"), tuple.getIntegerByField("ID"));
                treeArchiveThresholdTime++;
                if(treeArchiveThresholdTime==treeArchiveUserDefined){
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
                hashSet.addAll(bPlusTree.lessThenSpecificValueHash(tuple.getIntegerByField("Tuple")));
                //EmitLogic tomorrow
            }

        }
        if(time.size()==treeRemovalThreshold||duration.size()==treeRemovalThreshold){
            time.remove(time.getFirst());
            duration.remove(duration.getFirst());
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
