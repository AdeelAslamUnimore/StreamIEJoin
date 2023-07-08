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

public class RightStreamPredicateBplusTree extends BaseRichBolt {
    private LinkedList<BPlusTree> revenue=null;
    private LinkedList<BPlusTree> cost=null;
    private int treeRemovalThreshold;
    private int treeArchiveThresholdRevenue;
    private int treeArchiveThresholdCost;
    private int treeArchiveUserDefined;
    private int tupleRemovalCountForLocal;
    private OutputCollector outputCollector;
    private int bPlusTreeInitilization;
    public RightStreamPredicateBplusTree(int treeArchiveUserDefined, int treeRemovalThreshold, int bPlusTreeInitilization){
        this.treeRemovalThreshold=treeRemovalThreshold;
        this.treeArchiveUserDefined=treeArchiveUserDefined;
        this.bPlusTreeInitilization=bPlusTreeInitilization;
    }
    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        revenue= new LinkedList<>();
        cost= new LinkedList<>();
        this.outputCollector= outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        tupleRemovalCountForLocal++;
        if(tuple.getSourceStreamId().equals("Left")){
            if(!revenue.isEmpty()){
                BPlusTree currentBplusTreeDuration= revenue.getLast();
                currentBplusTreeDuration.insert(tuple.getIntegerByField("Tuple"), tuple.getIntegerByField("ID"));
                treeArchiveThresholdRevenue++;
                if(treeArchiveThresholdRevenue>=treeArchiveUserDefined){
                    treeArchiveThresholdRevenue=0;
                    BPlusTree bPlusTree= new BPlusTree(bPlusTreeInitilization);
                    revenue.add(bPlusTree);
                }
            }else
            {
                BPlusTree bPlusTree= new BPlusTree(bPlusTreeInitilization);
                bPlusTree.insert(tuple.getIntegerByField("Tuple"), tuple.getIntegerByField("ID"));
                revenue.add(bPlusTree);
            }
            for (BPlusTree bPlusTree : cost) {
               HashSet<Integer> hashSetGreater=bPlusTree.greaterThenSpecificValueHashSet(tuple.getIntegerByField("Tuple"));
                //EmitLogic tomorrow
            }


        }
        if(tuple.getSourceStreamId().equals("Right")){
            if(!cost.isEmpty()){
                BPlusTree currentBplusTreeDuration= cost.getLast();
                currentBplusTreeDuration.insert(tuple.getIntegerByField("Tuple"), tuple.getIntegerByField("ID"));
                treeArchiveThresholdCost++;
                if(treeArchiveThresholdCost>=treeArchiveUserDefined){
                    treeArchiveThresholdCost=0;
                    BPlusTree bPlusTree= new BPlusTree(bPlusTreeInitilization);
                    cost.add(bPlusTree);
                }
            }else
            {
                BPlusTree bPlusTree= new BPlusTree(bPlusTreeInitilization);
                bPlusTree.insert(tuple.getIntegerByField("Tuple"), tuple.getIntegerByField("ID"));
                cost.add(bPlusTree);
            }
            for (BPlusTree bPlusTree : revenue) {
                HashSet<Integer> hashSetsLess=bPlusTree.smallerThenSpecificValueHashSet(tuple.getIntegerByField("Tuple"));
                //EmitLogic tomorrow
            }

        }
        if(tupleRemovalCountForLocal>=treeRemovalThreshold){
            cost.remove(cost.getFirst());
            revenue.remove(revenue.getFirst());
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
