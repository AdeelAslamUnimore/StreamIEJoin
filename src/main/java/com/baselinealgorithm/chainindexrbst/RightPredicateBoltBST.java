package com.baselinealgorithm.chainindexrbst;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class RightPredicateBoltBST extends BaseRichBolt {
    private LinkedList<RedBlackBST> cost;
    private LinkedList<RedBlackBST> revenue;
    private int tupleCountForArchive=0;
    private int tupleCountForRemoval=0;
    private HashSet<Integer> hashSet=null;
    private OutputCollector outputCollector;
    public RightPredicateBoltBST(int tupleCountForArchive, int tupleCountForRemoval){
        this.tupleCountForArchive=tupleCountForArchive;
        this.tupleCountForRemoval=tupleCountForRemoval;
    }
    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        cost= new LinkedList<>();
        revenue= new LinkedList<>();
        hashSet= new HashSet<>();
        this.outputCollector=outputCollector;

    }

    @Override
    public void execute(Tuple tuple) {
        tupleCountForRemoval++;
        if(tuple.getSourceStreamId().equals("Left")) {
            if (!revenue.isEmpty()) {
                RedBlackBST treeForTupleInsertion = revenue.getLast();
                treeForTupleInsertion.put(tuple.getIntegerByField("Tuple"), tuple.getIntegerByField("ID"));
                if (treeForTupleInsertion.size() == tupleCountForArchive) {
                    RedBlackBST newRedBlackBST = new RedBlackBST();
                    revenue.add(newRedBlackBST);
                }
            } else {
                RedBlackBST redBlackBSTForRevenue = new RedBlackBST();
                //Adding tuple to the linked list when list is not empty:
                redBlackBSTForRevenue.put(tuple.getIntegerByField("Tuple"), tuple.getIntegerByField("ID"));
            }
            for (RedBlackBST redBlackBST : cost) {
                for(Node  nodeThatContainsGreater: redBlackBST.lessThanKey(tuple.getIntegerByField("tuple"))){
                    for(int id: nodeThatContainsGreater.getVals()){
                       hashSet.add(id); //HashSet
                       //Also add BitSet
                    }
                }
            }
            //Outputcollector hashSet
            }
        if (tuple.getSourceStreamId().equals("Right")) {
            if (!cost.isEmpty()) {
                RedBlackBST treeForTupleInsertion = cost.getLast();
                treeForTupleInsertion.put(tuple.getIntegerByField("Tuple"), tuple.getIntegerByField("ID"));
                if (treeForTupleInsertion.size() == tupleCountForArchive) {
                    RedBlackBST newRedBlackBST = new RedBlackBST();
                    cost.add(newRedBlackBST);
                }
            } else {
                RedBlackBST redBlackBSTForRevenue = new RedBlackBST();
                //Adding tuple to the linked list when list is not empty:
                redBlackBSTForRevenue.put(tuple.getIntegerByField("Tuple"), tuple.getIntegerByField("ID"));
            }
            for (RedBlackBST redBlackBST : revenue) {
                for(Node  nodeThatContainsGreater: redBlackBST.getNodesGreaterThan(tuple.getIntegerByField("tuple"))){
                    for(int id: nodeThatContainsGreater.getVals()){
                        hashSet.add(id); //HashSet
                        //Also add BitSet
                    }
                }

            }
        }
        if(revenue.size()==tupleCountForRemoval||cost.size()==tupleCountForRemoval){
            revenue.remove(revenue.getFirst());
            cost.remove(cost.getFirst());
        }
        //Add the data values


    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
