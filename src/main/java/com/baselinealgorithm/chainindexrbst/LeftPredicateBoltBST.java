package com.baselinealgorithm.chainindexrbst;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;

public class LeftPredicateBoltBST extends BaseRichBolt {
    private LinkedList<RedBlackBST> duration;
    private LinkedList<RedBlackBST> time;
    int tupleCountForArchive=0;
    int tupleCountForRemoval=0;
    private OutputCollector outputCollector;
    private HashSet<Integer> hashSet=null;
    public LeftPredicateBoltBST(int tupleCountForArchive, int tupleCountForRemoval){
        this.tupleCountForArchive=tupleCountForArchive;
        this.tupleCountForRemoval= tupleCountForRemoval;
    }
    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        duration= new LinkedList<>();
        time= new LinkedList<>();
        this.outputCollector= outputCollector;
        this.hashSet= new HashSet<>();
    }

    @Override
    public void execute(Tuple tuple) {
        tupleCountForRemoval++;
        if(tuple.getSourceStreamId().equals("Left")){
            if(!duration.isEmpty()){
                RedBlackBST treeForTupleInsertion=duration.getLast();
                treeForTupleInsertion.put(tuple.getIntegerByField("Tuple"), tuple.getIntegerByField("ID"));
                if (treeForTupleInsertion.size() == tupleCountForArchive) {
                    RedBlackBST newRedBlackBST = new RedBlackBST();
                    duration.add(newRedBlackBST);
                }
            }
            else{
                RedBlackBST redBlackBSTForRevenue = new RedBlackBST();
                //Adding tuple to the linked list when list is not empty:
                redBlackBSTForRevenue.put(tuple.getIntegerByField("Tuple"), tuple.getIntegerByField("ID"));
            }
            for (RedBlackBST redBlackBST : time) {
                for(Node  nodeThatContainsGreater: redBlackBST.getNodesGreaterThan(tuple.getIntegerByField("tuple"))){
                    for(int id: nodeThatContainsGreater.getVals()){
                        hashSet.add(id); //HashSet
                        //Also add BitSet
                    }
                }
            }
        }
        if(tuple.getSourceStreamId().equals("Right")){

            if (!time.isEmpty()) {
                RedBlackBST treeForTupleInsertion = time.getLast();
                treeForTupleInsertion.put(tuple.getIntegerByField("Tuple"), tuple.getIntegerByField("ID"));
                if (treeForTupleInsertion.size() == tupleCountForArchive) {
                    RedBlackBST newRedBlackBST = new RedBlackBST();
                    time.add(newRedBlackBST);
                }
            } else {
                RedBlackBST redBlackBSTForRevenue = new RedBlackBST();
                //Adding tuple to the linked list when list is not empty:
                redBlackBSTForRevenue.put(tuple.getIntegerByField("Tuple"), tuple.getIntegerByField("ID"));
            }
            for (RedBlackBST redBlackBST : duration) {
                for(Node  nodeThatContainsGreater: redBlackBST.lessThanKey(tuple.getIntegerByField("tuple"))){
                    for(int id: nodeThatContainsGreater.getVals()){
                        hashSet.add(id); //HashSet
                        //Also add BitSet
                    }
                }

            }
        }
        if(time.size()==tupleCountForRemoval||duration.size()==tupleCountForRemoval){
            time.remove(time.getFirst());
            duration.remove(duration.getFirst());
        }
        }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
