package com.experiment.selfjoin.redblacktree;

import com.baselinealgorithm.chainindexrbst.Node;
import com.baselinealgorithm.chainindexrbst.RedBlackBST;
import com.configurationsandconstants.iejoinandbaseworks.Configuration;
import com.configurationsandconstants.iejoinandbaseworks.Constants;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;

public class RightPredicateBoltBSTSelfJoin extends BaseRichBolt {
    private LinkedList<RedBlackBST> rightPredicateLinkedList;
    private int tupleCountForArchiveUserDefined =0;
    private int tupleCountForRemovalUserDefined =0;
    private int treeArchiveThresholdLeft;
    private int treeArchiveThresholdRight;
    private int tupleRemovalCountForLocal;
    private OutputCollector outputCollector;

    private String rightSmallerStreamID;
    private Map<String, Object> map;
    public RightPredicateBoltBSTSelfJoin(){
        this.tupleCountForArchiveUserDefined = Constants.TUPLE_ARCHIVE_THRESHOLD;
        this.tupleCountForRemovalUserDefined =Constants.TUPLE_REMOVAL_THRESHOLD;
        map= Configuration.configurationConstantForStreamIDs();

        this.rightSmallerStreamID = (String) map.get("RightSmallerPredicateTuple");
    }
    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        rightPredicateLinkedList = new LinkedList<>();
        this.outputCollector=outputCollector;
        this.tupleRemovalCountForLocal=0;

    }

    @Override
    public void execute(Tuple tuple) {
        tupleRemovalCountForLocal++;

        if (tuple.getSourceStreamId().equals(rightSmallerStreamID)) {
            if (!rightPredicateLinkedList.isEmpty()) {
                treeArchiveThresholdRight++;
                RedBlackBST treeForTupleInsertion = rightPredicateLinkedList.getFirst();
                treeForTupleInsertion.put(tuple.getIntegerByField(Constants.TUPLE), tuple.getIntegerByField(Constants.TUPLE_ID));
                if ( treeArchiveThresholdRight >= tupleCountForArchiveUserDefined) {
                    RedBlackBST newRedBlackBST = new RedBlackBST();
                    rightPredicateLinkedList.add(newRedBlackBST);
                    treeArchiveThresholdRight=0;
                }
            } else {
                treeArchiveThresholdRight++;
                RedBlackBST redBlackBSTForCost = new RedBlackBST();
                //Adding tuple to the linked list when list is not empty:
                redBlackBSTForCost.put(tuple.getIntegerByField(Constants.TUPLE), tuple.getIntegerByField(Constants.TUPLE_ID));
                rightPredicateLinkedList.add(redBlackBSTForCost);
            }
            for (RedBlackBST redBlackBST : rightPredicateLinkedList) {
                HashSet<Integer> hashSet= new HashSet<>();
                for(Node  nodeThatContainsGreater: redBlackBST.getNodesGreaterThan(tuple.getIntegerByField(Constants.TUPLE_ID))){

                        hashSet.addAll(nodeThatContainsGreater.getVals()); //HashSet
                        //Also add BitSet

                }
                outputCollector.emit(Constants.RIGHT_PREDICATE_BOLT,new Values(convertHashSetToByteArray(hashSet), tuple.getIntegerByField(Constants.TUPLE_ID)));
                outputCollector.ack(tuple);

            }
        }
        if(tupleRemovalCountForLocal>= tupleCountForRemovalUserDefined){
            rightPredicateLinkedList.remove(rightPredicateLinkedList.getFirst());
            tupleRemovalCountForLocal=0;
        }
        //Add the data values


    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(Constants.RIGHT_PREDICATE_BOLT,new Fields(Constants.RIGHT_HASH_SET, Constants.TUPLE_ID));

    }
    public static byte[] convertHashSetToByteArray(HashSet<Integer> hashSet) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try {
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(hashSet);
            oos.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return bos.toByteArray();
    }
}
