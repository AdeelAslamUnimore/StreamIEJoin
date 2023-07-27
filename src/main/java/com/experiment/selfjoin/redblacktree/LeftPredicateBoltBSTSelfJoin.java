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

public class LeftPredicateBoltBSTSelfJoin extends BaseRichBolt {
    private LinkedList<RedBlackBST> leftPredicateLinkedListBST;
    private int tupleCountForArchiveUserDefined = 0;
    private int tupleCountForRemovalUserDefined = 0;
    private int treeArchiveThresholdLeft;
    private int treeArchiveThresholdRight;
    private OutputCollector outputCollector;
    private int tupleRemovalCountForLocal;
    private Map<String, Object> map;
    private String leftGreaterStreamID;

    public LeftPredicateBoltBSTSelfJoin() {
        map = Configuration.configurationConstantForStreamIDs();
        this.tupleCountForArchiveUserDefined = Constants.TUPLE_ARCHIVE_THRESHOLD;
        this.tupleCountForRemovalUserDefined = Constants.TUPLE_REMOVAL_THRESHOLD;
        this.leftGreaterStreamID = (String) map.get("LeftGreaterPredicateTuple");


    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        leftPredicateLinkedListBST = new LinkedList<>();
        this.outputCollector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        tupleRemovalCountForLocal++;
        if (tuple.getSourceStreamId().equals(leftGreaterStreamID)) {
            if (!leftPredicateLinkedListBST.isEmpty()) {
                treeArchiveThresholdLeft++;
                RedBlackBST treeForTupleInsertion = leftPredicateLinkedListBST.getLast();
                treeForTupleInsertion.put(tuple.getIntegerByField(Constants.TUPLE), tuple.getIntegerByField(Constants.TUPLE_ID));
                if (treeArchiveThresholdLeft >= tupleCountForArchiveUserDefined) {
                    RedBlackBST newRedBlackBST = new RedBlackBST();
                    leftPredicateLinkedListBST.add(newRedBlackBST);
                    treeArchiveThresholdLeft = 0;
                }
            } else {
                RedBlackBST redBlackBSTForDuration = new RedBlackBST();
                //Adding tuple to the linked list when list is not empty:
                treeArchiveThresholdLeft++;
                redBlackBSTForDuration.put(tuple.getIntegerByField(Constants.TUPLE), tuple.getIntegerByField(Constants.TUPLE_ID));
                leftPredicateLinkedListBST.add(redBlackBSTForDuration);

            }
            ///Searching

            for (RedBlackBST redBlackBST : leftPredicateLinkedListBST) {
                HashSet<Integer> hashSet = new HashSet<>();
                for (Node nodeThatContainsGreater : redBlackBST.lessThanKey(tuple.getIntegerByField(Constants.TUPLE_ID))) {

                    hashSet.addAll(nodeThatContainsGreater.getVals()); //HashSet
                    //Also add BitSet

                }
                outputCollector.emit(Constants.LEFT_PREDICATE_BOLT, new Values(convertHashSetToByteArray(hashSet), tuple.getIntegerByField(Constants.TUPLE_ID)));
                outputCollector.ack(tuple);

            }
        }
        if(tupleRemovalCountForLocal>= tupleCountForRemovalUserDefined){
            leftPredicateLinkedListBST.remove(leftPredicateLinkedListBST.getFirst());
            tupleRemovalCountForLocal=0;
        }

    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(Constants.LEFT_PREDICATE_BOLT, new Fields(Constants.LEFT_HASH_SET, Constants.TUPLE_ID));

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
