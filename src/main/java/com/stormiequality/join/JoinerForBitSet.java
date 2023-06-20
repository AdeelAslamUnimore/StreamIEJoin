package com.stormiequality.join;

import com.stormiequality.BTree.Node;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.io.*;
import java.util.BitSet;
import java.util.Map;

public class JoinerForBitSet extends BaseRichBolt {
    private BitSet predicate1BitSet=null;
    private BitSet predicate2BitSet=null;
    private int bitSetCount;
    int predicate1TupleID;
    int predicate2TupleID;
    String predicate1=null;
    String predicate2=null;
    long time=0l;
    long time2=0l;
    int taskId=0;
    private BufferedWriter writer=null;
    private OutputCollector collector;

    public JoinerForBitSet (String predicate1, String predicate2){
        this.predicate1=predicate1;
        this.predicate2=predicate2;
    }
    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
       // predicate1BitSet= new BitSet(bitSetCount);
       // predicate2BitSet= new BitSet(bitSetCount);
         predicate1TupleID=0;
       predicate2TupleID=0;
       time=0l;
       time=0l;
       this.collector= outputCollector;
       taskId= topologyContext.getThisTaskId();
    }

    @Override
    public void execute(Tuple tuple) {
        if(tuple.getSourceStreamId().equals(predicate1)){
            byte[] byteArrayPredicateLeftBitSet = tuple.getBinaryByField("bitset");
            predicate1BitSet= convertToObject(byteArrayPredicateLeftBitSet);
            predicate1TupleID=  tuple.getIntegerByField("TupleID");
        }
    if(tuple.getSourceStreamId().equals(predicate2)){
            time2= tuple.getLongByField("Time");
            byte[] byteArrayPredicateRightArray = tuple.getBinaryByField("bitset");
            predicate2BitSet= convertToObject(byteArrayPredicateRightArray);
            predicate2TupleID=  tuple.getIntegerByField("TupleID");

        }
       if ((predicate1BitSet!=null) && (predicate2BitSet!=null)) {
           predicate2BitSet.and(predicate1BitSet);
           this.collector.ack(tuple);

       }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
    private BitSet convertToObject(byte[] byteData) {
        try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(byteData);
             ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream)) {
            Object object = objectInputStream.readObject();
            if (object instanceof BitSet) {
                return (BitSet) object;
            } else {
                throw new IllegalArgumentException("Invalid object type after deserialization");
            }
        } catch (ClassNotFoundException | IOException e) {
            e.printStackTrace();
            // Handle the exception appropriately
        }
        return null; // Return null or handle the failure case accordingly
    }

}
