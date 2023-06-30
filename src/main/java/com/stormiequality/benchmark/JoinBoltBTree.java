package com.stormiequality.benchmark;

import com.stormiequality.BTree.BPlusTree;
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
import java.io.Serializable;
import java.util.BitSet;
import java.util.Map;

public class JoinBoltBTree extends BaseRichBolt {
    private int tuplecounter=0;
    private BPlusTree LeftBPlusTree=null;
    private BPlusTree RightBPlusTree=null;
    int orderOfBPlusTree=0;
    String operator;
    int counterSize=0;
    String emittingStreamID=null;
    private OutputCollector collector;
    public JoinBoltBTree(int orderOfBPlusTree, int counterSize, String operator, String  emittingForANDStreamID){
       this.orderOfBPlusTree=orderOfBPlusTree;
       this.counterSize=counterSize;
       this.operator=operator;
       this.emittingStreamID=emittingForANDStreamID;

    }
    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.tuplecounter=0;
        this.LeftBPlusTree= new BPlusTree();
        this.LeftBPlusTree.initialize(orderOfBPlusTree);
        this.RightBPlusTree= new BPlusTree();
        this.RightBPlusTree.initialize(orderOfBPlusTree);
        this.collector=outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        tuplecounter++;
        if(tuple.getSourceComponent().equals("testBolt")){
            if(operator.equals("<")){
                lessPredicateEvaluation(tuple);
            }
            if(operator.equals(">")){
                greaterPredicateEvaluation(tuple);
            }
        }

        if(tuple.getSourceComponent().equals("ForEvaluation")){
            try {
            if(operator.equals("<")){
                lessPredicateEvaluation1(tuple);
            }
            if(operator.equals(">")){

                    greaterPredicateEvaluation1(tuple);

            }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }


        if(tuplecounter==100000){
            tuplecounter=0;
            LeftBPlusTree= new BPlusTree();
            LeftBPlusTree.initialize(orderOfBPlusTree);
            RightBPlusTree= new BPlusTree();
            RightBPlusTree.initialize(orderOfBPlusTree);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(emittingStreamID,new Fields("bitset","ID"));

    }

    public void lessPredicateEvaluation(Tuple tuple){
        if(tuple.getValueByField("StreamID").equals("LeftStream")){

            LeftBPlusTree.insert(tuple.getIntegerByField("Tuple"),tuple.getIntegerByField("ID"));

        }
        if(tuple.getValueByField("StreamID").equals("RightStream")){
            // BitSet If Tuple is from right stream R.r>S.s  it first insert then search on other stream
            RightBPlusTree.insert(tuple.getIntegerByField("Tuple"),tuple.getIntegerByField("ID"));

        }

    }
    public void greaterPredicateEvaluation(Tuple tuple){
        if(tuple.getValueByField("StreamID").equals("LeftStream")){
            LeftBPlusTree.insert(tuple.getIntegerByField("Tuple"),tuple.getIntegerByField("ID"));

        }
        if(tuple.getValueByField("StreamID").equals("RightStream")){
            RightBPlusTree.insert(tuple.getIntegerByField("Tuple"),tuple.getIntegerByField("ID"));

        }

    }
    public void lessPredicateEvaluation1(Tuple tuple) throws IOException {
        if(tuple.getValueByField("StreamID").equals("LeftStream")){
            BitSet RBitSet= RightBPlusTree. lessThenSpecificValue(tuple.getIntegerByField("Tuple"));
            if(RBitSet!=null) {
                byte[] byteRightBatch = convertToByteArray(RBitSet);
                Values values = new Values(byteRightBatch, tuple.getIntegerByField("ID"));
                collector.emit(emittingStreamID, tuple, values);
               collector.ack(tuple);
            }

        }
        if(tuple.getValueByField("StreamID").equals("RightStream")){
            // BitSet If Tuple is from right stream R.r>S.s  it first insert then search on other stream
            BitSet LBitSet=  LeftBPlusTree. greaterThenSpecificValue(tuple.getIntegerByField("Tuple"));
            if(LBitSet!=null) {
                byte[] byteLeftBatch = convertToByteArray(LBitSet);
                //  System.out.println(LBitSet+"...."+"RightStreamLeftEvalution");
                Values values = new Values(byteLeftBatch, tuple.getIntegerByField("ID"));
                collector.emit(emittingStreamID, tuple, values);
              collector.ack(tuple);
            }

        }

    }
    public void greaterPredicateEvaluation1(Tuple tuple) throws IOException {
        if(tuple.getValueByField("StreamID").equals("LeftStream")){
            BitSet RBitSet= RightBPlusTree. greaterThenSpecificValue(tuple.getIntegerByField("Tuple"));
            if(RBitSet!=null) {
                byte[] byteLeftBatch = convertToByteArray(RBitSet);
                Values values = new Values(byteLeftBatch, tuple.getIntegerByField("ID"));
                collector.emit(emittingStreamID,  values);
                collector.ack(tuple);
            }
        }
        if(tuple.getValueByField("StreamID").equals("RightStream")) {

            BitSet LBitSet = LeftBPlusTree.lessThenSpecificValue(tuple.getIntegerByField("Tuple"));
          //System.out.println(LBitSet);
            if (LBitSet != null) {
              byte[] byteLeftBatch = convertToByteArray(LBitSet);
                Values values = new Values(byteLeftBatch, tuple.getIntegerByField("ID"));
                collector.emit(emittingStreamID,  values);
                collector.ack(tuple);
            }
        }}


    private byte[] convertToByteArray(Serializable object) throws IOException {
        try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
             ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream)) {
            objectOutputStream.writeObject(object);
            return byteArrayOutputStream.toByteArray();
        }}
}
