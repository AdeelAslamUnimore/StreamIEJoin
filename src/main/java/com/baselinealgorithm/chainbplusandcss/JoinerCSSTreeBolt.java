package com.baselinealgorithm.chainbplusandcss;

import com.configurationsandconstants.iejoinandbaseworks.Constants;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.HashSet;
import java.util.Map;

public class JoinerCSSTreeBolt extends BaseRichBolt {
    private HashSet<Integer> leftHashSet;
    private HashSet<Integer> rightHashSet;
    private String leftStreamID;
    private String rightStreamID;
    public JoinerCSSTreeBolt(String leftStreamID, String rightStreamID){
        this.leftStreamID=leftStreamID;
        this.rightStreamID=rightStreamID;
    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {

    }

    @Override
    public void execute(Tuple tuple) {
        if(tuple.getSourceStreamId().equals(leftStreamID)) {
            leftHashSet = convertByteArrayToHashSet(tuple.getBinaryByField(Constants.LEFT_HASH_SET));
        }
        if(tuple.getSourceStreamId().equals(rightStreamID)){
            rightHashSet=convertByteArrayToHashSet(tuple.getBinaryByField(Constants.RIGHT_HASH_SET));
        }
        if(leftHashSet!=null&&rightHashSet!=null){
            leftHashSet.retainAll(rightHashSet);
          //  System.out.println(leftHashSet);
            leftHashSet=null;
            rightHashSet=null;
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
    public static HashSet<Integer> convertByteArrayToHashSet(byte[] byteArray) {
        HashSet<Integer> hashSet = null;
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(byteArray);
            ObjectInputStream ois = new ObjectInputStream(bis);
            hashSet = (HashSet<Integer>) ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return hashSet;
    }
}
