package com.proposed.iejoinandbplustreebased;

import com.stormiequality.BTree.Offset;
import com.stormiequality.join.Permutation;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Map;

public class IEComputationBolt extends BaseRichBolt {
    // boolean for waiting completion of whole batch of data
    private boolean isLeftStreamOffset;
    private boolean isRightStreamOffset;
    private boolean isLeftStreamPermutation;
    private boolean isRightStreamPermutation;
    private ArrayList<Permutation> listLeftPermutation;
    private ArrayList<Permutation> listRightPermutation;
    private ArrayList<Offset> listLeftOffset;
    private ArrayList<Offset> listRightOffset;
    private int tupleRemovalCounter=0;
    public IEComputationBolt(int tupleRemovalCounter){
        this.tupleRemovalCounter=tupleRemovalCounter;
    }
    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {


    }

    @Override
    public void execute(Tuple tuple) {
        if((tuple.getSourceStreamId().equals("LeftStream"))||(tuple.getSourceStreamId().equals("RightStream"))){
            if(checkConditionForAllPermutationAndOffsetArrays(isLeftStreamPermutation,isRightStreamPermutation,isLeftStreamOffset,isRightStreamOffset)){
              int tupleValue= tuple.getIntegerByField("");
              String streamID= tuple.getSourceStreamId();
                lookUpOperation(tupleValue,streamID);
            }
        }
        if(tuple.getSourceStreamId().equals("LeftPermutation")){
            permutationComputation(tuple,isLeftStreamPermutation,listLeftPermutation);
        }
        if(tuple.getSourceStreamId().equals("RightPermutation")){
            permutationComputation(tuple,isRightStreamPermutation,listRightPermutation);
        }
        if(tuple.getSourceStreamId().equals("LeftOffset")){
            offsetComputation(tuple, isLeftStreamOffset,listLeftOffset);
        }
        if(tuple.getSourceStreamId().equals("RightOffset")){
            offsetComputation(tuple,isRightStreamOffset,listRightOffset);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
    private void lookUpOperation(int tuple, String streamID){
        tupleRemovalCounter++;
        // Do the job

    }
    private boolean checkConditionForAllPermutationAndOffsetArrays(boolean listLeftPermutation, boolean listRightPermutation, boolean listLeftOffset, boolean listRightOffset){
        if(listLeftPermutation&&listRightPermutation&&listLeftOffset&&listRightOffset){
            return  true;

        }
        else{
            return false;
        }
    }
    private void permutationComputation(Tuple tuple, boolean flag,ArrayList<Permutation> permutationArrayList){
        if(tuple.getValueByField("Flag").equals(true)){
            flag=true;
        }
        else {
            permutationArrayList.add(new Permutation(tuple.getIntegerByField("Index")));
        }
    }
    private void offsetComputation(Tuple tuple,boolean flag, ArrayList<Offset> offsetArrayList){
        if(tuple.getValueByField("Flag").equals(true)){
            flag=true;
        }
        else {
            offsetArrayList.add(new Offset(tuple.getIntegerByField("Tuple"), tuple.getIntegerByField("Index"),
                    (BitSet)tuple.getValueByField("Bit"), tuple.getIntegerByField("Size")));
        }
    }

}
