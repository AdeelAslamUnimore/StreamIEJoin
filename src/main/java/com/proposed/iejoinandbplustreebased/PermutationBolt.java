package com.proposed.iejoinandbplustreebased;

import com.stormiequality.join.Permutation;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PermutationBolt extends BaseRichBolt {
    /**
     * Here permutation is computed
     * All streaming tuples from upstream processing bolts are either from left stream or right are processed here
     * Two flags indicate that all tuples in a batch is added to the data structure
     * outputCollector: for sending stream to downstream processing tasks;
     */
    private ArrayList<Permutation> leftStreamPermutation;
    private ArrayList<Permutation> rightStreamPermutation;
    private Boolean leftFlag;
    private Boolean rightFlag;
    private OutputCollector outputCollector;

    /**
     *  all initialization is performed both flags and and data structure is performed at here
     * @param map map is the config values that are later use;
     * @param topologyContext is the topology context
     * @param outputCollector initialization of output stream
     */
    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
            this.leftFlag=false;
            this.rightFlag=false;
            this.leftStreamPermutation= new ArrayList<>();
            this.rightStreamPermutation= new ArrayList<>();
            this.outputCollector=outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        // Tuple from left stream and compute permutation
        if ("PermutationLeft".equals(tuple.getSourceStreamId())) {
            if (Boolean.TRUE.equals(tuple.getValueByField("Flag"))) {
                leftFlag = true;
            } else {
                List<Integer> ids = convertToIntegerList((byte[]) tuple.getValueByField("IDs"));
                leftStreamPermutation.add(new Permutation(tuple.getIntegerByField("Tuple"), ids));
            }
        }
        // Tuple from right stream for compute permutation
        else if ("PermutationRight".equals(tuple.getSourceStreamId())) {
            if (Boolean.TRUE.equals(tuple.getValueByField("Flag"))) {
                rightFlag = true;
            } else {
                List<Integer> ids = convertToIntegerList((byte[]) tuple.getValueByField("IDs"));
                rightStreamPermutation.add(new Permutation(tuple.getIntegerByField("Tuple"), ids));
            }
        }
        // Check that represent both data structure is full moreover, it also flush the data structure after emitting the tuples
        if((leftFlag==true)&&(rightFlag==true)){
            permutationComputation(leftStreamPermutation, rightStreamPermutation);
            leftStreamPermutation= new ArrayList<>();
            rightStreamPermutation= new ArrayList<>();
            leftFlag=false;
            rightFlag=false;
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
    // conversion of byte Array into list of integer:
    private static List<Integer> convertToIntegerList(byte[] byteArray) {
        List<Integer> integerList = new ArrayList<>();
        ByteArrayInputStream inputStream = new ByteArrayInputStream(byteArray);
        int nextByte;
        while ((nextByte = inputStream.read()) != -1) {
            integerList.add(nextByte);
        }
        return integerList;
    }

    /**
     * Permutation is the index id of a stream with different relation and find index of that item in other index
     * The time complexity is O(n+m)
     * It is computed by taking a auxiliary counter that hold is assigned to each tuple
     * Finally that counter values are actual location
     * @param permutationsArrayLeft computing the permutation of left stream or right Stream
     * @param permutationsArrayRight same
     */
    private  void permutationComputation( ArrayList<Permutation> permutationsArrayLeft, ArrayList<Permutation> permutationsArrayRight){

        int [] holdingList= new int[permutationsArrayLeft.size()];
        int counter=1;
        for(int i=0;i<permutationsArrayLeft.size();i++){
            for(int ids: permutationsArrayLeft.get(i).getListOfIDs()){
                holdingList[ids]=counter;
                counter++;
            }
        }
        for(int i=0;i<permutationsArrayRight.size();i++){
            for(int ids: permutationsArrayRight.get(i).getListOfIDs()){
                //Emit these tuples at once
                //collector.emitDirect(taskID,streamID,tuple, new Values(holdingList[ids],permutationsArrayRight.get(i).getIndex(),false,System.currentTimeMillis()));
                //  permutationArray.add(new Permutation(holdingList[ids],permutationsArrayRight.get(i).getIndex()));
            }
        }
       // collector.emitDirect(taskID,streamID,tuple, new Values(0,0,true,System.currentTimeMillis()));
    }


}
