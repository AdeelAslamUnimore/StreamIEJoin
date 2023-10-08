package com.proposed.iejoinandbplustreebased;

import com.configurationsandconstants.iejoinandbaseworks.Configuration;
import com.configurationsandconstants.iejoinandbaseworks.Constants;
import com.stormiequality.join.Permutation;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.HashMap;
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
    // Flag for waiting other batches
    private Boolean leftFlag;
    private Boolean rightFlag;
    private OutputCollector outputCollector;
    private String permutationLeft;
    private String permutationRight;
    private List<Integer> listOfDownStreamTasks;
    // Two task for one for left one for right
    private int currentTaskIndex;
    private int downStreamTasks;
    // For Correctness:
    private Map<Integer, ArrayList<Permutation>> mapLeftStreamPermutation;
    private Map<Integer, ArrayList<Permutation>> mapRightStreamPermutation;

    // Counter Test;
    private int counterTest;

    public PermutationBolt(){
        Map<String, Object> map= Configuration.configurationConstantForStreamIDs();
        this.permutationLeft = (String) map.get("LeftBatchPermutation");
        this.permutationRight = (String) map.get("RightBatchPermutation");
    }
    /**
     * all initialization is performed both flags and and data structure is performed at here
     *
     * @param map             map is the config values that are later use;
     * @param topologyContext is the topology context
     * @param outputCollector initialization of output stream
     */
    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.leftFlag = false;
        this.rightFlag = false;
        this.leftStreamPermutation = new ArrayList<>();
        this.rightStreamPermutation = new ArrayList<>();
        this.currentTaskIndex= topologyContext.getThisTaskIndex();
        this.listOfDownStreamTasks= topologyContext.getComponentTasks(Constants.OFFSET_AND_IE_JOIN_BOLT_ID);
        this.downStreamTasks=0;
        this.outputCollector = outputCollector;
        // Error Checking or checking its resilience;
        mapLeftStreamPermutation= new HashMap<>();
        mapRightStreamPermutation= new HashMap<>();
        this.counterTest=0;
    }

    @Override
    public void execute(Tuple tuple) {
        // Tuple from left stream and compute permutation
        if (permutationLeft.equals(tuple.getSourceStreamId())) {
            if (Boolean.FALSE.equals(tuple.getValueByField(Constants.BATCH_COMPLETION_FLAG))) {
//                List<Integer> ids = convertToIntegerList((byte[]) tuple.getValueByField(Constants.PERMUTATION_TUPLE_IDS));
//                System.out.println(ids.size()+"Right Size");
                leftStreamPermutation.add(new Permutation(tuple.getIntegerByField(Constants.TUPLE), tuple.getIntegerByField(Constants.PERMUTATION_TUPLE_IDS)));


            } else {
                leftFlag = true;

                mapLeftStreamPermutation.put(tuple.getIntegerByField(Constants.IDENTIFIER),leftStreamPermutation);
                //Just for checking
                leftStreamPermutation = new ArrayList<>();
            }
        }
        // Tuple from right stream for compute permutation
        if (permutationRight.equals(tuple.getSourceStreamId())) {
            if (Boolean.FALSE.equals(tuple.getValueByField(Constants.BATCH_COMPLETION_FLAG))) {
               // List<Integer> ids = convertToIntegerList((byte[]) tuple.getValueByField(Constants.PERMUTATION_TUPLE_IDS));
              // counterTest=counterTest+ids.size();

                rightStreamPermutation.add(new Permutation(tuple.getIntegerByField(Constants.TUPLE), tuple.getIntegerByField(Constants.PERMUTATION_TUPLE_IDS)));
            } else {
                rightFlag = true;

                mapRightStreamPermutation.put(tuple.getIntegerByField(Constants.IDENTIFIER),rightStreamPermutation);
                // Just for checking
                rightStreamPermutation = new ArrayList<>();
            }
        }
        // Check that represent both data structure is full moreover, it also flush the data structure after emitting the tuples
        if ((leftFlag == true) && (rightFlag == true)) {

            if(currentTaskIndex==0) {
                List<Integer> idList = new ArrayList<>(mapLeftStreamPermutation.keySet());
                for(int id:idList) {
                    if(mapRightStreamPermutation.containsKey(id)){
                        //
                     //   System.out.println(mapRightStreamPermutation.get(id).size());
                    permutationComputation(mapLeftStreamPermutation.get(id), mapRightStreamPermutation.get(id), listOfDownStreamTasks.get(downStreamTasks), permutationLeft, tuple,id);
                    mapLeftStreamPermutation.remove(id);
                    mapRightStreamPermutation.remove(id);
                }}
                // Down stream Task For Window Update
                for(int i=0;i<listOfDownStreamTasks.size();i++){
                    if(i>downStreamTasks) {
                        outputCollector.emitDirect(listOfDownStreamTasks.get(i), "WindowCount", tuple, new Values(Constants.MUTABLE_WINDOW_SIZE));
                        outputCollector.ack(tuple);
                    }
                }

            }
            else{
                List<Integer> idList = new ArrayList<>(mapLeftStreamPermutation.keySet());
                for(int id:idList) {
                    if(mapRightStreamPermutation.containsKey(id)){
                    permutationComputation(mapLeftStreamPermutation.get(id), mapRightStreamPermutation.get(id), listOfDownStreamTasks.get(downStreamTasks), permutationRight, tuple,id);
                    mapLeftStreamPermutation.remove(id);
                    mapRightStreamPermutation.remove(id);
                }}
            }

            downStreamTasks++;
            if(listOfDownStreamTasks.size()==downStreamTasks){
                downStreamTasks=0;
            }

//            leftStreamPermutation = new ArrayList<>();
//            rightStreamPermutation = new ArrayList<>();
            leftFlag = false;
            rightFlag = false;
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(permutationLeft,new Fields(Constants.TUPLE,Constants.PERMUTATION_COMPUTATION_INDEX,Constants.BATCH_COMPLETION_FLAG, Constants.IDENTIFIER));
        outputFieldsDeclarer.declareStream(permutationRight, new Fields(Constants.TUPLE,Constants.PERMUTATION_COMPUTATION_INDEX,Constants.BATCH_COMPLETION_FLAG,Constants.IDENTIFIER));
        outputFieldsDeclarer.declareStream("WindowCount", new Fields("Count"));
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
     *
     * @param permutationsArrayLeft  computing the permutation of left stream or right Stream
     * @param permutationsArrayRight same
     */
    private void permutationComputation(ArrayList<Permutation> permutationsArrayLeft, ArrayList<Permutation> permutationsArrayRight, int downStreamTaskIDs, String streamID, Tuple tuple, int id) {

        int[] holdingList = new int[20000000];
        int counter = 1;
        for (int i = 0; i < permutationsArrayLeft.size(); i++) {
//            for (int ids : permutationsArrayLeft.get(i).getId()) {

                holdingList[permutationsArrayLeft.get(i).getId()] = counter;
                counter++;


           // }
        }
        for (int i = 0; i < permutationsArrayRight.size(); i++) {
          //  for (int ids : permutationsArrayRight.get(i).getListOfIDs()) {
                //Emit these tuples at once
                try {
                    outputCollector.emitDirect(downStreamTaskIDs, streamID, tuple, new Values( permutationsArrayRight.get(i).getTuple(),holdingList[permutationsArrayRight.get(i).getId()], false,id));
                    outputCollector.ack(tuple);
                }
                catch (ArrayIndexOutOfBoundsException e){
                    //  outputCollector.emitDirect(downStreamTaskIDs, streamID, tuple, new Values(0, permutationsArrayRight.size(), false));
                    outputCollector.ack(tuple);
                }
                //  permutationArray.add(new Permutation(holdingList[ids],permutationsArrayRight.get(i).getIndex()));
            }
       // }
        outputCollector.emitDirect(downStreamTaskIDs,streamID,tuple, new Values(0,0,true,id));
        outputCollector.ack(tuple);

    }


}
