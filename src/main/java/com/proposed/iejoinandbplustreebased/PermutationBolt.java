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
//        this.permutationLeft = (String) map.get("LeftBatchPermutation");
//        this.permutationRight = (String) map.get("RightBatchPermutation");
        this.currentTaskIndex= topologyContext.getThisTaskIndex();
        this.listOfDownStreamTasks= topologyContext.getComponentTasks(Constants.OFFSET_AND_IE_JOIN_BOLT_ID);
        this.downStreamTasks=0;
        this.outputCollector = outputCollector;


    }

    @Override
    public void execute(Tuple tuple) {
        // Tuple from left stream and compute permutation
        if (permutationLeft.equals(tuple.getSourceStreamId())) {
            if (Boolean.TRUE.equals(tuple.getValueByField(Constants.BATCH_COMPLETION_FLAG))) {
                leftFlag = true;
            } else {
                List<Integer> ids = convertToIntegerList((byte[]) tuple.getValueByField(Constants.PERMUTATION_TUPLE_IDS));

                leftStreamPermutation.add(new Permutation(tuple.getIntegerByField(Constants.TUPLE), ids));
            }
        }
        // Tuple from right stream for compute permutation
        else if (permutationRight.equals(tuple.getSourceStreamId())) {
            if (Boolean.TRUE.equals(tuple.getValueByField(Constants.BATCH_COMPLETION_FLAG))) {
                rightFlag = true;
            } else {
                List<Integer> ids = convertToIntegerList((byte[]) tuple.getValueByField(Constants.PERMUTATION_TUPLE_IDS));

                rightStreamPermutation.add(new Permutation(tuple.getIntegerByField(Constants.TUPLE), ids));
            }
        }
        // Check that represent both data structure is full moreover, it also flush the data structure after emitting the tuples
        if ((leftFlag == true) && (rightFlag == true)) {
            if(currentTaskIndex==0) {
                permutationComputation(leftStreamPermutation, rightStreamPermutation, listOfDownStreamTasks.get(downStreamTasks), permutationLeft,tuple);
            }
            else{
                permutationComputation(leftStreamPermutation, rightStreamPermutation, listOfDownStreamTasks.get(downStreamTasks),permutationRight,tuple );

            }

            downStreamTasks++;
            if(listOfDownStreamTasks.size()==downStreamTasks){
                downStreamTasks=0;
            }

            leftStreamPermutation = new ArrayList<>();
            rightStreamPermutation = new ArrayList<>();
            leftFlag = false;
            rightFlag = false;
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(permutationLeft,new Fields(Constants.TUPLE_ID,Constants.PERMUTATION_COMPUTATION_INDEX,Constants.BATCH_COMPLETION_FLAG));
        outputFieldsDeclarer.declareStream(permutationRight, new Fields(Constants.TUPLE_ID,Constants.PERMUTATION_COMPUTATION_INDEX,Constants.BATCH_COMPLETION_FLAG));
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
    private void permutationComputation(ArrayList<Permutation> permutationsArrayLeft, ArrayList<Permutation> permutationsArrayRight, int downStreamTaskIDs, String streamID, Tuple tuple ) {

        int[] holdingList = new int[permutationsArrayLeft.size()];
        int counter = 1;
        for (int i = 0; i < permutationsArrayLeft.size(); i++) {
            for (int ids : permutationsArrayLeft.get(i).getListOfIDs()) {

                    holdingList[ids] = counter;
                    counter++;


            }
        }
        for (int i = 0; i < permutationsArrayRight.size(); i++) {
            for (int ids : permutationsArrayRight.get(i).getListOfIDs()) {
                //Emit these tuples at once
                try {
                    outputCollector.emitDirect(downStreamTaskIDs, streamID, tuple, new Values(holdingList[ids], permutationsArrayRight.get(i).getIndex(), false));
                    outputCollector.ack(tuple);
                }
                catch (ArrayIndexOutOfBoundsException e){
                  //  outputCollector.emitDirect(downStreamTaskIDs, streamID, tuple, new Values(0, permutationsArrayRight.size(), false));
                    outputCollector.ack(tuple);
                }
                //  permutationArray.add(new Permutation(holdingList[ids],permutationsArrayRight.get(i).getIndex()));
            }
        }
        outputCollector.emitDirect(downStreamTaskIDs,streamID,tuple, new Values(0,0,true));
        outputCollector.ack(tuple);
    }


}
