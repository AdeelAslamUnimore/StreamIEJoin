package com.experiment.selfjoin.iejoinproposed;

import com.configurationsandconstants.iejoinandbaseworks.Configuration;
import com.configurationsandconstants.iejoinandbaseworks.Constants;
import com.proposed.iejoinandbplustreebased.SearchModel;
import com.stormiequality.join.Permutation;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.ByteArrayInputStream;
import java.util.*;

public class IEJoin extends BaseRichBolt {
    private boolean isLeftStreamPermutation;
    private boolean isRightStreamPermutation;
    private ArrayList<Permutation> listLeftPermutation;
    private ArrayList<Permutation> listRightPermutation;
    private String permutationLeft;
    private String permutationRight;
    private String mergeOperationStreamID;
    private boolean flagDuringMerge; //For tuple
    private int tupleRemovalCounter = 0;
    private Queue<Tuple> queueDuringMerge;
    private String leftStreamID;
    private LinkedList<ArrayList<PermutationSelfJoin>> linkedList;

    public IEJoin() {
        Map<String, Object> map = Configuration.configurationConstantForStreamIDs();
        this.permutationLeft = (String) map.get("LeftBatchPermutation");
        this.permutationRight = (String) map.get("RightBatchPermutation");
        this.leftStreamID = (String) map.get("LeftPredicateTuple");
        this.mergeOperationStreamID = (String) map.get("MergingFlag");
    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        isLeftStreamPermutation = false;
        isRightStreamPermutation = false;
        this.queueDuringMerge = new LinkedList<>();
        this.listLeftPermutation = new ArrayList<>();
        this.listRightPermutation = new ArrayList<>();
        this.linkedList = new LinkedList<>();
        this.tupleRemovalCounter = Constants.MUTABLE_WINDOW_SIZE;
        this.flagDuringMerge = false;

    }

    @Override
    public void execute(Tuple tuple) {
        if (tuple.getSourceStreamId().equals(mergeOperationStreamID)) {

            flagDuringMerge = tuple.getBooleanByField(Constants.MERGING_OPERATION_FLAG);
        }
        if ((tuple.getSourceStreamId().equals(this.leftStreamID))) {
            tupleRemovalCounter++;
            if (flagDuringMerge) {
                this.queueDuringMerge.offer(tuple);
            }
            if (!linkedList.isEmpty()) {
                resultComputation(tuple);
            }
        }


        if (permutationLeft.equals(tuple.getSourceStreamId())) {
            if (Boolean.TRUE.equals(tuple.getValueByField(Constants.BATCH_COMPLETION_FLAG))) {
                isLeftStreamPermutation = true;
            } else {
                List<Integer> ids = convertToIntegerList((byte[]) tuple.getValueByField(Constants.PERMUTATION_TUPLE_IDS));

                listLeftPermutation.add(new Permutation(tuple.getIntegerByField(Constants.TUPLE), ids));
            }
        }
        // Tuple from right stream for compute permutation
        if (permutationRight.equals(tuple.getSourceStreamId())) {
            if (Boolean.TRUE.equals(tuple.getValueByField(Constants.BATCH_COMPLETION_FLAG))) {
                isRightStreamPermutation = true;
            } else {
                List<Integer> ids = convertToIntegerList((byte[]) tuple.getValueByField(Constants.PERMUTATION_TUPLE_IDS));

                listRightPermutation.add(new Permutation(tuple.getIntegerByField(Constants.TUPLE), ids));
            }
        }
        // Check that represent both data structure is full moreover, it also flush the data structure after emitting the tuples
        if ((isLeftStreamPermutation == true) && (isRightStreamPermutation == true)) {
            permutationComputation(listLeftPermutation, listRightPermutation);
            listLeftPermutation = new ArrayList<>();
            listRightPermutation = new ArrayList<>();
            isLeftStreamPermutation = false;
            isRightStreamPermutation = false;
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }


    private static List<Integer> convertToIntegerList(byte[] byteArray) {
        List<Integer> integerList = new ArrayList<>();
        ByteArrayInputStream inputStream = new ByteArrayInputStream(byteArray);
        int nextByte;
        while ((nextByte = inputStream.read()) != -1) {
            integerList.add(nextByte);
        }
        return integerList;
    }

    private void permutationComputation(ArrayList<Permutation> listLeftPermutation, ArrayList<Permutation> listRightPermutation) {
        ArrayList<PermutationSelfJoin> listPermutationSelfJoin = new ArrayList<>();
        int[] holdingList = new int[listLeftPermutation.size()];
        int counter = 1;
        for (int i = 0; i < listLeftPermutation.size(); i++) {
            for (int ids : listLeftPermutation.get(i).getListOfIDs()) {

                holdingList[ids] = counter;
                counter++;


            }
        }

        for (int i = 0; i < listRightPermutation.size(); i++) {
            for (int ids : listRightPermutation.get(i).getListOfIDs()) {
                //Emit these tuples at once
                try {

                    listPermutationSelfJoin.add(new PermutationSelfJoin(holdingList[ids], listRightPermutation.get(i).getValue(), listLeftPermutation.get(i).getValue()));

                } catch (ArrayIndexOutOfBoundsException e) {
                    //  outputCollector.emitDirect(downStreamTaskIDs, streamID, tuple, new Values(0, permutationsArrayRight.size(), false));

                }
                //  permutationArray.add(new Permutation(holdingList[ids],permutationsArrayRight.get(i).getIndex()));
            }
        }
        linkedList.add(listPermutationSelfJoin);
        // Tuple Merge Evaluation: // Queue evaluation for the last item of linked list
        for(Tuple tuple:queueDuringMerge){
            System.out.println(queueDuringMerge.size()+"Merging Operation");
            bitSetEvaluation(linkedList.getLast(),tuple);
        }


        if (tupleRemovalCounter >= Constants.IMMUTABLE_WINDOW_SIZE) {
            linkedList.remove(linkedList.getFirst());
            tupleRemovalCounter = Constants.MUTABLE_WINDOW_SIZE;
        }
        queueDuringMerge = new LinkedList<>();


    }

    public void resultComputation(Tuple tuple) {
        for (int i = 0; i < linkedList.size(); i++) {

            bitSetEvaluation(linkedList.get(i), tuple);
        }
    }

    public void bitSetEvaluation(ArrayList<PermutationSelfJoin> listPermutationSelfJoin, Tuple tuple) {
        BitSet bitSet = new BitSet(listPermutationSelfJoin.size());
        int indexWithRight = binarySearchForIndexWithFlag(listPermutationSelfJoin, tuple.getInteger(1));
        int indexWithLeft = binarySearchWithIndex(listPermutationSelfJoin, tuple.getInteger(0));
        try {
            if (indexWithRight < listPermutationSelfJoin.size()) {
                for (int i = indexWithRight; i < listPermutationSelfJoin.size(); i++) {
                    bitSet.set(listPermutationSelfJoin.get(i).getIndex(), true);
                }
            }
        } catch (ArrayIndexOutOfBoundsException e) {

        }
        int count = 0;
        for (int i = indexWithLeft - 1; i >= 0; i--) {
            if (bitSet.get(i)) {
                count++;
            }
        }


    }

    public int binarySearchForIndexWithFlag(ArrayList<PermutationSelfJoin> arr, int target) {
        int left = 0;
        int right = arr.size() - 1;
        int result = -1; // Initialize the result variable to keep track of the next greatest index

        while (left <= right) {
            int mid = left + (right - left) / 2;
            int midVal = arr.get(mid).getRightStreamValue();

            if (midVal == target) {
                return mid + 1; // Target found, return its index
            } else if (midVal < target) {
                result = mid; // Update the result to the current index before moving to the right
                left = mid + 1;
            } else {
                right = mid - 1;
            }
        }

        // If the target is not found, return the index of the nearest element
        return result + 1;
    }

    public int binarySearchWithIndex(ArrayList<PermutationSelfJoin> arr, int target) {
        int left = 0;
        int right = arr.size() - 1;
        int result = -1; // Initialize the result variable to keep track of the next greatest index

        while (left <= right) {
            int mid = left + (right - left) / 2;
            int midVal = arr.get(mid).getLeftStreamValue();

            if (midVal == target) {
                return mid; // Target found, return its index
            } else if (midVal < target) {
                result = mid; // Update the result to the current index before moving to the right
                left = mid + 1;
            } else {
                right = mid - 1;
            }
        }

        // If the target is not found, return the index of the nearest element
        return result + 1;
    }
}
