package com.proposed.iejoinandbplustreebased;

import com.stormiequality.BTree.Offset;
import com.stormiequality.join.Permutation;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import javax.management.Query;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.*;

public class IEJoinBolt extends BaseRichBolt {
    /**
     * Four booleans for Two relation and and two predicates
     * For making IEJoin it requires these two permutation array and two offset array
     * These boolean indicates the completeness of all tuples requires for permutation and computation array;
     * Tuples comes others bolts these boolean indicates their completeness.
     */
    private boolean isLeftStreamOffset;
    private boolean isRightStreamOffset;
    private boolean isLeftStreamPermutation;
    private boolean isRightStreamPermutation;
    /**
     * These four data structures are uses for holding the permutation and offset array
     * The permutation array is just the index array [1, 2, 3,4]
     * The offset array contains the original key, offset index on the next array position, bit that tell if original item presence and
     * size of the offset values if the original item present in other array location.
     */
    private ArrayList<Permutation> listLeftPermutation;
    private ArrayList<Permutation> listRightPermutation;
    private String leftStreamPermutation;
    private String rightSteamPermutation;
    private ArrayList<Offset> listLeftOffset;
    private ArrayList<Offset> listRightOffset;
    private String leftStreamOffset;
    private String rightSteamOffset;
    /*
        This counter is used for flush out the data structures that holds the keys.
     */
    private int tupleRemovalCounter = 0;
    /*
            This is the constructor used for tuple  taking  removing tuples by users
     */
    private boolean flagDuringMerge; //For tuple
    // StreamID For Merge Operation
    private String mergeOperationStreamID;
    // New Tuple Stream IDs
    private String leftStreamID;
    private String rightStreamID;
    // Two queues that holds the left and right Stream Items during merge
    private Queue<Tuple> leftStreamQueue;
    private Queue<Tuple> rightStreamQueue;

    /*
        All initilizations either for data structures or booleans are defined

     */
    public IEJoinBolt() {
        Map<String, Object> map = Configuration.configurationConstantForStreamIDs();
        this.mergeOperationStreamID = (String) map.get("MergingFlag");
        this.leftStreamID = (String) map.get("LeftPredicateTuple");
        this.rightStreamID = (String) map.get("RightPredicateTuple");
        this.leftStreamOffset = (String) map.get("LeftBatchOffset");
        this.rightSteamOffset = (String) map.get("RightBatchOffset");
        this.leftStreamPermutation = (String) map.get("LeftBatchPermutation");
        this.rightSteamPermutation = (String) map.get("RightBatchPermutation");
    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.isLeftStreamOffset = false;
        this.isRightStreamOffset = false;
        this.isRightStreamPermutation = false;
        this.isLeftStreamPermutation = false;
        this.listLeftPermutation = new ArrayList<>();
        this.listRightPermutation = new ArrayList<>();
        this.listLeftOffset = new ArrayList<>();
        this.listRightOffset = new ArrayList<>();
        this.leftStreamQueue = new LinkedList<>();
        this.rightStreamQueue = new LinkedList<>();
        this.flagDuringMerge = false;
        this.tupleRemovalCounter = Constants.mutableWindowSize;

    }

    /**
     * Several operations are performed here
     * All data structures tuples are collected here: for offset arrays and permutation Arrays in the form of streaming tuples
     * a tuple from source that is used for evaluation is also considered here. either from stream R or Stream S
     * Queue is maintained during merge operation between mutable and immutable component.
     *
     * @param tuple original tuple receive from upstream processing bolts
     */
    @Override
    public void execute(Tuple tuple) {
        // hold items during merge
        if (tuple.getSourceStreamId().equals(mergeOperationStreamID)) {
            flagDuringMerge = tuple.getBooleanByField(Constants.MERGING_OPERATION_FLAG);
        }
        /*
        This if statement is for probing input tuples for that task.
         */
        if ((tuple.getSourceStreamId().equals(this.leftStreamID)) || (tuple.getSourceStreamId().equals(this.rightStreamID))) {
            if (flagDuringMerge) {
                (tuple.getSourceStreamId().equals(this.leftStreamID) ? this.leftStreamQueue : this.rightStreamQueue)
                        .offer(tuple);
            }
            // Probing is performed over here
            if (checkConditionForAllPermutationAndOffsetArrays(isLeftStreamPermutation, isRightStreamPermutation, isLeftStreamOffset, isRightStreamOffset)) {
                // Probing of results of batch during merge operation;
                if (flagDuringMerge) {
                    mergeOperationTupleProbing(leftStreamQueue, rightStreamQueue);
                    flagDuringMerge = false;
                }
                String streamID = tuple.getSourceStreamId();
                lookUpOperation(tuple, streamID);
            }
        }
        /*
        This is for left permutation computation input tuple is added into this array
         */
        if (tuple.getSourceStreamId().equals(leftStreamPermutation)) {
            permutationComputation(tuple, true, listLeftPermutation);
        }
        /*
        This is for right stream permutation computation input stream tuple will add in this array
         */
        if (tuple.getSourceStreamId().equals(rightSteamPermutation)) {
            permutationComputation(tuple, false, listRightPermutation);
        }
        /*
        This is for left stream offset computation input stream tuple will add in this array
         */
        if (tuple.getSourceStreamId().equals(leftStreamOffset)) {
            offsetComputation(tuple, true, listLeftOffset);
        }
        /*
        This is for right stream offset computation input stream tuple will add in this array
         */
        if (tuple.getSourceStreamId().equals(rightSteamOffset)) {
            offsetComputation(tuple, false, listRightOffset);
        }
        /*
        To Do Use queues for holding incoming tuples for completeness;
         */
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    private void lookUpOperation(Tuple tuple, String streamID) {
        tupleRemovalCounter++;
        // If tuple is from left predicate then it perform operation for evaluation of tuples from right
        if (streamID.equals(leftStreamID)) {

            rightPredicateEvaluation(tuple.getIntegerByField("Duration"), tuple.getIntegerByField("Revenue"));
        } else {

            // If tuple is from right predicate then it perform operation for evaluation of tuples from left
            leftPredicateEvaluation(tuple.getIntegerByField("Time"), tuple.getIntegerByField("Cost"));
        }
        // bulk removal;
        // Re initialise the data to for freeing the memory
        if (tupleRemovalCounter >= Constants.immutableWindowSize) {

            this.isLeftStreamOffset = false;
            this.isRightStreamOffset = false;
            this.isRightStreamPermutation = false;
            this.isLeftStreamPermutation = false;
            this.listLeftPermutation = new ArrayList<>();
            this.listRightPermutation = new ArrayList<>();
            this.listLeftOffset = new ArrayList<>();
            this.listRightOffset = new ArrayList<>();
            tupleRemovalCounter = Constants.mutableWindowSize;
        }
        // Do the job

    }

    /*
    This check indicate all data structure are full and ready for creating IEJoin structure
    All boolean are needed to true for such evaluation
     */
    private boolean checkConditionForAllPermutationAndOffsetArrays(boolean listLeftPermutation, boolean listRightPermutation, boolean listLeftOffset, boolean listRightOffset) {
        if (listLeftPermutation && listRightPermutation && listLeftOffset && listRightOffset) {
            return true;

        } else {
            return false;
        }
    }

    /**
     * holding permutation computation array items
     * flag parameter differentiate If true then left stream otherwise right
     *
     * @param tuple                tuple input streaming tuple
     * @param flag                 is sent from upstream processing stream to indicate it last tuple this is signature for its associated
     *                             isLeftStreamOrRightStreamPermutation to be true
     * @param permutationArrayList a immutable list that which needs to be fill
     */
    private void permutationComputation(Tuple tuple, boolean flag, ArrayList<Permutation> permutationArrayList) {
        if (flag) {
            if (tuple.getValueByField(Constants.BATCH_COMPLETION_FLAG).equals(true)) {
                isLeftStreamPermutation = true;
            } else {
                permutationArrayList.add(new Permutation(tuple.getIntegerByField(Constants.PERMUTATION_COMPUTATION_INDEX)));
            }
        } else {
            if (tuple.getValueByField(Constants.BATCH_COMPLETION_FLAG).equals(true)) {
                isRightStreamPermutation = true;
            } else {
                permutationArrayList.add(new Permutation(tuple.getIntegerByField(Constants.PERMUTATION_COMPUTATION_INDEX)));
            }
        }
    }

    /**
     * holding offset computation array items
     * flag parameter differentiate If true then left stream otherwise right
     *
     * @param tuple           tuple input streaming tuple
     * @param flag            is sent from upstream processing stream to indicate it last tuple this is signature for its associated
     *                        isLeftStreamOrRightStreamPermutation to be true
     * @param offsetArrayList a immutable list that which needs to be fill
     */

    private void offsetComputation(Tuple tuple, boolean flag, ArrayList<Offset> offsetArrayList) {
        if (flag) {
            if (tuple.getValueByField(Constants.BATCH_COMPLETION_FLAG).equals(true)) {
                isLeftStreamOffset = true;
            } else {
                byte[] presenceBit = tuple.getBinaryByField(Constants.BYTE_ARRAY);
                BitSet presenceBitSetConversion = convertToObject(presenceBit);
                offsetArrayList.add(new Offset(tuple.getIntegerByField(Constants.TUPLE), tuple.getIntegerByField(Constants.OFFSET_TUPLE_INDEX),
                        presenceBitSetConversion, tuple.getIntegerByField(Constants.OFFSET_SIZE_OF_TUPLE)));
            }
        } else {
            if (tuple.getValueByField(Constants.BATCH_COMPLETION_FLAG).equals(true)) {
                isRightStreamOffset = true;
            } else {
                byte[] presenceBit = tuple.getBinaryByField(Constants.BYTE_ARRAY);
                BitSet presenceBitSetConversion = convertToObject(presenceBit);
                offsetArrayList.add(new Offset(tuple.getIntegerByField(Constants.TUPLE), tuple.getIntegerByField(Constants.OFFSET_TUPLE_INDEX),
                        presenceBitSetConversion, tuple.getIntegerByField(Constants.OFFSET_SIZE_OF_TUPLE)));
            }
        }
    }

    /**
     * This method is used for finding the key and its index, rightStream because it searches in right Stream
     * It performs binary search
     * Search the key if found then return original otherwise greatest;
     * It has also flag that indicate if original data item present or not:
     *
     * @param offsetArrayList offset array of right stream or stream
     * @param key             that is new key to probe
     * @return SearchModel that has a key and a bit for actual presence check
     */
    private SearchModel searchKeyForRightStream(ArrayList<Offset> offsetArrayList, int key) {
        SearchModel searchModel = new SearchModel();
        Offset offset = new Offset(key);
        Comparator<Offset> comparator = new Comparator<Offset>() {
            public int compare(Offset obj1, Offset obj2) {
                return Integer.compare(obj1.getKeyForSearch(), obj2.getKeyForSearch());
            }
        };
        int low = 0;
        int high = offsetArrayList.size() - 1;
        int result = -1;

        while (low <= high) {
            int mid = low + (high - low) / 2;
            if (comparator.compare(offsetArrayList.get(mid), offset) >= 0) {
                result = mid;
                high = mid - 1;
            } else {
                low = mid + 1;
            }
        }

        if (result != -1) {
            int id=0;
           try{
                id=listLeftOffset.get(result).getKeyForSearch();
            }catch (IndexOutOfBoundsException e){
                id=listLeftOffset.get(listLeftOffset.size()-1).getKeyForSearch();
            }
            if (id == key) {
                searchModel.setIndexPosition(result);
                BitSet bitSet = new BitSet();
                bitSet.set(0, true);
                searchModel.setBitSet(bitSet);
                return searchModel;
            } else {
                searchModel.setIndexPosition(result);
                BitSet bitSet = new BitSet();
                bitSet.set(0, false);
                searchModel.setBitSet(bitSet);
                return searchModel;
            }
        } else {
            searchModel.setIndexPosition(offsetArrayList.size() - 1);
            BitSet bitSet = new BitSet();
            bitSet.set(0, false);
            searchModel.setBitSet(bitSet);
            return searchModel;
        }

    }
    /*
    For evaluation according to the query mentioned in the original paper It start scanning the it switch on the all the bits from start to the
    offset position of identified location -1.
    For other scanning it consider the flag if the original key is found then It searches offset location to the end:
    In this case if offset also have original tuple on the next array it starts scanning bit array from (tuple location +size(same tuples exists on other stream)) to the end
    else start from the index position.

        */

    private void rightPredicateEvaluation(int tuple1, int tuple2) {
        SearchModel offsetSearchKeySecond = searchKeyForRightStream(listRightOffset, tuple2);
        BitSet bitSet = new BitSet();
        int count=0;
        int offset = listRightOffset.get(offsetSearchKeySecond.getIndexPosition()).getIndex() - 1;

        offset = offset - 1;
        // System.exit(-1);
        if (offset >= 0) {
            for (int j = 0; j <= offset; j++) {
                try {
                    bitSet.set(listRightPermutation.get(j).getIndex() - 1, true);
                } catch (IndexOutOfBoundsException e) {
                    e.printStackTrace();
                }
            }
            SearchModel offsetSearchKeyFirst = searchKeyForRightStream(listLeftOffset, tuple1);
            Offset offset1 = listLeftOffset.get(offsetSearchKeyFirst.getIndexPosition());
            int off = offset1.getIndex() - 1;
            int size = listLeftPermutation.size();
            if (offset1.getBitSet().get(0) && offsetSearchKeyFirst.getBitSet().get(0)) {
                for (int k = off + offset1.getSize(); k < size; k++) {
                    if (bitSet.get(k)) {
                        count++;
                        // System.out.println("I am here" + k);
                    }
                }
            } else {
                for (int k = off; k < size; k++) {
                    if (bitSet.get(k)) {
                        count++;
                        // System.out.println("I am here" + k);
                    }
                }
            }
        }


    }

    /**
     * This method is used for finding the key and its index, rightStream because it searches in right Stream
     * For left stream we donot need because this search performs binary operation and return first great key index in the offset array
     *
     * @param offsetArrayList offset array of right stream or stream
     * @param key             that is new key to probe
     * @return integer index of searched key
     */

    private int searchKeyForLeftStream(ArrayList<Offset> offsetArrayList, int key) {
        Offset offset = new Offset(key);
        Comparator<Offset> comparator = new Comparator<Offset>() {
            public int compare(Offset u1, Offset u2) {
                return Integer.compare(u1.getKeyForSearch(), u2.getKeyForSearch());
            }
        };
        int result = -1;

        int low = 0;
        int high = offsetArrayList.size() - 1;

        while (low <= high) {
            int mid = low + (high - low) / 2;

            if (comparator.compare(offsetArrayList.get(mid), offset) > 0) {
                result = mid;
                high = mid - 1;
            } else {
                low = mid + 1;
            }
        }

        if (result != -1) {
            return result;

        } else {
            return offsetArrayList.size();
        }
    }

    /*
       For evaluation according to the query mentioned in the original paper It start scanning the it switch on the all the bits from start to the
       offset position of identified location -1.
       A bit array is used for evaluation, It uses the position of search item that is greater than the original key from offset right.
       first it switch on all permutation location bits from the identified position to the end .
       Similarly, for probing it consider first key that identified  offset array left and scan it to the start for probing.
           */
    private void leftPredicateEvaluation(int tuple1, int tuple2) {

        int offsetSearchKeySecond = searchKeyForLeftStream(listRightOffset, tuple2);
        int count = 0;
        BitSet bitSet = new BitSet();
        for (int i = offsetSearchKeySecond; i < listLeftPermutation.size(); i++) {
            try {
                bitSet.set(listLeftPermutation.get(i).getIndex() - 1, true);
            } catch (IndexOutOfBoundsException e) {
                e.printStackTrace();
            }
        }
        int offsetSearchKeyFirst = searchKeyForLeftStream(listLeftOffset, tuple1) - 1;
        // Checking the equalities
        for (int i = offsetSearchKeyFirst; i < -1; i--) {
            if (listLeftOffset.get(i).getKey() != offsetSearchKeyFirst) {
                offsetSearchKeyFirst = i;
                break;
            }
        }
        for (int i = offsetSearchKeyFirst; i < -1; i--) {
            if (bitSet.get(i)) {
                count++;
            }
        }

    }

    /**
     * probing
     *
     * @param leftStreamQueue  left Queue during merge operation
     * @param rightStreamQueue Right Queue during merge operation
     */
    private void mergeOperationTupleProbing(Queue<Tuple> leftStreamQueue, Queue<Tuple> rightStreamQueue) {
        if (!leftStreamQueue.isEmpty()) {
            for (Tuple tuple : leftStreamQueue) {
                tupleRemovalCounter++;
                rightPredicateEvaluation(tuple.getIntegerByField("Duration"), tuple.getIntegerByField("Revenue"));
            }
        }
        if (!rightStreamQueue.isEmpty()) {
            // If tuple is from left predicate then it perform operation for evaluation of tuples from right
            for (Tuple tuple : rightStreamQueue) {
                tupleRemovalCounter++;
                leftPredicateEvaluation(tuple.getIntegerByField("Time"), tuple.getIntegerByField("Cost"));
            }
        }

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





