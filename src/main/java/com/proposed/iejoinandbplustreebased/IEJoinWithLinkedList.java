package com.proposed.iejoinandbplustreebased;

import com.configurationsandconstants.iejoinandbaseworks.Configuration;
import com.configurationsandconstants.iejoinandbaseworks.Constants;
import com.stormiequality.BTree.Offset;
import com.stormiequality.join.Permutation;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.*;

public class IEJoinWithLinkedList extends BaseRichBolt {
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
    private int testingCounter;
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
    private LinkedList<IEJoinModel> linkedListIEJoinModel;
    private String R1;
    private String R2;
    private String S1;
    private String S2;
    //
    private int taskIndexTest=0;
    private IEJoinModel ieJoinModel;
    //Map for Left Permutation
    private HashMap<Integer, ArrayList<Permutation>> mapLeftPermutation;
    // Map for Right Permutation
    private HashMap<Integer, ArrayList<Permutation>> mapRightPermutation;
    //map for Left offset
    private HashMap<Integer, ArrayList<Offset>> mapLeftOffset;
    // Map for Right Offset
    private HashMap<Integer, ArrayList<Offset>> mapRightOffset;


    /*
        All initilizations either for data structures or booleans are defined

     */
    public IEJoinWithLinkedList(String R1, String R2, String S1, String S2) {
        Map<String, Object> map = Configuration.configurationConstantForStreamIDs();
        this.mergeOperationStreamID = (String) map.get("MergingFlag");
        this.leftStreamID = (String) map.get("LeftPredicateTuple");
        this.rightStreamID = (String) map.get("RightPredicateTuple");
        this.leftStreamOffset = (String) map.get("LeftBatchOffset");
        this.rightSteamOffset = (String) map.get("RightBatchOffset");
        this.leftStreamPermutation = (String) map.get("LeftBatchPermutation");
        this.rightSteamPermutation = (String) map.get("RightBatchPermutation");
        this.R1=R1;
        this.R2=R2;
        this.S1=S1;
        this.S2=S2;
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
        linkedListIEJoinModel = new LinkedList<>();
        this.taskIndexTest= topologyContext.getThisTaskIndex();
        this.ieJoinModel = new IEJoinModel();
        // Initilization of HashTable
        this.mapLeftPermutation= new HashMap<>();
        this.mapRightPermutation= new HashMap<>();
        this.mapLeftOffset= new HashMap<>();
        this.mapRightOffset= new HashMap<>();
        this.tupleRemovalCounter = Constants.MUTABLE_WINDOW_SIZE;

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

            if (!this.linkedListIEJoinModel.isEmpty()) {
                // Probing of results of batch during merge operation;
                if (flagDuringMerge) {
                    mergeOperationTupleProbing(leftStreamQueue, rightStreamQueue);
                    flagDuringMerge = false;
                }
                // Probing Result
                String streamID = tuple.getSourceStreamId();
                lookUpOperation(tuple, streamID);
            }
        }
        /*
        This is for left permutation computation input tuple is added into this array here true is indicator it is left
         */
        if (tuple.getSourceStreamId().equals(leftStreamPermutation)) {
            permutationComputation(tuple, true);
//            mapLeftPermutation.put(tuple.getIntegerByField(Constants.IDENTIFIER),listLeftPermutation);
//            listLeftPermutation= new ArrayList<>();
        }
        /*
        This is for right stream permutation computation input stream tuple will add in this array
         */
        if (tuple.getSourceStreamId().equals(rightSteamPermutation)) {
            permutationComputation(tuple, false);
//            mapRightPermutation.put(tuple.getIntegerByField(Constants.IDENTIFIER),listRightPermutation);
//            listRightPermutation= new ArrayList<>();
        }
        /*
        This is for left stream offset computation input stream tuple will add in this array
         */
        if (tuple.getSourceStreamId().equals(leftStreamOffset)) {
            offsetComputation(tuple, true);
//            mapLeftOffset.put(tuple.getIntegerByField(Constants.IDENTIFIER),listLeftOffset);
//            listLeftOffset= new ArrayList<>();
        }
        /*
        This is for right stream offset computation input stream tuple will add in this array
         */
        if (tuple.getSourceStreamId().equals(rightSteamOffset)) {
            offsetComputation(tuple, false);

//            mapRightOffset.put(tuple.getIntegerByField(Constants.IDENTIFIER),listRightOffset);
//            listRightOffset= new ArrayList<>();
        }
        if (checkConditionForAllPermutationAndOffsetArrays(isLeftStreamPermutation, isRightStreamPermutation, isLeftStreamOffset, isRightStreamOffset)) {
            List<Integer> idList = new ArrayList<>(mapLeftPermutation.keySet());
            for(int id:idList) {
                if(mapRightPermutation.containsKey(id)&&mapLeftOffset.containsKey(id)&&mapRightOffset.containsKey(id)){
                   System.out.println(mapLeftPermutation.get(id).size()+"Left Permutation ");
                    System.out.println(mapRightPermutation.get(id).size()+"Right Permutation ");
                    System.out.println(mapLeftOffset.get(id).size()+"The mapLeft");
                    System.out.println(mapRightOffset.get(id).size()+"The map Right Offser");
                   // System.exit(-1);
                    ieJoinModel.setLeftStreamPermutation(mapLeftPermutation.get(id));
                    ieJoinModel.setRightStreamPermutation(mapRightPermutation.get(id));
                    ieJoinModel.setRightStreamOffset(mapRightOffset.get(id));
                    ieJoinModel.setLeftStreamOffset(mapLeftOffset.get(id));
                    ieJoinModel.setTupleRemovalCounter(mapLeftPermutation.get(id).size() + mapRightPermutation.get(id).size());
                    this.linkedListIEJoinModel.add(ieJoinModel);
                    System.out.println(linkedListIEJoinModel.size()+"===LinkedListSize");
                    ieJoinModel = new IEJoinModel();
                    mapLeftPermutation.remove(id);
                    mapRightPermutation.remove(id);
                    mapRightOffset.remove(id);
                    mapLeftOffset.remove(id);
            }}
            // Tuple Removing threshold also set it to the according for the Asynchronize window

            this.isLeftStreamOffset = false;
            this.isRightStreamOffset = false;
            this.isRightStreamPermutation = false;
            this.isLeftStreamPermutation = false;
            this.listLeftPermutation = new ArrayList<>();
            this.listRightPermutation = new ArrayList<>();
            this.listLeftOffset = new ArrayList<>();
            this.listRightOffset = new ArrayList<>();
            if(!linkedListIEJoinModel.isEmpty())
            if(linkedListIEJoinModel.getFirst().getTupleRemovalCounter()>=Constants.IMMUTABLE_WINDOW_SIZE){
                System.out.println("Tuples are removing"+linkedListIEJoinModel.size());
                linkedListIEJoinModel.remove(linkedListIEJoinModel.getFirst());
                System.out.println("Tuples are removed"+linkedListIEJoinModel.size());
            }


        }
        /*
        To Do Use queues for holding incoming tuples for completeness;
         */
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
// Give proper name to the fields
    private void lookUpOperation(Tuple tuple, String streamID) {
        tupleRemovalCounter++;
        // If tuple is from left predicate then it perform operation for evaluation of tuples from right
        if (streamID.equals(leftStreamID)) {

            rightPredicateEvaluation(tuple.getIntegerByField(R1), tuple.getIntegerByField(R2));
        } else {

            // If tuple is from right predicate then it perform operation for evaluation of tuples from left
            leftPredicateEvaluation(tuple.getIntegerByField(S1), tuple.getIntegerByField(S2));
        }
        // bulk removal;
        // Re initialise the data to for freeing the memory
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

     */
    private void permutationComputation(Tuple tuple, boolean flag) {
        if (flag) {
            if (tuple.getValueByField(Constants.BATCH_COMPLETION_FLAG).equals(true)) {
                isLeftStreamPermutation = true;
                mapLeftPermutation.put(tuple.getIntegerByField(Constants.IDENTIFIER),listLeftPermutation);
                listLeftPermutation= new ArrayList<>();

            } else {
                listLeftPermutation.add(new Permutation(tuple.getIntegerByField(Constants.PERMUTATION_COMPUTATION_INDEX)));
                // permutationArrayList.add(new Permutation(tuple.getIntegerByField(Constants.PERMUTATION_COMPUTATION_INDEX)));
            }
        } else {
            if (tuple.getValueByField(Constants.BATCH_COMPLETION_FLAG).equals(true)) {
                  isRightStreamPermutation = true;
                mapRightPermutation.put(tuple.getIntegerByField(Constants.IDENTIFIER),listRightPermutation);
                listRightPermutation= new ArrayList<>();
            } else {
                listRightPermutation.add(new Permutation(tuple.getIntegerByField(Constants.PERMUTATION_COMPUTATION_INDEX)));
                // permutationArrayList.add(new Permutation(tuple.getIntegerByField(Constants.PERMUTATION_COMPUTATION_INDEX)));
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

     */

    private void offsetComputation(Tuple tuple, boolean flag) {
        if (flag) {
            if (tuple.getValueByField(Constants.BATCH_COMPLETION_FLAG).equals(true)) {
                isLeftStreamOffset = true;
                mapLeftOffset.put(tuple.getIntegerByField(Constants.IDENTIFIER),listLeftOffset);
               System.out.println("lis+Left==="+listLeftOffset.size());
                testingCounter=0;
              listLeftOffset= new ArrayList<>();
                // Left Stream offset now in map
              //  this.ieJoinModel.setLeftStreamOffset(offsetArrayList);
            } else {


                byte[] presenceBit = tuple.getBinaryByField(Constants.BYTE_ARRAY);
                BitSet presenceBitSetConversion = convertToObject(presenceBit);
                listLeftOffset.add(new Offset(tuple.getIntegerByField(Constants.TUPLE), tuple.getIntegerByField(Constants.OFFSET_TUPLE_INDEX),
                        presenceBitSetConversion, tuple.getIntegerByField(Constants.OFFSET_SIZE_OF_TUPLE)));
                testingCounter=testingCounter+1;
            }
        } else {
            if (tuple.getValueByField(Constants.BATCH_COMPLETION_FLAG).equals(true)) {
                        // Right Stream Offset now in map
                //this.ieJoinModel.setRightStreamOffset(offsetArrayList);
                isRightStreamOffset = true;
                System.out.println("lis+Right"+listRightOffset.size());
                mapRightOffset.put(tuple.getIntegerByField(Constants.IDENTIFIER),listRightOffset);

                listRightOffset= new ArrayList<>();

            } else {
                byte[] presenceBit = tuple.getBinaryByField(Constants.BYTE_ARRAY);
                BitSet presenceBitSetConversion = convertToObject(presenceBit);
                listRightOffset.add(new Offset(tuple.getIntegerByField(Constants.TUPLE), tuple.getIntegerByField(Constants.OFFSET_TUPLE_INDEX),
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
    private SearchModel searchKeyForRightStream(ArrayList<Offset> offsetArrayList, int  key) {
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
            int id = 0;
            try {
                id = offsetArrayList.get(result).getKeyForSearch();
            } catch (IndexOutOfBoundsException e) {
                id = offsetArrayList.get(offsetArrayList.size() - 1).getKeyForSearch();
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
        for (IEJoinModel ieJoinModel : this.linkedListIEJoinModel) {

            ieJoinModel.setTupleRemovalCounter(ieJoinModel.getTupleRemovalCounter() + 1);
           // k

            SearchModel offsetSearchKeySecond = searchKeyForRightStream(ieJoinModel.getRightStreamOffset(), tuple2);
         //   System.out.println(offsetSearchKeySecond.getIndexPosition());
            BitSet bitSet = new BitSet();
            int count = 0;
            int index=offsetSearchKeySecond.getIndexPosition();
            if(index<0){
                index=0;
            }
            int offset = ieJoinModel.getRightStreamOffset().get(index).getIndex() - 1;

            offset = offset - 1;
            // System.exit(-1);
            if (offset >= 0) {

                for (int j = 0; j <= offset; j++) {

                    try {

                        bitSet.set(ieJoinModel.getRightStreamPermutation().get(j).getTupleIndexPermutation() - 1, true);
                    } catch (IndexOutOfBoundsException e) {
                        e.printStackTrace();
                    }
                }

                SearchModel offsetSearchKeyFirst = searchKeyForRightStream(ieJoinModel.getLeftStreamOffset(), tuple1);
               int indexTuple2=offsetSearchKeyFirst.getIndexPosition();
                if(indexTuple2<0){
                    indexTuple2=0;
                }
                Offset offset1 = ieJoinModel.getLeftStreamOffset().get(indexTuple2);
                int off = offset1.getIndex() - 1;
                int size = ieJoinModel.getLeftStreamPermutation().size();
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
    private void leftPredicateEvaluation(int  tuple1, int tuple2) {
// Linked List
        for (IEJoinModel ieJoinModel : this.linkedListIEJoinModel) {

            ieJoinModel.setTupleRemovalCounter(ieJoinModel.getTupleRemovalCounter() + 1);

            int offsetSearchKeySecond = searchKeyForLeftStream(ieJoinModel.getRightStreamOffset(), tuple2);
            if(offsetSearchKeySecond<0){
                offsetSearchKeySecond=0;
            }
            int count = 0;
            BitSet bitSet = new BitSet();
            for (int i = offsetSearchKeySecond; i < ieJoinModel.getLeftStreamPermutation().size(); i++) {

                try {
                    bitSet.set(ieJoinModel.getLeftStreamPermutation().get(i).getTupleIndexPermutation() - 1, true);
                } catch (IndexOutOfBoundsException e) {
                    e.printStackTrace();
                }
            }
            int offsetSearchKeyFirst = searchKeyForLeftStream(ieJoinModel.getLeftStreamOffset(), tuple1) - 1;
            // Checking the equalities
            for (int i = offsetSearchKeyFirst; i>-1; i--) {
                // Check the equality to find first not equal element while back tracing.
                if (ieJoinModel.getLeftStreamOffset().get(i).getKey() != ieJoinModel.getLeftStreamOffset().get(offsetSearchKeyFirst).getKey()) {
                    offsetSearchKeyFirst = i;
                    break;

                }
            }
            for (int i = offsetSearchKeyFirst; i >-1; i--) {
                if (bitSet.get(i)) {
                    count++;
                }
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
        for (IEJoinModel ieJoinModel : this.linkedListIEJoinModel) {
            // Here When use seperate window then increment differently
            ieJoinModel.setTupleRemovalCounter(ieJoinModel.getTupleRemovalCounter() + leftStreamQueue.size() + rightStreamQueue.size());
        }

        if (!leftStreamQueue.isEmpty()) {
            for (Tuple tuple : leftStreamQueue) {
                rightPredicateEvaluation(tuple.getIntegerByField("Duration"), tuple.getIntegerByField("Revenue"));
            }
        }
        if (!rightStreamQueue.isEmpty()) {
            // If tuple is from left predicate then it perform operation for evaluation of tuples from right
            for (Tuple tuple : rightStreamQueue) {
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





