package com.proposed.iejoinandbplustreebased;

import com.configurationsandconstants.iejoinandbaseworks.Configuration;
import com.configurationsandconstants.iejoinandbaseworks.Constants;
import com.experiment.selfjoin.iejoinproposed.IEJoin;
import com.stormiequality.BTree.Offset;
import com.stormiequality.join.Permutation;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.*;
import java.net.InetAddress;
import java.util.*;

public class IEJoinWithLinkedListUpdated extends BaseRichBolt {
    private boolean isLeftStreamOffset;
    private boolean isRightStreamOffset;
    private boolean isLeftStreamPermutation;
    private boolean isRightStreamPermutation;
    private ArrayList<Permutation> listLeftPermutation;
    private ArrayList<Permutation> listRightPermutation;
    private String leftStreamPermutation;
    private String rightSteamPermutation;
    private ArrayList<Offset> listLeftOffset;
    private ArrayList<Offset> listRightOffset;
    private String leftStreamOffset;
    private String rightSteamOffset;
    //global window count for parallel tasks;
    private static int globalWindowCount;
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
    //
    private boolean globalCountFlag;

    private String R1;
    private String R2;
    private String S1;
    private String S2;
    //

    //Map for Left Permutation
    private HashMap<Integer, ArrayList<Permutation>> mapLeftPermutation;
    // Map for Right Permutation
    private HashMap<Integer, ArrayList<Permutation>> mapRightPermutation;
    //map for Left offset
    private HashMap<Integer, ArrayList<Offset>> mapLeftOffset;
    // Map for Right Offset
    private HashMap<Integer, ArrayList<Offset>> mapRightOffset;


    private OutputCollector outputCollector;
    private long mergingTime;
    private int taskID;
    private String hostName;
    private String mergingTuplesRecord;
    private String recordIEJoinStreamID;
    private String mergeRecordEvaluationStreamID;

    /// File Writer: Writing
    private int recordIEJoinCounter;
    private StringBuilder recordIEJoinStringBuilder;
    private BufferedWriter bufferedWriterRecordIEJoin;
    //   private BufferedWriter bufferedWriterRecordMergingTuple;
    private BufferedWriter bufferedWriterRecordEvaluationTuple;


    public IEJoinWithLinkedListUpdated(String R1, String R2, String S1, String S2) {
        Map<String, Object> map = Configuration.configurationConstantForStreamIDs();
        this.mergeOperationStreamID = (String) map.get("MergingFlag");
        this.leftStreamID = (String) map.get("LeftPredicateTuple");
        this.rightStreamID = (String) map.get("RightPredicateTuple");
        this.leftStreamOffset = (String) map.get("LeftBatchOffset");
        this.rightSteamOffset = (String) map.get("RightBatchOffset");
        this.leftStreamPermutation = (String) map.get("LeftBatchPermutation");
        this.rightSteamPermutation = (String) map.get("RightBatchPermutation");
        this.mergingTuplesRecord = (String) map.get("MergingTuplesRecord");
        this.recordIEJoinStreamID = (String) map.get("IEJoinResult");
        this.mergeRecordEvaluationStreamID = (String) map.get("MergingTupleEvaluation");
        this.R1 = R1;
        this.R2 = R2;
        this.S1 = S1;
        this.S2 = S2;
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
        this.globalCountFlag = false;
        this.linkedListIEJoinModel = new LinkedList<>();
        this.mapLeftPermutation = new HashMap<>();
        this.mapRightPermutation = new HashMap<>();
        this.mapLeftOffset = new HashMap<>();
        this.mapRightOffset = new HashMap<>();
        this.tupleRemovalCounter = Constants.MUTABLE_WINDOW_SIZE;
        this.taskID = topologyContext.getThisTaskId();
        this.outputCollector = outputCollector;
        this.globalWindowCount = 0;
        try {
            this.recordIEJoinStringBuilder = new StringBuilder();
            this.hostName = InetAddress.getLocalHost().getHostName();
            // writing files
            this.recordIEJoinCounter = 0;
            this.bufferedWriterRecordIEJoin = new BufferedWriter(new FileWriter(new File("/home/adeel/Data/Results/" + taskID + "bufferedWriterRecordIEJoin.csv")));
            this.bufferedWriterRecordEvaluationTuple = new BufferedWriter(new FileWriter(new File("/home/adeel/Data/Results/" + taskID + "bufferedWriterRecordEvaluationTupleMerge.csv")));
            this.bufferedWriterRecordIEJoin.write("ID,KafkaTime,Kafka_SPOUT_TIME, TupleArrivalTime, TupleEvaluationTime, stream_ID,Task, Host \n");
            this.bufferedWriterRecordIEJoin.flush();
            this.bufferedWriterRecordEvaluationTuple.write("MergeStartTime,MergeEndTime,EvaluatedTime,Task, Host\n");
            this.bufferedWriterRecordEvaluationTuple.flush();

        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    @Override
    public void execute(Tuple tuple) {

        if (tuple.getSourceStreamId().equals(mergeOperationStreamID)) {
            this.flagDuringMerge = tuple.getBooleanByField(Constants.MERGING_OPERATION_FLAG);
            this.mergingTime = tuple.getLongByField(Constants.MERGING_START_TIME);
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
                //  String streamID = tuple.getSourceStreamId();
                lookUpOperation(tuple, tuple.getSourceStreamId());
                if ((linkedListIEJoinModel.getFirst().getTupleRemovalCounter())  >= Constants.IMMUTABLE_WINDOW_SIZE) {
                    linkedListIEJoinModel.remove(linkedListIEJoinModel.getFirst());
                }

            }


        }
        /// State Management
        if (tuple.getSourceStreamId().equals("WindowCount")) {

                globalWindowCount = globalWindowCount + tuple.getInteger(0);

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
        if (checkConditionForAllPermutationAndOffsetArrays(isLeftStreamPermutation, isRightStreamPermutation, isLeftStreamOffset, isRightStreamOffset)) {
            long startTime = System.currentTimeMillis();
            // Retrieve the list of keys from the left permutation map
            List<Integer> idList = new ArrayList<>(mapLeftPermutation.keySet());

            for (int id : idList) {
                if (mapRightPermutation.containsKey(id) && mapLeftOffset.containsKey(id) && mapRightOffset.containsKey(id)) {
                    // Print sizes for debugging purposes
//                    System.out.println(mapLeftPermutation.get(id).size() + " Left Permutation ");
//                    System.out.println(mapRightPermutation.get(id).size() + " Right Permutation ");
//                    System.out.println(mapLeftOffset.get(id).size() + " The mapLeft");
//                    System.out.println(mapRightOffset.get(id).size() + " The map Right Offset");

                    // Create a new IEJoinModel and populate it with data
                    // A mehtod Call only for Sub Window Removal It is used For Removing the batch of tuples


                    IEJoinModel ieJoinModel = new IEJoinModel();
                    ieJoinModel.setLeftStreamPermutation(mapLeftPermutation.get(id));
                    ieJoinModel.setRightStreamPermutation(mapRightPermutation.get(id));
                    ieJoinModel.setRightStreamOffset(mapRightOffset.get(id));
                    ieJoinModel.setLeftStreamOffset(mapLeftOffset.get(id));
                    ieJoinModel.setTupleRemovalCounter(this.listLeftPermutation.size() + this.listRightPermutation.size()+globalWindowCount);
                   // reset the global Window Count
                    globalWindowCount=0;
                    // Add the IEJoinModel to the linked list
                    this.linkedListIEJoinModel.add(ieJoinModel);

                    // Reset the flags and lists
                    this.isLeftStreamOffset = false;
                    this.isRightStreamOffset = false;
                    this.isRightStreamPermutation = false;
                    this.isLeftStreamPermutation = false;

                    // Check if the linked list size exceeds the threshold and remove elements if necessary
//                    if ((linkedListIEJoinModel.getFirst().getTupleRemovalCounter())+globalWindowCount >= Constants.IMMUTABLE_WINDOW_SIZE) {
//                        linkedListIEJoinModel.remove(linkedListIEJoinModel.getFirst());
//                    }

                    long mergeStartTime = System.currentTimeMillis();
                    // Perform merge operation
                    mergeOperationTupleProbing(ieJoinModel);
                    long mergeEndTime = System.currentTimeMillis() - mergeStartTime;

                    // Emit relevant data
                    try {
                        bufferedWriterRecordEvaluationTuple.write(mergingTime + "," + startTime + "," + mergeEndTime + "," + taskID + "," + hostName + "\n");
                        bufferedWriterRecordEvaluationTuple.flush();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

//                    outputCollector.emit(mergeRecordEvaluationStreamID, tuple, new Values(mergingTime, startTime, mergeEndTime, taskID, hostName));
//                    outputCollector.ack(tuple);

                    // Reset flags, queues, and remove data from maps
                    flagDuringMerge = false;
                    leftStreamQueue = new LinkedList<>();
                    rightStreamQueue = new LinkedList<>();
                    mapLeftOffset.remove(id);
                    mapRightOffset.remove(id);
                    mapLeftPermutation.remove(id);
                    mapRightPermutation.remove(id);
                }
            }
        }

        /*
        To Do Use queues for holding incoming tuples for completeness;
         */
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(recordIEJoinStreamID, new Fields("ID", "KafkaTupleReadingTime", "KafkaTime", "Time", "Com_Time", "stream_id", "taskId", "hostName"));

        outputFieldsDeclarer.declareStream(mergeRecordEvaluationStreamID, new Fields("StartMergeTime", "EndMergeTime", "Time", "taskId", "hostName"));
    }

    private void lookUpOperation(Tuple tuple, String streamID) {
        long kafkaReadingTime = tuple.getLongByField(Constants.KAFKA_TIME);
        long KafkaSpoutTime = tuple.getLongByField(Constants.KAFKA_SPOUT_TIME);
        int tupleID = tuple.getIntegerByField(Constants.TUPLE_ID);
        long tupleArrivalTime = System.currentTimeMillis();

        tupleRemovalCounter++;
        // If tuple is from left predicate then it perform operation for evaluation of tuples from right
        if (streamID.equals(leftStreamID)) {

            rightPredicateEvaluation(tuple.getIntegerByField(R1), tuple.getIntegerByField(R2));
        } else {

            // If tuple is from right predicate then it perform operation for evaluation of tuples from left
            leftPredicateEvaluation(tuple.getIntegerByField(S1), tuple.getIntegerByField(S2));
        }
        long tupleEvaluationTime = System.currentTimeMillis();
        //g
        // Record counter
        this.recordIEJoinCounter++;
        recordIEJoinStringBuilder.append(tupleID + "," + kafkaReadingTime + "," + KafkaSpoutTime + "," + tupleArrivalTime + "," + tupleEvaluationTime + "," + tuple.getSourceStreamId() + "," + taskID + "," + hostName + "\n");
//
        if (recordIEJoinCounter == 1000) {
            try {
                bufferedWriterRecordIEJoin.write(recordIEJoinStringBuilder.toString());
                bufferedWriterRecordIEJoin.flush();
                recordIEJoinStringBuilder = new StringBuilder();
                recordIEJoinCounter = 0;
            } catch (Exception e) {
                e.printStackTrace();
            }
       }
//
//        outputCollector.emit(recordIEJoinStreamID, tuple,new Values(tupleID, kafkaReadingTime, KafkaSpoutTime, tupleArrivalTime, tupleEvaluationTime, tuple.getSourceStreamId(),taskID, hostName));
//        outputCollector.ack(tuple);

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
                mapLeftPermutation.put(tuple.getIntegerByField(Constants.IDENTIFIER), listLeftPermutation);
                //  this.ieJoinModel.setLeftStreamPermutation(permutationArrayList);
                this.listLeftPermutation = new ArrayList<>();


            } else {
                permutationArrayList.add(new Permutation(tuple.getIntegerByField(Constants.PERMUTATION_COMPUTATION_INDEX)));
            }
        } else {
            if (tuple.getValueByField(Constants.BATCH_COMPLETION_FLAG).equals(true)) {
                isRightStreamPermutation = true;
                mapRightPermutation.put(tuple.getIntegerByField(Constants.IDENTIFIER), listRightPermutation);
                this.listRightPermutation = new ArrayList<>();

                // this.ieJoinModel.setRightStreamPermutation(permutationArrayList);
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
                mapLeftOffset.put(tuple.getIntegerByField(Constants.IDENTIFIER), listLeftOffset);
                this.listLeftOffset = new ArrayList<>();

                // this.ieJoinModel.setLeftStreamOffset(offsetArrayList);
            } else {
                byte[] presenceBit = tuple.getBinaryByField(Constants.BYTE_ARRAY);
                BitSet presenceBitSetConversion = convertToObject(presenceBit);
                offsetArrayList.add(new Offset(tuple.getIntegerByField(Constants.TUPLE), tuple.getIntegerByField(Constants.OFFSET_TUPLE_INDEX),
                        presenceBitSetConversion, tuple.getIntegerByField(Constants.OFFSET_SIZE_OF_TUPLE)));
            }
        } else {
            if (tuple.getValueByField(Constants.BATCH_COMPLETION_FLAG).equals(true)) {
                isRightStreamOffset = true;
                mapRightOffset.put(tuple.getIntegerByField(Constants.IDENTIFIER), listRightOffset);
                this.listRightOffset = new ArrayList<>();
                //   this.ieJoinModel.setRightStreamOffset(offsetArrayList);
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
            SearchModel offsetSearchKeySecond = searchKeyForRightStream(ieJoinModel.getRightStreamOffset(), tuple2);
            BitSet bitSet = new BitSet();
            int count = 0;
            int offset = ieJoinModel.getRightStreamOffset().get(offsetSearchKeySecond.getIndexPosition()).getIndex() - 1;

            offset = offset - 1;
            // System.exit(-1);
            if (offset >= 0) {

                for (int j = 0; j <= offset; j++) {

                    try {
                        if ((ieJoinModel.getRightStreamPermutation().get(j).getTupleIndexPermutation() - 1) > 0)
                            bitSet.set(ieJoinModel.getRightStreamPermutation().get(j).getTupleIndexPermutation() - 1, true);
                    } catch (IndexOutOfBoundsException e) {
                        e.printStackTrace();
                    }
                }
                SearchModel offsetSearchKeyFirst = searchKeyForRightStream(ieJoinModel.getLeftStreamOffset(), tuple1);
                Offset offset1 = ieJoinModel.getLeftStreamOffset().get(offsetSearchKeyFirst.getIndexPosition());
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
    private void leftPredicateEvaluation(int tuple1, int tuple2) {
// Linked List
        for (IEJoinModel ieJoinModel : this.linkedListIEJoinModel) {

            ieJoinModel.setTupleRemovalCounter(ieJoinModel.getTupleRemovalCounter() + 1);
            int offsetSearchKeySecond = searchKeyForLeftStream(ieJoinModel.getRightStreamOffset(), tuple2);
            int count = 0;
            BitSet bitSet = new BitSet();
            for (int i = offsetSearchKeySecond; i < ieJoinModel.getLeftStreamPermutation().size(); i++) {

                try {
                    if ((ieJoinModel.getLeftStreamPermutation().get(i).getTupleIndexPermutation() - 1) > 0)
                        bitSet.set(ieJoinModel.getLeftStreamPermutation().get(i).getTupleIndexPermutation() - 1, true);
                } catch (IndexOutOfBoundsException e) {
                    e.printStackTrace();
                }
            }
            int offsetSearchKeyFirst = searchKeyForLeftStream(ieJoinModel.getLeftStreamOffset(), tuple1) - 1;
            // Checking the equalities
            for (int i = offsetSearchKeyFirst; i < -1; i--) {
                // Check the equality to find first not equal element while back tracing.
                if (ieJoinModel.getLeftStreamOffset().get(i).getKey() != ieJoinModel.getLeftStreamOffset().get(offsetSearchKeyFirst).getKey()) {
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
    }

    /**
     * probing
     */
    private void mergeOperationTupleProbing(IEJoinModel ieJoinModel) {


        //for (IEJoinModel ieJoinModel : this.linkedListIEJoinModel) {
        // Here When use seperate window then increment differently
        ieJoinModel.setTupleRemovalCounter(ieJoinModel.getTupleRemovalCounter() + leftStreamQueue.size() + rightStreamQueue.size());
        //  }

        if (!leftStreamQueue.isEmpty()) {
            for (Tuple tuple : leftStreamQueue) {
                rightPredicateEvaluationForMerge(ieJoinModel, tuple.getIntegerByField(R1), tuple.getIntegerByField(R2));


            }
        }
        if (!rightStreamQueue.isEmpty()) {
            // If tuple is from left predicate then it perform operation for evaluation of tuples from right
            for (Tuple tuple : rightStreamQueue) {
                leftPredicateEvaluationForMerge(ieJoinModel, tuple.getIntegerByField(S1), tuple.getIntegerByField(S2));
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

    private void rightPredicateEvaluationForMerge(IEJoinModel ieJoinModel, int tuple1, int tuple2) {


        ieJoinModel.setTupleRemovalCounter(ieJoinModel.getTupleRemovalCounter() + 1);
        SearchModel offsetSearchKeySecond = searchKeyForRightStream(ieJoinModel.getRightStreamOffset(), tuple2);
        BitSet bitSet = new BitSet();
        int count = 0;
        int offset = ieJoinModel.getRightStreamOffset().get(offsetSearchKeySecond.getIndexPosition()).getIndex() - 1;

        offset = offset - 1;
        // System.exit(-1);
        if (offset >= 0) {

            for (int j = 0; j <= offset; j++) {

                try {
                    if ((ieJoinModel.getRightStreamPermutation().get(j).getTupleIndexPermutation() - 1) > 0)
                        bitSet.set(ieJoinModel.getRightStreamPermutation().get(j).getTupleIndexPermutation() - 1, true);
                } catch (IndexOutOfBoundsException e) {
                    e.printStackTrace();
                }
            }
            SearchModel offsetSearchKeyFirst = searchKeyForRightStream(ieJoinModel.getLeftStreamOffset(), tuple1);
            Offset offset1 = ieJoinModel.getLeftStreamOffset().get(offsetSearchKeyFirst.getIndexPosition());
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

    private void leftPredicateEvaluationForMerge(IEJoinModel ieJoinModel, int tuple1, int tuple2) {
// Linked List


        ieJoinModel.setTupleRemovalCounter(ieJoinModel.getTupleRemovalCounter() + 1);
        int offsetSearchKeySecond = searchKeyForLeftStream(ieJoinModel.getRightStreamOffset(), tuple2);
        int count = 0;
        BitSet bitSet = new BitSet();
        for (int i = offsetSearchKeySecond; i < ieJoinModel.getLeftStreamPermutation().size(); i++) {

            try {
                if ((ieJoinModel.getLeftStreamPermutation().get(i).getTupleIndexPermutation() - 1) > 0)
                    bitSet.set(ieJoinModel.getLeftStreamPermutation().get(i).getTupleIndexPermutation() - 1, true);
            } catch (IndexOutOfBoundsException e) {
                e.printStackTrace();
            }
        }
        int offsetSearchKeyFirst = searchKeyForLeftStream(ieJoinModel.getLeftStreamOffset(), tuple1) - 1;
        // Checking the equalities
        for (int i = offsetSearchKeyFirst; i < -1; i--) {
            // Check the equality to find first not equal element while back tracing.
            if (ieJoinModel.getLeftStreamOffset().get(i).getKey() != ieJoinModel.getLeftStreamOffset().get(offsetSearchKeyFirst).getKey()) {
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

    public void subWindowMergingTupleRemoval() {
        int totalCount = globalWindowCount;
        for (int i = 0; i < linkedListIEJoinModel.size(); i++) {
            totalCount = totalCount + linkedListIEJoinModel.get(i).getTupleRemovalCounter();
            if (totalCount >= Constants.IMMUTABLE_WINDOW_SIZE) {
                linkedListIEJoinModel.removeFirst();
                globalWindowCount = globalWindowCount - Constants.MUTABLE_WINDOW_SIZE;
                break;
            }
        }
    }
}

