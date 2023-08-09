package com.experiment.selfjoin.iejoinproposed;

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

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.net.InetAddress;
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
    private LinkedHashMap<Integer, PermutationData> mapForHandlingOverflow;
    private int leftCountForOverflow;
    private int rightCountForOverflow;

    private BufferedWriter bufferedWriter;


    private OutputCollector outputCollector;
    private LinkedList<ArrayList<PermutationSelfJoin>> linkedList;
    /// Time Taken
    private long mergingTime;
    private int taskID;
    private String hostName;
    private String mergingTuplesRecord;
    private String recordIEJoinStreamID;
    private String mergeRecordEvaluationStreamID;
    private long timeForMergingLeftStream;
    private long timeForMergingRightStream;

    public IEJoin() {
        Map<String, Object> map = Configuration.configurationConstantForStreamIDs();
        this.permutationLeft = (String) map.get("LeftBatchPermutation");
        this.permutationRight = (String) map.get("RightBatchPermutation");
        this.leftStreamID = (String) map.get("StreamR");
        this.mergeOperationStreamID = (String) map.get("MergingFlag");
        this.recordIEJoinStreamID = (String) map.get("IEJoinResult");
        this.mergingTuplesRecord = (String) map.get("MergingTuplesRecord");
        this.mergeRecordEvaluationStreamID = (String) map.get("MergingTupleEvaluation");
        this.mergingTime = 0l;
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
        this.taskID = topologyContext.getThisTaskId();
        this.mapForHandlingOverflow = new LinkedHashMap<>();
        this.leftCountForOverflow = 0;
        this.rightCountForOverflow = 0;

        try {
            // bufferedWriter= new BufferedWriter(new FileWriter(new File("/home/adeel/Data/Results/IEJoin.csv")));
            this.hostName = InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            e.printStackTrace();
        }
        this.outputCollector = outputCollector;

    }

    @Override
    public void execute(Tuple tuple) {
        if (tuple.getSourceStreamId().equals(mergeOperationStreamID)) {

            flagDuringMerge = tuple.getBooleanByField(Constants.MERGING_OPERATION_FLAG);
            this.mergingTime = tuple.getLongByField(Constants.MERGING_TIME);
        }
        if ((tuple.getSourceStreamId().equals("StreamR"))) {


            if (flagDuringMerge) {
                this.queueDuringMerge.offer(tuple);
            }

            if (!linkedList.isEmpty()) {
                tupleRemovalCounter++;
                resultComputation(tuple);
            }
        }


        if (permutationLeft.equals(tuple.getSourceStreamId())) {


            if (tuple.getBooleanByField(Constants.BATCH_COMPLETION_FLAG) == true) {
                PermutationData permutationData = new PermutationData();
                leftCountForOverflow++;
                isLeftStreamPermutation = true;
                // timeForMergingLeftStream=tuple.getLongByField(Constants.MERGING_TIME);

                if (mapForHandlingOverflow.containsKey(leftCountForOverflow)) {
                    PermutationData permutationDataOverFlow = mapForHandlingOverflow.get(leftCountForOverflow);
                    permutationDataOverFlow.setListLeftPermutation(listLeftPermutation);
                    permutationDataOverFlow.setMergingTimeForEndTuple(tuple.getLongByField(Constants.MERGING_TIME));
                    mapForHandlingOverflow.replace(leftCountForOverflow, permutationDataOverFlow);
                } else {
                    permutationData.setMergingTimeForEndTuple(tuple.getLongByField(Constants.MERGING_TIME));
                    permutationData.setListLeftPermutation(listLeftPermutation);
                    mapForHandlingOverflow.put(leftCountForOverflow, permutationData);
                }
                listLeftPermutation = new ArrayList<>();
                /// isLeftStreamPermutation = false;
            } else {
                //  List<Integer> ids = convertToIntegerList((byte[]) tuple.getValueByField(Constants.PERMUTATION_TUPLE_IDS));
                int ids = tuple.getIntegerByField(Constants.PERMUTATION_TUPLE_IDS);
                listLeftPermutation.add(new Permutation(tuple.getIntegerByField(Constants.TUPLE), ids));
            }
        }
        // Tuple from right stream for compute permutation
        if (permutationRight.equals(tuple.getSourceStreamId())) {

            if (tuple.getBooleanByField(Constants.BATCH_COMPLETION_FLAG) == true) {
                PermutationData permutationData = new PermutationData();
                rightCountForOverflow++;
                isRightStreamPermutation = true;

                if (mapForHandlingOverflow.containsKey(rightCountForOverflow)) {
                    PermutationData permutationDataOverFlow = mapForHandlingOverflow.get(rightCountForOverflow);
                    permutationDataOverFlow.setListRightPermutation(listRightPermutation);
                    permutationDataOverFlow.setMergingTimeForEndTuple(tuple.getLongByField(Constants.MERGING_TIME));
                    mapForHandlingOverflow.replace(rightCountForOverflow, permutationDataOverFlow);
                } else {
                    permutationData.setMergingTimeForEndTuple(tuple.getLongByField(Constants.MERGING_TIME));
                    permutationData.setListRightPermutation(listRightPermutation);
                    mapForHandlingOverflow.put(rightCountForOverflow, permutationData);
                }
                listRightPermutation = new ArrayList<>();

                /// isRightStreamPermutation = false;
            } else {
                //List<Integer> ids = convertToIntegerList((byte[]) tuple.getValueByField(Constants.PERMUTATION_TUPLE_IDS));
                int ids = tuple.getIntegerByField(Constants.PERMUTATION_TUPLE_IDS);
                listRightPermutation.add(new Permutation(tuple.getIntegerByField(Constants.TUPLE), ids));
            }
        }
        if (!mapForHandlingOverflow.isEmpty()) {
            Iterator<Integer> iterator = mapForHandlingOverflow.keySet().iterator();

            while (iterator.hasNext()) {
                int key = iterator.next();
                PermutationData permutationDataForMap = mapForHandlingOverflow.get(key);
                if (permutationDataForMap.getListLeftPermutation() != null && permutationDataForMap.getListRightPermutation() != null) {
                    permutationComputation(permutationDataForMap.getListLeftPermutation(), permutationDataForMap.getListRightPermutation(), permutationDataForMap.getMergingTimeForEndTuple());
                    iterator.remove(); // Use the iterator's remove method to safely remove the element from the map
                    isLeftStreamPermutation = false;
                    isRightStreamPermutation = false;
                }
            }
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(mergingTuplesRecord, new Fields("Time", "MergeTime", "BeforeTimePermutation", "Com_Time", "taskId", "hostName"));
        outputFieldsDeclarer.declareStream(recordIEJoinStreamID, new Fields("ID", "KafkaTupleReadingTime", "KafkaTime", "Time", "Com_Time", "taskId", "hostName"));
        outputFieldsDeclarer.declareStream(mergeRecordEvaluationStreamID, new Fields("Time", "taskId", "hostName"));

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

    private synchronized void permutationComputation(ArrayList<Permutation> listLeftPermutation, ArrayList<Permutation> listRightPermutation, long timeForMergingLeftStream) {

        long timeBeforePermutationComputation = System.currentTimeMillis();
        ArrayList<PermutationSelfJoin> listPermutationSelfJoin = new ArrayList<>();
        int[] holdingList = new int[20000000];
        int counter = 1;
        for (int i = 0; i < listLeftPermutation.size(); i++) {
            //for (int ids : listLeftPermutation.get(i).getListOfIDs()) {
            try {
                holdingList[listLeftPermutation.get(i).getId()] = counter;
            } catch (Exception e) {
                holdingList[listLeftPermutation.size() - 1] = counter;

            }
            counter++;

        }

        for (int i = 0; i < listRightPermutation.size(); i++) {
            try {
                int ids = listRightPermutation.get(i).getId();
                listPermutationSelfJoin.add(new PermutationSelfJoin(holdingList[ids], listRightPermutation.get(i).getTuple(), listLeftPermutation.get(i).getTuple()));
            } catch (IndexOutOfBoundsException e) {
                listPermutationSelfJoin.add(new PermutationSelfJoin(holdingList[listRightPermutation.size() - 1], listRightPermutation.get(i).getId(), listLeftPermutation.get(i).getId()));

            }
        }

        long time = System.currentTimeMillis();

        linkedList.add(listPermutationSelfJoin);

        // Tuple Merge Evaluation: // Queue evaluation for the last item of linked list
        long timeDuringMerge = System.currentTimeMillis();
        for (Tuple tuple : queueDuringMerge) {
            bitSetEvaluation(linkedList.getLast(), tuple);
        }
        timeDuringMerge = System.currentTimeMillis() - timeDuringMerge;
        if (tupleRemovalCounter >= Constants.IMMUTABLE_WINDOW_SIZE) {
            linkedList.remove(linkedList.getFirst());
            tupleRemovalCounter = Constants.MUTABLE_WINDOW_SIZE;

        }
        queueDuringMerge = new LinkedList<>();
        //   System.out.print(mergingTime+"=="+time+"=="+taskID+"=="+hostName);
        outputCollector.emit(mergingTuplesRecord, new Values(mergingTime, timeForMergingLeftStream, timeBeforePermutationComputation, time, taskID, hostName));
        //   System.out.print(timeDuringMerge+"=="+taskID+"=="+hostName);
        outputCollector.emit(mergeRecordEvaluationStreamID, new Values(timeDuringMerge, taskID, hostName));

    }

    public synchronized void resultComputation(Tuple tuple) {
        long kafkaReadingTime = tuple.getLongByField("kafkaTime");
        long KafkaSpoutTime = tuple.getLongByField("Time");
        int tupleID = tuple.getIntegerByField("ID");
        long tupleArrivalTime = System.currentTimeMillis();
        for (int i = 0; i < linkedList.size(); i++) {

            bitSetEvaluation(linkedList.get(i), tuple);
        }
        long tupleEvaluationTime = System.currentTimeMillis();

        outputCollector.emit(recordIEJoinStreamID, tuple,new Values(tupleID, kafkaReadingTime, KafkaSpoutTime, tupleArrivalTime, tupleEvaluationTime, taskID, hostName));
        outputCollector.ack(tuple);
    }

    public void bitSetEvaluation(ArrayList<PermutationSelfJoin> listPermutationSelfJoin, Tuple tuple) {
        BitSet bitSet = new BitSet(listPermutationSelfJoin.size());

        int indexWithRight = binarySearchForIndexWithFlag(listPermutationSelfJoin, tuple.getInteger(1));
        int indexWithLeft = binarySearchWithIndex(listPermutationSelfJoin, tuple.getInteger(0));

        try {
            if (indexWithRight > -1)
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

        return result;
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

    public void setMapForHandlingOverflow(HashMap<Integer, PermutationData> mapForHandlingOverflow) {


    }
}
