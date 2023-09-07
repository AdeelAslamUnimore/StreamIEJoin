package com.proposed.iejoinandbplustreebased;

import com.configurationsandconstants.iejoinandbaseworks.Configuration;
import com.configurationsandconstants.iejoinandbaseworks.Constants;
import com.stormiequality.BTree.BPlusTree;
import com.stormiequality.BTree.BPlusTreeWithTmpForPermutation;
import com.stormiequality.BTree.Node;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.util.BitSet;
import java.util.List;
import java.util.Map;

public class MutableBPlusTreeBolt extends BaseRichBolt {
    //left stream for MutableBPlusTree
    private BPlusTreeWithTmpForPermutation leftStreamBPlusTree;
    // right stream for MutableBPlusTree
    private BPlusTreeWithTmpForPermutation rightStreamBPlusTree;
    // Order of Trees
    //private mergeIntervalCount;
    private int mergeIntervalCounter;
    //Merge Interval provided by user
    private int mergeIntervalDefinedByUser;
    // operator that need to perform
    private String operator;
    // Permutation Computation Stream ID
    private String permutationComputationStreamID;
    //Offset Computation  Stream ID
    private String offsetComputationStreamID;
    // Output collector
    private OutputCollector outputCollector;
    // leftPredicateID
    private String leftPredicateBitSetStreamID = null;
    // RightPredicateID
    private String rightPredicateBitSetStreamID = null;
    // Left StreamID
    private String leftStreamSmaller = null;
    //Right StreamID
    private String rightStreamSmaller = null;


    private String leftStreamGreater = null;
    //Right StreamID
    private String rightStreamGreater = null;
    // Right batch permutation
    //List Permutation Down Stream Tasks the size is fixed 2
    // 0 for left 1 for right
    private List<Integer> downStreamTaskIdsForPermutation;
    // ID for downStream Tasks
    private int idForDownStreamTasksOffset;
    // List of all tasks that holds IE Join tasks
    private List<Integer> downStreamTasksForOffset;
    // Merge operation Initiator
    private String mergeOperationStreamID;
    // tmpCounterForPermutationArrayForStreamR
    private int tmpIDForPermutationForStreamR;
    // tmpCounterForPermutationForStreamS
    private int tmpIDForPermutationForStreamS;
    // Tuple Arrival Time
    private long tupleArrivalTime;
    // Task ID that process this tuple
    private int taskID;
    // Host Name
    private String hostName;

    /**
     * constructor that holds the variables
     *
     * @param operator <, >  define the operator for operation
     * @PermtuationStreamID is the stream id provided by parameters however, it exists in configuration file
     * @OffsetStreamID is the streamID is also provided by parameter from configuration file.
     */
    public MutableBPlusTreeBolt(String operator, String permutationStreamID, String offsetStreamID) {
        this.operator = operator;
        this.mergeIntervalDefinedByUser = Constants.MUTABLE_WINDOW_SIZE;
        this.permutationComputationStreamID = permutationStreamID;
        this.offsetComputationStreamID = offsetStreamID;
        Map<String, Object> map = Configuration.configurationConstantForStreamIDs();
        this.leftStreamSmaller = (String) map.get("LeftSmallerPredicateTuple");
        this.rightStreamSmaller = (String) map.get("RightSmallerPredicateTuple");
        this.leftStreamGreater = (String) map.get("LeftGreaterPredicateTuple");
        this.rightStreamGreater = (String) map.get("RightGreaterPredicateTuple");
        this.leftPredicateBitSetStreamID = (String) map.get("LeftPredicateSourceStreamIDBitSet");
        this.mergeOperationStreamID = (String) map.get("MergingFlag");
        this.rightPredicateBitSetStreamID = (String) map.get("RightPredicateSourceStreamIDBitSet");


    }

    /**
     * order of Bplus tree is obtained from configuration file
     *
     * @param map
     * @param topologyContext
     * @param outputCollector
     */
    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        try {
            leftStreamBPlusTree = new BPlusTreeWithTmpForPermutation(Constants.ORDER_OF_B_PLUS_TREE);
            rightStreamBPlusTree = new BPlusTreeWithTmpForPermutation(Constants.ORDER_OF_B_PLUS_TREE);
            downStreamTaskIdsForPermutation = topologyContext.getComponentTasks(Constants.PERMUTATION_COMPUTATION_BOLT_ID);
            downStreamTasksForOffset = topologyContext.getComponentTasks(Constants.OFFSET_AND_IE_JOIN_BOLT_ID);
            this.idForDownStreamTasksOffset = 0;
            this.outputCollector = outputCollector;
            this.taskID=topologyContext.getThisTaskId();
            this.hostName= InetAddress.getLocalHost().getHostName();

        } catch (Exception e) {

        }
    }

    @Override
    public void execute(Tuple tuple) {
        // Keep track for window limit
        mergeIntervalCounter++;
        try {
            // Right and left predicate evaluation It can be optimized depending on the condition it can be customized
            if (operator.equals("<")) {
            tupleArrivalTime= System.currentTimeMillis();
                lessPredicateEvaluation(tuple);
            }
            if (operator.equals(">")) {
            tupleArrivalTime= System.currentTimeMillis();
                greaterPredicateEvaluation(tuple);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        // Check condition for window limit
        if (mergeIntervalCounter >= mergeIntervalDefinedByUser) {
            mergingOfMutableStructureForImmutableDataStructure(tuple);

        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        //Left part of predicate
        outputFieldsDeclarer.declareStream(leftPredicateBitSetStreamID, new Fields(Constants.BYTE_ARRAY, Constants.TUPLE_ID));
        //Right part of predicate
        outputFieldsDeclarer.declareStream(rightPredicateBitSetStreamID, new Fields(Constants.BYTE_ARRAY, Constants.TUPLE_ID));
        //Permutation Array
        outputFieldsDeclarer.declareStream(permutationComputationStreamID, new Fields(Constants.TUPLE, Constants.PERMUTATION_TUPLE_IDS, Constants.BATCH_COMPLETION_FLAG));
        //Offset Array
        outputFieldsDeclarer.declareStream(offsetComputationStreamID, new Fields(Constants.TUPLE, Constants.OFFSET_TUPLE_INDEX, Constants.BYTE_ARRAY, Constants.OFFSET_SIZE_OF_TUPLE, Constants.BATCH_COMPLETION_FLAG));
        // Merge Operation
        outputFieldsDeclarer.declareStream(mergeOperationStreamID, new Fields(Constants.MERGING_OPERATION_FLAG));
    }

    // Completely evaluate the < predicate here
    private void lessPredicateEvaluation(Tuple tuple) {
        if (tuple.getSourceStreamId().equals(leftStreamSmaller)) {
            tmpIDForPermutationForStreamR++;
            // Inserting the tuple into the BTree location;
            leftStreamBPlusTree.insert(tuple.getIntegerByField(Constants.TUPLE), tuple.getIntegerByField(Constants.TUPLE_ID),tmpIDForPermutationForStreamR);
            // Evaluating Tuples from right stream BPlus Tree.
            BitSet bitSet = rightStreamBPlusTree.greaterThenSpecificValue(tuple.getIntegerByField(Constants.TUPLE));
            if (bitSet != null) {
                try {
                    byte[] bytArrayRBitSet = convertToByteArray(bitSet);
                    // Only emitting the bit array  tuple is due to acking mechanisim
                    this.outputCollector.emit(leftPredicateBitSetStreamID, tuple, new Values(bytArrayRBitSet, tuple.getIntegerByField(Constants.TUPLE_ID)));
                    this.outputCollector.ack(tuple);
                    // Emitting Logic for tuples

                } catch (IOException e) {

                }
            }

        }

        if (tuple.getSourceStreamId().equals(rightStreamSmaller)) {
            // Insert into the other stream
            tmpIDForPermutationForStreamS++;
            rightStreamBPlusTree.insert(tuple.getIntegerByField(Constants.TUPLE), tuple.getIntegerByField(Constants.TUPLE_ID),tmpIDForPermutationForStreamS);
            // Evaluation of the query
            BitSet bitSet = leftStreamBPlusTree.lessThenSpecificValue(tuple.getIntegerByField(Constants.TUPLE));
            //
            if (bitSet != null) {
                try {
                    byte[] bytArrayLBitSet = convertToByteArray(bitSet);
                    // Emitting Tuple logic
                    this.outputCollector.emit(rightPredicateBitSetStreamID, tuple, new Values(bytArrayLBitSet, tuple.getIntegerByField(Constants.TUPLE_ID)));
                    this.outputCollector.ack(tuple);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        }

    }

    // Completely evaluate the > predicte right part of query predicate
    // leftStream: this leftStream depicts the left tuple from right part of predicate
    public void greaterPredicateEvaluation(Tuple tuple) {
        if (tuple.getSourceStreamId().equals(leftStreamGreater)) {
            tmpIDForPermutationForStreamR++;
            leftStreamBPlusTree.insert(tuple.getIntegerByField(Constants.TUPLE), tuple.getIntegerByField(Constants.TUPLE_ID),tmpIDForPermutationForStreamR);
            BitSet bitSet = rightStreamBPlusTree.lessThenSpecificValue(tuple.getIntegerByField(Constants.TUPLE));
            if (bitSet != null) {
                try {
                    byte[] bytArrayRBitSet = convertToByteArray(bitSet);
                    //Emit logic here tuple emitting
                    this.outputCollector.emit(leftPredicateBitSetStreamID, tuple, new Values(bytArrayRBitSet, tuple.getIntegerByField(Constants.TUPLE_ID),
                            tuple.getLongByField(Constants.KAFKA_TIME), tuple.getLongByField(Constants.KAFKA_SPOUT_TIME), tuple.getLongByField(Constants.SPLIT_BOLT_TIME), tuple.getValueByField(Constants.TASK_ID_FOR_SPLIT_BOLT),
                            tuple.getStringByField(Constants.HOST_NAME_FOR_SPLIT_BOLT), tupleArrivalTime, System.currentTimeMillis(), this.taskID, hostName));
                    this.outputCollector.ack(tuple);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        // rightStream: this rightStream depicts the right tuple from right part of predicate
        if (tuple.getSourceStreamId().equals(rightStreamGreater)) {
            tmpIDForPermutationForStreamS++;
            rightStreamBPlusTree.insert(tuple.getIntegerByField(Constants.TUPLE), tuple.getIntegerByField(Constants.TUPLE_ID),tmpIDForPermutationForStreamS);
            BitSet bitSet = leftStreamBPlusTree.greaterThenSpecificValue(tuple.getIntegerByField(Constants.TUPLE));
            if (bitSet != null) {
                try {
                    //Emiting the tuples to the downStream processing tasks
                    byte[] bytArrayRBitSet = convertToByteArray(bitSet);
                    this.outputCollector.emit(rightPredicateBitSetStreamID, tuple, new Values(bytArrayRBitSet, tuple.getIntegerByField(Constants.TUPLE_ID)));
                    this.outputCollector.ack(tuple);
                } catch (IOException e) {

                }
            }
        }


    }

    // convert the bit set for transferring
    public synchronized byte[] convertToByteArray(Serializable object) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream objectOutputStream = new ObjectOutputStream(baos)) {
            objectOutputStream.writeObject(object);
            objectOutputStream.flush();
        }
        return baos.toByteArray();
    }

    // convert integer for moving to the network;
    private static byte[] convertToByteArray(List<Integer> integerList) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        for (Integer num : integerList) {
            outputStream.write(num.byteValue());
        }
        return outputStream.toByteArray();
    }

    // Emitting tuple for Permutation Computation:
    public void emitTuplePermutation(Node node, Tuple tuple, String streamID, int downStreamTaskID) {
        while (node != null) {
            for (int i = 0; i < node.getKeys().size(); i++) {
                // Emitting tuples to downStream Task for tuple
                for(int j:node.getKeys().get(i).getValues())
                this.outputCollector.emitDirect(downStreamTaskID, streamID, tuple, new Values(node.getKeys().get(i).getKey(), convertToByteArray(node.getKeys().get(i).getTmpIDs()), false));
                this.outputCollector.ack(tuple);
            }
            node = node.getNext();
        }
        // Flag tuple that indicates the completeness of batch

        this.outputCollector.emitDirect(downStreamTaskID, streamID, tuple, new Values(0, 0, true));
        this.outputCollector.ack(tuple);
    }

    public void offsetComputation(Node nodeForLeft, BPlusTreeWithTmpForPermutation rightBTree, Tuple tuple, int taskIndex) throws IOException {
        // ArrayList<Offset> offsetArrayList= new ArrayList<>();
        int key = nodeForLeft.getKeys().get(0).getKey(); // FirstKEy Added
        boolean check = false;
        List<Integer> values = nodeForLeft.getKeys().get(0).getValues();
        Node node = rightBTree.searchRelativeNode(key);
        // System.out.println("NodeForRight"+node+"..."+key);
        int globalCount = 0;
        int startingIndexForNext = 0;
        BitSet bitset1 = null;
        int sizeOfvalues = 0;
        // Checking the extreme case
        for (int i = 0; i < node.getKeys().size(); i++) {
            if (node.getKeys().get(i).getKey() < key) {
                globalCount += node.getKeys().get(i).getValues().size();
                sizeOfvalues = node.getKeys().get(i).getValues().size();

            }
            if ((node.getKeys().get(i).getKey() >= key) || (i == (node.getKeys().size() - 1))) {
                bitset1 = new BitSet();
                if (node.getKeys().get(i).getKey() == key) {
                    bitset1.set(0, true);
                }
                sizeOfvalues = node.getKeys().get(i).getValues().size();
                if ((i == (node.getKeys().size() - 1)) && (key > node.getKeys().get(i).getKey())) {
                    startingIndexForNext = 0;
                    // node =node.getNext();
                    check = true;
                } else {
                    startingIndexForNext = i;
                }
                break;
            }

        }
        // Global count is the single variable that added up to each iteration
        globalCount = globalCount + calculatePreviousNode(node.getPrev());
        for (int j = 0; j < values.size(); j++) {
            // Add logic for Emit the tuples
            outputCollector.emitDirect(taskIndex, offsetComputationStreamID, new Values(key, (globalCount + 1), convertToByteArray(bitset1), sizeOfvalues, false));
            //////// offsetArrayList.add(new Offset(key,(globalCount + 1),bitset1,sizeOfvalues));
        }
        // Add to the Offset Array with key
        if (check) {
            // System.out.println(node+"....");

            linearScanning(nodeForLeft, node.getNext(), startingIndexForNext, globalCount, taskIndex);
        } else {

            linearScanning(nodeForLeft, node, startingIndexForNext, globalCount, taskIndex);
            outputCollector.emitDirect(taskIndex, offsetComputationStreamID, new Values(0, 0, null, 0, true));
            this.outputCollector.ack(tuple);
        }

        // return offsetArrayList;
    }

    // Calculating previous node
    public int calculatePreviousNode(Node node) {
        int count = 0;
        while (node != null) {
            for (int i = 0; i < node.getKeys().size(); i++) {
                count += node.getKeys().get(i).getValues().size();
            }
            node = node.getPrev();
        }
        return count;
    }

    // finding Offset position of leftStream tuple in right stream
    public void linearScanning(Node nodeForLeft, Node nodeForRight, int indexForStartingScanningFromRightNode, int globalCount, int taskIndex) throws IOException {
        boolean counterCheckForOverFlow = false;
        int counterGlobalCheck = 0;
        int startIndexForNodeForLeft = 1;
        // int startIndexForNodeForRight=indexForStartingScanningFromRightNode;
        while (nodeForLeft != null) {
            for (int i = startIndexForNodeForLeft; i < nodeForLeft.getKeys().size(); i++) {
                int key = nodeForLeft.getKeys().get(i).getKey();
                List<Integer> valuesForSearchingKey = nodeForLeft.getKeys().get(i).getValues();
                label1:
                while (nodeForRight != null) {
                    for (int j = indexForStartingScanningFromRightNode; j < nodeForRight.getKeys().size(); j++) {
                        int sizeOfValue = nodeForRight.getKeys().get(j).getValues().size();
                        if ((nodeForRight.getNext() == null) && (j == nodeForRight.getKeys().size() - 1) && (key > nodeForRight.getKeys().get(j).getKey())) {
                            if (!counterCheckForOverFlow) {
                                counterGlobalCheck = globalCount;
                                counterCheckForOverFlow = true;
                            }

                            if (counterCheckForOverFlow) {
                                int values = nodeForRight.getKeys().get(j).getValues().size(); //values in relative Index
                                BitSet bitset1 = new BitSet();
                                bitset1.set(0, false);
                                for (int k = 0; k < valuesForSearchingKey.size(); k++) {
                                    int gc = counterGlobalCheck + (values + 1);
                                    // System.out.println(counterGlobalCheck + "After"+gc);
                                    // Emitting Logic here
                                    /////////offsetArrayList.add(new Offset(key, gc, bitset1,sizeOfValue));
                                    outputCollector.emitDirect(taskIndex, offsetComputationStreamID, new Values(key, gc, convertToByteArray(bitset1), sizeOfValue, false));


                                }
                            }
                            // Add here
                            break label1;
                        }

                        //System.out.println(j+"Indexxxx"+key);
                        // System.exit(-1);
                        if (nodeForRight.getKeys().get(j).getKey() < key) {
                            //System.out.println(nodeForRight.getKeys().get(j).getKey()+"...."+key);
                            globalCount = globalCount + (nodeForRight.getKeys().get(j).getValues().size());
                        }
                        if (nodeForRight.getKeys().get(j).getKey() >= key) {
                            BitSet bitset1 = new BitSet();
                            if (nodeForRight.getKeys().get(j).getKey() == key) {

                                bitset1.set(0, true);
                            }
                            for (int k = 0; k < valuesForSearchingKey.size(); k++) {
                                // Emitting Logic here
                                //////  offsetArrayList.add(new Offset(key,(globalCount + 1),bitset1,sizeOfValue));
                                // System.out.println((globalCount + 1)+"...... "+nodeForRight);
                                outputCollector.emitDirect(taskIndex, offsetComputationStreamID, new Values(key, (globalCount + 1), convertToByteArray(bitset1), sizeOfValue, false));

                            }
                            indexForStartingScanningFromRightNode = j;
                            break label1;
                        }
                    }
                    indexForStartingScanningFromRightNode = 0;
                    nodeForRight = nodeForRight.getNext();
                }
            }
            nodeForLeft = nodeForLeft.getNext();
            startIndexForNodeForLeft = 0;
        }
    }

    //It include all merge operation require for creating IEJoin structure.
    public void mergingOfMutableStructureForImmutableDataStructure(Tuple tuple) {
        // Collector for merge operation.

        this.outputCollector.emitDirect(downStreamTasksForOffset.get(idForDownStreamTasksOffset), mergeOperationStreamID, new Values(true));

        // Left most node for stream R
        Node leftBatch = leftStreamBPlusTree.leftMostNode();
        // Left Most Node for Stream S
        Node rightBatch = rightStreamBPlusTree.leftMostNode();
        // 3 thread that can work in parallel manner
        Thread t1 = new Thread(() -> {
            // Write emit method, stream id, down stream task
            emitTuplePermutation(leftBatch, tuple, permutationComputationStreamID, downStreamTaskIdsForPermutation.get(0));
            // execute any other code related to BM here
            tmpIDForPermutationForStreamR=0;
        });

        Thread t2 = new Thread(() -> {
            emitTuplePermutation(rightBatch, tuple, permutationComputationStreamID, downStreamTaskIdsForPermutation.get(1));
            //  emitTuple(rightBatch, tuple,downstreamTaskIdsForIEJoinPermutationRight.get(0),  collector, localAddress.getHostName());
            tmpIDForPermutationForStreamS=0;
        });
        Thread t3 = new Thread(() -> {
            try {
                offsetComputation(leftBatch, rightStreamBPlusTree, tuple, downStreamTasksForOffset.get(idForDownStreamTasksOffset));
            } catch (IOException e) {
                e.printStackTrace();
            }
        });


        t1.start();
        t2.start();
        t3.start();

        try {
            t1.join();
            t2.join();
            t3.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        idForDownStreamTasksOffset++; // DownStream Task ID updation
        // Flushing the mutable data structure
        leftStreamBPlusTree = new BPlusTreeWithTmpForPermutation(Constants.ORDER_OF_B_PLUS_TREE);
        rightStreamBPlusTree = new BPlusTreeWithTmpForPermutation(Constants.ORDER_OF_B_PLUS_TREE);
        mergeIntervalCounter = 0; // Merge interval to 0 if not zero than B tree will be null for next
        if (idForDownStreamTasksOffset == downStreamTasksForOffset.size()) {
            idForDownStreamTasksOffset = 0;
        }
    }
}
