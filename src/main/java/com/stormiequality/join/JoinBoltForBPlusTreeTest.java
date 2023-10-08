package com.stormiequality.join;

import com.stormiequality.BTree.BPlusTreeUpdated;
import com.stormiequality.BTree.Key;
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

public class JoinBoltForBPlusTreeTest extends BaseRichBolt {
    private BPlusTreeUpdated LeftStreamBPlusTree=null;
    //Tuple from right stream
    private BPlusTreeUpdated RightStreamBPlusTree=null;
    // Definition of order of BPlus Tree
    private int orderOfBPlusTree=0;
    // Tuple counter that see the counter on the runtime
    private int tupleCounter=0;
    private int counter;
    // Runtime count maintainance
    private WindowBoltForBPlusTree.Count count;
    // BitArray Definition
    private int bitSetSize=0;
    //Stream Id for Left or Right Stream
    private String emittingStreamID;
    private String operator;
    private String RStream;
    // This is for output collector
    private OutputCollector collector;
    //private String emitBatchForIEJoin;
    private String emit2T;
    private InetAddress localAddress;
    private String downStreamPermutationComponetTasksLeft;
    private String downStreamPermutationComponetTasksRight;
    private List<Integer> downstreamTaskIdsForIEJoinPermutationLeft;
    private List<Integer> downstreamTaskIdsForIEJoinPermutationRight;
    private List<Integer> downStreamTasksIdForIEOfset;
    private String downStreamTasksForIEJoinPermutation;
    private String downStreamTaskOffset;
    private int taskIdForOffset;
    private String downStreamIDOffset;
    public JoinBoltForBPlusTreeTest(int orderOfBPlusTree, int count, String operator, String emittingForANDStreamID, String emit2T,
                                    String downStreamTasksForIEJoinPermutation,String downStreamPermutationLComponetTasks,String downStreamPermutationRComponetTasks, String downStreamTaskOffset, String downStreamIDOffset )
    {
        this.orderOfBPlusTree=orderOfBPlusTree;
        this.counter=count;
        bitSetSize=count;
        this.operator=operator;
        this.emittingStreamID=emittingForANDStreamID;
        //this.emitBatchForIEJoin=emitBatchForIEjoin;
        this.emit2T=emit2T;
        this.downStreamPermutationComponetTasksLeft=downStreamPermutationLComponetTasks;
        this.downStreamPermutationComponetTasksRight=downStreamPermutationRComponetTasks;
        this.downStreamTasksForIEJoinPermutation=downStreamTasksForIEJoinPermutation;
        this.downStreamTaskOffset=downStreamTaskOffset;
        this.downStreamIDOffset=downStreamIDOffset;
    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        try{
            LeftStreamBPlusTree= new BPlusTreeUpdated();
            RightStreamBPlusTree= new BPlusTreeUpdated();
            LeftStreamBPlusTree.initialize(orderOfBPlusTree);
            RightStreamBPlusTree.initialize(orderOfBPlusTree);
            localAddress = InetAddress.getLocalHost();
            this.collector=outputCollector;
            downstreamTaskIdsForIEJoinPermutationLeft=topologyContext.getComponentTasks(downStreamPermutationComponetTasksLeft);
            downstreamTaskIdsForIEJoinPermutationRight=topologyContext.getComponentTasks(downStreamPermutationComponetTasksRight);
            downStreamTasksIdForIEOfset=topologyContext.getComponentTasks(downStreamIDOffset);
            taskIdForOffset=0;


        }catch (Exception e){

        }
    }

    @Override
    public void execute(Tuple tuple) {

        tupleCounter++;
        try {
            if (operator.equals("<")) {
                lessPredicateEvaluation(tuple);
            }
            if (operator.equals(">")) {
                greaterPredicateEvaluation(tuple);
            }
        }
        catch (Exception e){

        }
        if(tupleCounter==counter){
            Node leftBatch= LeftStreamBPlusTree.leftMostNode();
            // Left Most Node for Stream S
            Node rightBatch=RightStreamBPlusTree.leftMostNode();
         //   int key= leftBatch.getKeys().get(0).getKey();
           // Node searchedNode=RightStreamBPlusTree.searchRelativeNode(key);
            // Flush out existing Data Struture
//            LeftStreamBPlusTree= new BPlusTree();
//            RightStreamBPlusTree= new BPlusTree();
//            LeftStreamBPlusTree.initialize(orderOfBPlusTree);
//            RightStreamBPlusTree.initialize(orderOfBPlusTree);
//            tupleCounter=0;
//            emitTuple(leftBatch, tuple,downstreamTaskIdsForIEJoinPermutationLeft.get(0),  collector, localAddress.getHostName());
//            emitTuple(rightBatch, tuple,downstreamTaskIdsForIEJoinPermutationRight.get(0),  collector, localAddress.getHostName());


            Thread t1 = new Thread(() -> {
                emitTuple(leftBatch, tuple,downstreamTaskIdsForIEJoinPermutationLeft.get(0),  collector, localAddress.getHostName());
                    // execute any other code related to BM here
                });

                Thread t2 = new Thread(() -> {
                    emitTuple(rightBatch, tuple,downstreamTaskIdsForIEJoinPermutationRight.get(0),  collector, localAddress.getHostName());
                });
            Thread t3 = new Thread(() -> {
                offsetComputation(leftBatch,RightStreamBPlusTree,collector,downStreamTasksIdForIEOfset.get(taskIdForOffset),downStreamTaskOffset,localAddress.getHostName(),tuple);
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




//            emitTuple(leftBatch, tuple,downstreamTaskIdsForIEJoinPermutation.get(0),  collector, localAddress.getHostName());
//            emitTuple(rightBatch, tuple,downstreamTaskIdsForIEJoinPermutation.get(1),  collector, localAddress.getHostName());
            //Open this comment
       //     offsetComputation(leftBatch,RightStreamBPlusTree,collector,downStreamTasksIdForIEOfset.get(taskIdForOffset),downStreamTaskOffset,localAddress.getHostName(),tuple);
            LeftStreamBPlusTree= new BPlusTreeUpdated();
            RightStreamBPlusTree= new BPlusTreeUpdated();
            LeftStreamBPlusTree.initialize(orderOfBPlusTree);
            RightStreamBPlusTree.initialize(orderOfBPlusTree);
            tupleCounter=0;

            taskIdForOffset++;
            if(taskIdForOffset==downStreamTasksIdForIEOfset.size()){
                taskIdForOffset=0;
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(emittingStreamID,new Fields("bitset","ID","Time","TupleID"));
        outputFieldsDeclarer.declareStream(emit2T,new Fields("bitset","ID","Time","TupleID"));
        outputFieldsDeclarer.declareStream(downStreamTasksForIEJoinPermutation, new Fields("Tuple", "IDs","Flag"));
        outputFieldsDeclarer.declareStream(downStreamTaskOffset, new Fields("Tuple","Index","Flag","Time"));
       // collector.emitDirect(taskId,streamID,tuple, new Values(key, (relativeIndexOfLeftInRight+j)+1,System.currentTimeMillis(),nodeName));

    }
    public void lessPredicateEvaluation(Tuple tuple) throws IOException {
        if(tuple.getValueByField("StreamID").equals("LeftStream")){

            LeftStreamBPlusTree.insert(tuple.getIntegerByField("Tuple"),tuple.getIntegerByField("ID"));
            long startSearchTime=System.currentTimeMillis();
            BitSet RBitSet= RightStreamBPlusTree. greaterThenSpecificValue(tuple.getIntegerByField("Tuple"));
          //  System.out.println(RBitSet+"..............Less");
            //   long endSearchTime=System.currentTimeMillis()-startSearchTime;
            if(RBitSet!=null) {
                byte[] bytArrayRBitSet = convertToByteArray(RBitSet);

                Values values = new Values(bytArrayRBitSet, tuple.getIntegerByField("ID"),startSearchTime,tuple.getValueByField("TupleID"));
                collector.emit(emittingStreamID, tuple, values);
                collector.ack(tuple);
            }
        }
        if(tuple.getValueByField("StreamID").equals("RightStream")){

            // BitSet If Tuple is from right stream R.r>S.s  it first insert then search on other stream
            RightStreamBPlusTree.insert(tuple.getIntegerByField("Tuple"),tuple.getIntegerByField("ID"));
            long startSearchTime=System.currentTimeMillis();
            BitSet LBitSet=  LeftStreamBPlusTree. lessThenSpecificValue(tuple.getIntegerByField("Tuple"));
       //   System.out.println(LBitSet+"Less");
           // System.out.println(LBitSet+",,,Left");
            //long endSearchTime=System.currentTimeMillis()-startSearchTime;
            if(LBitSet!=null) {
                byte[] bytArrayLBitSet = convertToByteArray(LBitSet);

                Values values = new Values(bytArrayLBitSet, tuple.getIntegerByField("ID"), startSearchTime,tuple.getValueByField("TupleID"));
                collector.emit(emit2T, tuple, values);
                collector.ack(tuple);
            }
        }

    }
    public void greaterPredicateEvaluation(Tuple tuple) throws IOException {

        if(tuple.getValueByField("StreamID").equals("LeftStream")){

            LeftStreamBPlusTree.insert(tuple.getIntegerByField("Tuple"),tuple.getIntegerByField("ID"));
            long startSearchTime=System.currentTimeMillis();
            BitSet RBitSet= RightStreamBPlusTree. lessThenSpecificValue(tuple.getIntegerByField("Tuple"));
        //    System.out.println(RBitSet+"..----------------------------greater-.");
            // System.out.println(RBitSet+"....LeftStream"+ ".."+tuple.getIntegerByField("Tuple"));
            //long endSearchTime=System.currentTimeMillis()-startSearchTime;
            if(RBitSet!=null) {
                byte[] bytArrayRBitSet = convertToByteArray(RBitSet);
                Values values = new Values(bytArrayRBitSet, tuple.getIntegerByField("ID"),startSearchTime, tuple.getValueByField("TupleID"));
                collector.emit(emittingStreamID, tuple, values);
                collector.ack(tuple);
            }
        }
        if(tuple.getValueByField("StreamID").equals("RightStream")){

            RightStreamBPlusTree.insert(tuple.getIntegerByField("Tuple"),tuple.getIntegerByField("ID"));
            long startSearchTime=System.currentTimeMillis();
            BitSet LBitSet=  LeftStreamBPlusTree. greaterThenSpecificValue(tuple.getIntegerByField("Tuple"));
   //     System.out.println(LBitSet+"greater..............");
            //  System.out.println(LBitSet+"....RightStream");
            // long endSearchTime=System.currentTimeMillis()-startSearchTime;
            if(LBitSet!=null) {
                byte[] bytArrayLBitSet = convertToByteArray(LBitSet);
                Values values = new Values(bytArrayLBitSet, tuple.getIntegerByField("ID"),startSearchTime,tuple.getValueByField("TupleID"));
                collector.emit(emit2T, tuple, values);
                collector.ack(tuple);
            }
        }

    }
    public synchronized byte[] convertToByteArray(Serializable object) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream objectOutputStream = new ObjectOutputStream(baos)) {
            objectOutputStream.writeObject(object);
            objectOutputStream.flush();
        }
        return baos.toByteArray();
    }
    public void emitTuple(Node node, Tuple tuple, int taskId, OutputCollector collector, String hostName){
        while(node!=null){
            for(int i=0;i<node.getKeys().size();i++){

                collector.emitDirect(taskId,downStreamTasksForIEJoinPermutation,tuple,new Values(node.getKeys().get(i).getKey(),  convertToByteArray(node.getKeys().get(i).getValues()), false));
            }
            node = node.getNext();
        }
            collector.emitDirect(taskId,downStreamTasksForIEJoinPermutation,tuple, new Values(0,0,true));

    }




    public synchronized void offsetComputation(Node nodeForLeft, BPlusTreeUpdated rightBTree, OutputCollector collector, int taskId, String streamID, String nodeName, Tuple tuple){
       // ArrayList<Offset> offsetArrayList = new ArrayList();
        // Node nodeForLeft = leftBTree.leftMostNode();
        // Node nodeForRight = null;
        int relativeIndexOfLeftInRight = 0;
        Node intermediateNode = null;
        boolean checkIndex = false;
        Node nodeForRight=null;
        while (nodeForLeft != null) {
            for (Key keyNode : nodeForLeft.getKeys()) {
                int key = keyNode.getKey();
                int valueSize= keyNode.getValues().size();
                if (!checkIndex) {
                     nodeForRight = rightBTree.searchRelativeNode(key);
                    intermediateNode = nodeForRight.getPrev();
                    while (intermediateNode != null) {
                        relativeIndexOfLeftInRight += intermediateNode.getKeys().size();
                        intermediateNode = intermediateNode.getPrev();
                    }
                    checkIndex = true;
                }

                boolean foundKey = false;
                while (nodeForRight != null) {
                    for (int j = 0; j < nodeForRight.getKeys().size(); j++) {

                        if (nodeForRight.getKeys().get(j).getKey() >= key) {
                           // int sized=nodeForRight.getKeys().get(j).getValues().size();
                            for(int size=0;size<valueSize;size++)
                                collector.emitDirect(taskId,streamID,tuple, new Values(key, (relativeIndexOfLeftInRight+j)+1,false,System.currentTimeMillis()));
                            // offsetArrayList.add(new Offset(key, relativeIndexOfLeftInRight + j + 1));
                            foundKey = true;
                            break;
                        } else if (nodeForRight.getNext() == null && j == nodeForRight.getKeys().size() - 1 && key > nodeForRight.getKeys().get(j).getKey()) {
                            int newIndex = relativeIndexOfLeftInRight + (nodeForRight.getKeys().size() + 1);
                           // int sized=nodeForRight.getKeys().get(j).getValues().size();
                            for(int size=0;size<valueSize;size++)
                                collector.emitDirect(taskId,streamID,tuple, new Values(key, newIndex,false,System.currentTimeMillis()));

                            //   offsetArrayList.add(new Offset(key, newIndex));
                            foundKey = true;
                            break;
                        }
                    }

                    if (foundKey) {
                        break;
                    } else {
                        relativeIndexOfLeftInRight += nodeForRight.getKeys().size();
                        nodeForRight = nodeForRight.getNext();
                    }
                }
            }
            nodeForLeft = nodeForLeft.getNext();
        }
        collector.emitDirect(taskId,streamID,tuple, new Values(0, 0,true, System.currentTimeMillis()));

    }

    private static byte[] convertToByteArray(List<Integer> integerList) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        for (Integer num : integerList) {
            outputStream.write(num.byteValue());
        }
        return outputStream.toByteArray();
    }

}
