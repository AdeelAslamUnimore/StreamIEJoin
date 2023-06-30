package com.stormiequality.join;

import com.stormiequality.BTree.*;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.*;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.GZIPOutputStream;

public class JoinBoltForBPlusTree extends WindowBoltForBPlusTree implements Serializable {
    // Let consider the two stream R an S the R.r> S.s then in this case two stream of tuples can arrive at here
    // Tuple from left stream
   private BPlusTree LeftStreamBPlusTree=null;
   //Tuple from right stream
   private BPlusTree RightStreamBPlusTree=null;
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
   private String emitBatchForIEJoin;
   private String emit2T;
   private InetAddress localAddress;
   private String downStreamComponentID;
   private  List<Integer> downstreamTaskIds;
  // private BufferedWriter writer;
   public JoinBoltForBPlusTree(){

   }
   public JoinBoltForBPlusTree(int orderOfBPlusTree, int count, String operator, String emittingForANDStreamID, String emit2T, String emitBatchForIEjoin)  {
       this.orderOfBPlusTree=orderOfBPlusTree;
       this.counter=count;
       bitSetSize=count;
       this.operator=operator;
       this.emittingStreamID=emittingForANDStreamID;
       this.emitBatchForIEJoin=emitBatchForIEjoin;
       this.emit2T=emit2T;


       // Count based window initialization
      // this.withCountBasedWindow(count);

   }
    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
       // Index structure for both stream here R.r ans S.s

        try {
            LeftStreamBPlusTree= new BPlusTree();
            RightStreamBPlusTree= new BPlusTree();
            LeftStreamBPlusTree.initialize(orderOfBPlusTree);
            RightStreamBPlusTree.initialize(orderOfBPlusTree);
            localAddress = InetAddress.getLocalHost();
            this.collector=outputCollector;
            downstreamTaskIds= topologyContext.getComponentTasks("IEJoin");
        }
        catch (Exception e){

        }

    }

    @Override
    public void execute(Tuple tuple) {

       // Counter increment for every tuple
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
            // ArrayList<Offset> offsetArrayList=offsetComputation(LeftStreamBPlusTree,RightStreamBPlusTree);

            // LeftMostNode for Stream R
            Node leftBatch= LeftStreamBPlusTree.leftMostNode();
            // Left Most Node for Stream S
            Node rightBatch=RightStreamBPlusTree.leftMostNode();
            int key= leftBatch.getKeys().get(0).getKey();
            Node searchedNode=RightStreamBPlusTree.searchRelativeNode(key);
            // Flush out existing Data Struture
            LeftStreamBPlusTree= new BPlusTree();
            RightStreamBPlusTree= new BPlusTree();
            LeftStreamBPlusTree.initialize(orderOfBPlusTree);
            RightStreamBPlusTree.initialize(orderOfBPlusTree);
            tupleCounter=0;
            // Emitting the whole batch for IE Join.
            if(leftBatch!=null && rightBatch!=null ) {
                try {

                    long time=System.currentTimeMillis();
                    byte[] byteLeftBatch = convertToByteArray(leftBatch);
                    byte[] byteRightBatch=convertToByteArray(rightBatch);
                    byte[] byteSearchedNode=convertToByteArray(searchedNode);
                    Values values = new Values(byteLeftBatch, byteRightBatch, byteSearchedNode,localAddress.getHostName(),time);
                   // Values values= new Values(bytes,localAddress.getHostName(),time);

                 collector.emit(emitBatchForIEJoin, tuple, values);
                    collector.ack(tuple);
                } catch (Exception e) {
                    e.printStackTrace();
                   // System.exit(-1);
                }

            }
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
       // Output declarer for Predicate that emits the Bitsets
        outputFieldsDeclarer.declareStream(emittingStreamID,new Fields("bitset","ID","Time","TupleID"));
        outputFieldsDeclarer.declareStream(emit2T,new Fields("bitset","ID","Time","TupleID"));
        //outputFieldsDeclarer.declareStream(emitBatchForIEJoin, new Fields("Tuple","Size","Time","Node"));
        // Output declarer that emits the Whole structure for batch of IEJoin
        outputFieldsDeclarer.declareStream(emitBatchForIEJoin, new Fields("L1Stream","R1Stream","Offset1","Node","Time"));
       // outputFieldsDeclarer.declareStream(emitBatchForIEJoin, new Fields("Tuple","Node","Time"));
    }
    public static ArrayList<Offset> offsetComputation(BPlusTree leftBTree, BPlusTree rightBTree){
        ArrayList<Offset> offsetArrayList = new ArrayList();
        Node nodeForLeft = leftBTree.leftMostNode();
        Node nodeForRight = null;
        int relativeIndexOfLeftInRight = 0;
        Node intermediateNode = null;
        boolean checkIndex = false;

        while (nodeForLeft != null) {
            for (Key keyNode : nodeForLeft.getKeys()) {
                int key = keyNode.getKey();
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
                            offsetArrayList.add(new Offset(key, relativeIndexOfLeftInRight + j + 1));
                            foundKey = true;
                            break;
                        } else if (nodeForRight.getNext() == null && j == nodeForRight.getKeys().size() - 1 && key > nodeForRight.getKeys().get(j).getKey()) {
                            int newIndex = relativeIndexOfLeftInRight + (nodeForRight.getKeys().size() + 1);
                            offsetArrayList.add(new Offset(key, newIndex));
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
        return offsetArrayList;
    }
    public void lessPredicateEvaluation(Tuple tuple) throws IOException {
        if(tuple.getValueByField("StreamID").equals("LeftStream")){

            LeftStreamBPlusTree.insert(tuple.getIntegerByField("Tuple"),tuple.getIntegerByField("ID"));
            long startSearchTime=System.currentTimeMillis();
            BitSet RBitSet= RightStreamBPlusTree. lessThenSpecificValue(tuple.getIntegerByField("Tuple"));
          //  System.out.println(RBitSet+",,,RightBitStream");
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
            BitSet LBitSet=  LeftStreamBPlusTree. greaterThenSpecificValue(tuple.getIntegerByField("Tuple"));
            //long endSearchTime=System.currentTimeMillis()-startSearchTime;
            if(LBitSet!=null) {
                byte[] bytArrayLBitSet = convertToByteArray(LBitSet);
               // System.out.println(LBitSet+"...."+"RightStreamLeftEvalution");
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
            BitSet RBitSet= RightStreamBPlusTree. greaterThenSpecificValue(tuple.getIntegerByField("Tuple"));
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
            BitSet LBitSet=  LeftStreamBPlusTree. lessThenSpecificValue(tuple.getIntegerByField("Tuple"));
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
    public void emitTuple(Node node, int taskId, Tuple tuple){
        while (node != null) {
            for(int i=0;i<node.getKeys().size();i++){
                node.getKeys().get(i).getKey();
                node.getKeys().get(i).getValues().size();
                Values values= new Values(node.getKeys().get(i).getKey(),node.getKeys().get(i).getValues().size(),System.currentTimeMillis(), localAddress.getHostName());

                collector.emitDirect(taskId,emitBatchForIEJoin, tuple, values);
            }
            node= node.getNext();
        }
    }
//    public byte[] conversion() throws IOException {
//        ByteArrayOutputStream os = new ByteArrayOutputStream();
//        try (DeflaterOutputStream dos = new DeflaterOutputStream(os)) {
//            dos.write(bArray);    }
//        return os.toByteArray();}
//    }
}

