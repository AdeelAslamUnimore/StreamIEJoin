package com.proposed.iejoinandbplustreebased;

import com.stormiequality.BTree.BPlusTree;
import com.stormiequality.BTree.Key;
import com.stormiequality.BTree.Node;
import com.stormiequality.BTree.Offset;
import com.stormiequality.join.WindowBoltForBPlusTree;
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
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;

public class MutableBPlusTree extends BaseRichBolt {
    //left stream for MutableBPlusTree
    private BPlusTree leftStreamBPlusTree;
    // right stream for MutableBPlusTree
    private BPlusTree rightStreamBPlusTree;
    // Order of Trees
    private int orderOfTree;
    //private mergeIntervalCount;
    private int mergeIntervalCounter;
    //Merge Interval provided by user
    private int mergeIntervalDefinedByUser;
    // operator that need to perform
    private String operator;
    // Output collector
    private OutputCollector outputCollector;
    // downStream
    private String downStreamTasksForIEJoinPermutation;


    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        try {
            leftStreamBPlusTree = new BPlusTree(orderOfTree);
            rightStreamBPlusTree = new BPlusTree(orderOfTree);
            this.outputCollector = outputCollector;
        } catch (Exception e) {

        }
    }

    @Override
    public void execute(Tuple tuple) {
        mergeIntervalCounter++;
        try {
            if (operator.equals("<")) {
                lessPredicateEvaluation(tuple);
            }
            if (operator.equals(">")) {
                greaterPredicateEvaluation(tuple);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (mergeIntervalCounter >= mergeIntervalDefinedByUser) {
            Thread t1 = new Thread(() -> {
                // Write emit method
                // emitTuple(leftBatch, tuple,downstreamTaskIdsForIEJoinPermutationLeft.get(0),  collector, localAddress.getHostName());
                // execute any other code related to BM here
            });

            Thread t2 = new Thread(() -> {
                //  emitTuple(rightBatch, tuple,downstreamTaskIdsForIEJoinPermutationRight.get(0),  collector, localAddress.getHostName());
            });
            Thread t3 = new Thread(() -> {
                // offsetComputation(leftBatch,RightStreamBPlusTree,collector,downStreamTasksIdForIEOfset.get(taskIdForOffset),downStreamTaskOffset,localAddress.getHostName(),tuple);
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


        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        // For offset
        //For Stream join
        // For Permutation
    }

    // Completely evaluate the < predicate here
    private void lessPredicateEvaluation(Tuple tuple) {
        if (tuple.getSourceStreamId().equals("Left")) {
            // Inserting the tuple into the BTree location;
            leftStreamBPlusTree.insert(tuple.getIntegerByField("Tuple"), tuple.getIntegerByField("ID"));
            BitSet bitSet = rightStreamBPlusTree.greaterThenSpecificValue(tuple.getIntegerByField("Tuple"));
            if (bitSet != null) {
                try {
                    byte[] bytArrayRBitSet = convertToByteArray(bitSet);
                    // Emitting Logic for tuples

                } catch (IOException e) {

                }
            }

        }

        if (tuple.getSourceStreamId().equals("Right")) {
            // Insert into the other stream
            rightStreamBPlusTree.insert(tuple.getIntegerByField("Tuple"), tuple.getIntegerByField("ID"));
            // Evaluation of the query
            BitSet bitSet = leftStreamBPlusTree.lessThenSpecificValue(tuple.getIntegerByField("Tuple"));
            //
            if (bitSet != null) {
                try {
                    byte[] bytArrayLBitSet = convertToByteArray(bitSet);
                    // Emitting Tuple logic
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        }

    }

    // Completely evaluate the > predicte
    public void greaterPredicateEvaluation(Tuple tuple) {
        if (tuple.getSourceStreamId().equals("Left")) {
            leftStreamBPlusTree.insert(tuple.getIntegerByField("Tuple"), tuple.getIntegerByField("ID"));
            BitSet bitSet = rightStreamBPlusTree.lessThenSpecificValue(tuple.getIntegerByField("Tuple"));
            if (bitSet != null) {
                try {
                    byte[] bytArrayRBitSet = convertToByteArray(bitSet);
                    //Emit logic here tuple emitting
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        if (tuple.getSourceStreamId().equals("Right")) {
            rightStreamBPlusTree.insert(tuple.getIntegerByField("Tuple"), tuple.getIntegerByField("ID"));
            BitSet bitSet = leftStreamBPlusTree.greaterThenSpecificValue(tuple.getIntegerByField("Tuple"));
            if (bitSet != null) {
                try {
                    byte[] bytArrayRBitSet = convertToByteArray(bitSet);
                    //Emit logic at here
                } catch (IOException e) {

                }
            }
        }


    }

    // convert the bit array for transferring
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
    public void emitTuple(Node node, Tuple tuple, int taskId, OutputCollector collector, String hostName) {
        while (node != null) {
            for (int i = 0; i < node.getKeys().size(); i++) {
                // Emitting tuples to downStream Task for tuple
                collector.emitDirect(taskId, downStreamTasksForIEJoinPermutation, tuple, new Values(node.getKeys().get(i).getKey(), convertToByteArray(node.getKeys().get(i).getValues()), false));
            }
            node = node.getNext();
        }
        collector.emitDirect(taskId, downStreamTasksForIEJoinPermutation, tuple, new Values(0, 0, true));

    }
    public void offsetComputationExtremeCase(Node nodeForLeft, BPlusTree rightBTree, OutputCollector outputCollector) {
       // ArrayList<Offset> offsetArrayList= new ArrayList<>();
        int key=nodeForLeft.getKeys().get(0).getKey(); // FirstKEy Added
        boolean check=false;
        List<Integer> values=nodeForLeft.getKeys().get(0).getValues();
        Node node= rightBTree.searchRelativeNode(key);
        // System.out.println("NodeForRight"+node+"..."+key);
        int globalCount=0;
        int startingIndexForNext=0;
        BitSet bitset1=null;
        int sizeOfvalues=0;
        // Checking the extreme case
        for(int i=0; i<node.getKeys().size();i++){
            if(node.getKeys().get(i).getKey()<key){
                globalCount+=node.getKeys().get(i).getValues().size();
                //System.out.println("NodeForRight"+globalCount+"..."+key);
                //New
                sizeOfvalues=node.getKeys().get(i).getValues().size();

            }
            if((node.getKeys().get(i).getKey()>=key)||(i==(node.getKeys().size()-1))){
                bitset1= new BitSet();
                if(node.getKeys().get(i).getKey()==key){
                    bitset1.set(0,true);
                }
                sizeOfvalues=node.getKeys().get(i).getValues().size();
                if((i==(node.getKeys().size()-1))&&(key>node.getKeys().get(i).getKey())){
                    startingIndexForNext = 0;
                    // node =node.getNext();
                    check=true;
                }
                else{
                    startingIndexForNext = i;
                }
                break;
            }

        }
       // Global count is the single variable that added up to each iteration
        globalCount=globalCount+  calculatePreviousNode(node.getPrev());
        for(int j=0;j<values.size();j++) {
            // Add logic for Emit the tuples
           //////// offsetArrayList.add(new Offset(key,(globalCount + 1),bitset1,sizeOfvalues));
        }
        // Add to the Offset Array with key
        if(check){
            // System.out.println(node+"....");

            linearScanning(nodeForLeft, node.getNext(), startingIndexForNext, globalCount,  outputCollector);
        }else {

            linearScanning(nodeForLeft, node, startingIndexForNext, globalCount, outputCollector);
        }

       // return offsetArrayList;
    }
    // Calculating previous node
    public int calculatePreviousNode(Node node){
        int count=0;
        while(node!=null){
            for(int i=0;i<node.getKeys().size();i++){
                count+=node.getKeys().get(i).getValues().size();
            }
            node= node.getPrev();
        }
        return  count;
    }
    // finding Offset position of leftStream tuple in right stream
    public void  linearScanning(Node nodeForLeft, Node nodeForRight, int indexForStartingScanningFromRightNode, int globalCount,  OutputCollector outputCollector){
        boolean counterCheckForOverFlow=false;
        int counterGlobalCheck=0;
        int startIndexForNodeForLeft=1;
        // int startIndexForNodeForRight=indexForStartingScanningFromRightNode;
        while(nodeForLeft!=null){
            for(int i=startIndexForNodeForLeft;i<nodeForLeft.getKeys().size();i++){
                int  key= nodeForLeft.getKeys().get(i).getKey();
                List<Integer> valuesForSearchingKey=nodeForLeft.getKeys().get(i).getValues();
                label1:  while(nodeForRight!=null){
                    for(int j=indexForStartingScanningFromRightNode;j<nodeForRight.getKeys().size();j++){
                        int sizeOfValue=nodeForRight.getKeys().get(j).getValues().size();
                        if((nodeForRight.getNext()==null)&&(j==nodeForRight.getKeys().size()-1)&&(key > nodeForRight.getKeys().get(j).getKey())){
                            if(!counterCheckForOverFlow){
                                counterGlobalCheck=globalCount;
                                counterCheckForOverFlow=true;
                            }

                            if(counterCheckForOverFlow) {
                                int values = nodeForRight.getKeys().get(j).getValues().size(); //values in relative Index
                                BitSet bitset1 = new BitSet();
                                bitset1.set(0, false);
                                for (int k = 0; k < valuesForSearchingKey.size(); k++) {
                                    int gc = counterGlobalCheck + (values + 1);
                                    // System.out.println(counterGlobalCheck + "After"+gc);
                                    // Emitting Logic here
                                    /////////offsetArrayList.add(new Offset(key, gc, bitset1,sizeOfValue));

                                }
                            }
                            // Add here
                            break label1;
                        }

                        //System.out.println(j+"Indexxxx"+key);
                        // System.exit(-1);
                        if(nodeForRight.getKeys().get(j).getKey()<key){
                            //System.out.println(nodeForRight.getKeys().get(j).getKey()+"...."+key);
                            globalCount=globalCount+(nodeForRight.getKeys().get(j).getValues().size());
                        }
                        if(nodeForRight.getKeys().get(j).getKey()>=key){
                            BitSet bitset1= new BitSet();
                            if(nodeForRight.getKeys().get(j).getKey()==key){

                                bitset1.set(0,true);
                            }
                            for(int k=0;k<valuesForSearchingKey.size();k++) {
                                // Emitting Logic here
                              //////  offsetArrayList.add(new Offset(key,(globalCount + 1),bitset1,sizeOfValue));
                                // System.out.println((globalCount + 1)+"...... "+nodeForRight);
                            }
                            indexForStartingScanningFromRightNode=j;
                            break label1;
                        }
                    }
                    indexForStartingScanningFromRightNode=0;
                    nodeForRight=nodeForRight.getNext();
                }
            }
            nodeForLeft= nodeForLeft.getNext();
            startIndexForNodeForLeft=0;
        }
    }
}
