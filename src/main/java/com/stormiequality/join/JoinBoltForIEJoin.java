package com.stormiequality.join;

import com.stormiequality.BTree.Key;
import com.stormiequality.BTree.Node;
import com.stormiequality.BTree.Offset;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.InetAddress;
import java.util.*;
import java.util.zip.GZIPInputStream;

public class JoinBoltForIEJoin extends BaseRichBolt {
    Node LeftStreamNodeL1=null;
    Node RightStreamNodeL1=null;
    Node LeftStreamNodeL2=null;
    Node RightStreamNodeL2=null;
    int counter=0;
    ArrayList<Offset> offsetLeft=null;
    ArrayList<Offset> offsetRight=null;
    ArrayList<Permutation> permutationLeft=null;
    ArrayList<Permutation> permutationRight=null;
    Boolean flag=false;
    private BitSet bitSet=null;
    private int taskID=0;
    private int taskIndex=0;
    private int counterForDelete;
    private int initialCount;
    private StringBuilder builder=null;
    private InetAddress inetAddress=null;

    public JoinBoltForIEJoin(int counterForDelete, int initialCount){
        this.counterForDelete=counterForDelete;
        this.initialCount=initialCount;
    }
    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        try {
            this.counter = 0;
            this.flag = false;
            this.counterForDelete = 0;
            taskID = topologyContext.getThisTaskId();
            taskIndex = topologyContext.getThisTaskIndex();
            this.builder = new StringBuilder();
            this.inetAddress = InetAddress.getLocalHost();
        }
        catch (Exception e){

        }
    }

    @Override
    public void execute( Tuple tuple) {

        counter++;
        if(counter==counterForDelete){
            bitSet=null;
            counter=0;
            this.flag=false;
            permutationLeft=null;
            permutationRight=null;
            offsetLeft=null;
            offsetRight=null;
        }

        if(tuple.getSourceStreamId().equals("StreamForIEJoin1")){

            try {
                byte[] byteDataLeftStreamNodeL1 = tuple.getBinaryByField("L1Stream");
                LeftStreamNodeL1 = convertToObject(byteDataLeftStreamNodeL1);
                //(Node) tuple.getValueByField("L1Stream");
                byte[] byteDataRightStreamNodeL1 = tuple.getBinaryByField("R1Stream");
                RightStreamNodeL1 = convertToObject(byteDataRightStreamNodeL1);
                //(Node) tuple.getValueByField("R1Stream");
                byte[] byteDataNodeForRight = tuple.getBinaryByField("Offset1");
                Node nodeForRight = convertToObject(byteDataNodeForRight);
                long OffsetCalulationTimeStart=System.currentTimeMillis();
                offsetLeft=offsetComputation(LeftStreamNodeL1,nodeForRight);
               String calculation="StreamForIEJoin1,"+taskID+","+System.currentTimeMillis()+","+OffsetCalulationTimeStart+","+tuple.getValueByField("Node")+","+tuple.getValueByField("Time")+","+inetAddress.getHostAddress()+"\n";
               //System.out.println(calculation);
            }
            catch (Exception e){
                System.out.println(e);
            }

       }
        if(tuple.getSourceStreamId().equals("StreamForIEJoin2")){

            byte[] byteDataLeftStreamNodeL2 = tuple.getBinaryByField("L1Stream");
            LeftStreamNodeL2= convertToObject(byteDataLeftStreamNodeL2);
            //(Node) tuple.getValueByField("L1Stream");
            byte[] byteDataRightStreamNodeL2 = tuple.getBinaryByField("R1Stream");
            RightStreamNodeL2=  convertToObject(byteDataRightStreamNodeL2);
            //(Node) tuple.getValueByField("R1Stream");
            byte[] byteDataNodeForRight = tuple.getBinaryByField("Offset1");
            Node nodeForRight= convertToObject(byteDataNodeForRight);
            long timeForLeftSynchronization=System.currentTimeMillis();
            //(Node) tuple.getValueByField("Offset1");
            offsetRight=offsetComputation(LeftStreamNodeL2,nodeForRight);

            String calculation="StreamForIEJoin2,"+taskID+","+System.currentTimeMillis()+","+timeForLeftSynchronization+","+tuple.getValueByField("Node")+","+tuple.getValueByField("Time")+","+inetAddress.getHostAddress()+"\n";
           // System.out.println(calculation);
           // System.exit(-2);
            //  System.out.println(nodeForRight.getKeys().get(0).getKey());
//           LeftStreamNodeL2= (Node) tuple.getValueByField("L1Stream");
//           RightStreamNodeL2= (Node) tuple.getValueByField("R1Stream");
//           Node nodeForLeft= (Node) tuple.getValueByField("Offset1");
//           offsetRight=offsetComputation(RightStreamNodeL2,nodeForLeft);

       // this.flag=false;
       }
       if(!flag)
            if((LeftStreamNodeL1!=null)&&(RightStreamNodeL1!=null)&&(offsetLeft!=null)&&(offsetRight!=null)){
               // System.out.println("I am Here "+taskID+"..."+taskIndex);
                long intialTime=System.currentTimeMillis();
                Thread t1 = new Thread(() -> {
                 permutationLeft = permutationComputation(LeftStreamNodeL1, LeftStreamNodeL2, 5000);
                    // execute any other code related to BM here
                });

                Thread t2 = new Thread(() -> {
                   permutationRight = permutationComputation(RightStreamNodeL1, RightStreamNodeL2, 5000);
                });

                t1.start();
                t2.start();

                try {
                    t1.join();
                    t2.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                long finalTime=System.currentTimeMillis();
                //System.out.println(finalTime-intialTime);
             counter=  Search(offsetRight, permutationLeft,  offsetLeft, permutationRight);
              long SearchTime=System.currentTimeMillis();
            //  System.out.println(intialTime+","+finalTime+","+SearchTime+"...............");

                this.flag=true;

            }
            if(tuple.getSourceStreamId().equals("LeftStreamTuples")||tuple.getSourceStreamId().equals("RightStream")){
      // if(tuple.getSourceComponent().equals("testSpout")){
           if(bitSet!=null){
             // int locationInOffset= binarySearch(offsetRight, tuple.getInteger(0));
              int permutationLocation=binarySearchForPermutation(permutationLeft,tuple.getInteger(1));
             // System.out.println(permutationLocation+".."+tuple.getInteger(1));
             int index= offsetLeft.get(permutationLocation).getIndex();
               for (int j = index+1; j < permutationRight.size(); j++) {

                   if (bitSet.get(j)) {
                     //  System.out.println(bitSet);
                       //System.out.println("..."+offsetArrayL1[permutationArrayEast[i]]);
                       // System.out.println(bitSet+"...."+j);
                       //
                   }
               }

           }


//         SearchSingleTuple(permutationLeft, offsetLeft, permutationRight, offset1 > offset2 ? offset2 : offset1, offset1 > offset2 ? offset1 : offset2);
//          counter++;
       }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
    public  ArrayList<Permutation> permutationComputation(Node StreamNodeL1, Node StreamNodeL2, int count){

        int [] holdingList= new int[count];
        int counter=1;
        while (StreamNodeL1!=null){
            for(Key key: StreamNodeL1.getKeys()){
                for(int item: key.getValues()) {
                    //  holdingList.add(key.getId(),counter);
                    holdingList[item] = counter;
                    counter++;
                }

            }
            StreamNodeL1= StreamNodeL1.getNext();
        }

        ArrayList<Permutation> permutationArray= new ArrayList<Permutation>(holdingList.length);
        //Permutation [] permutationArray= new Permutation[holdingList.length];

       // int counterForPermutation=0;
        while(StreamNodeL2!=null){
            for(Key key:StreamNodeL2.getKeys()){
                for(int item: key.getValues()) {
                    permutationArray.add(new Permutation(holdingList[item],key.getKey()));
//                    permutationArray[counterForPermutation] = new Permutation(holdingList[item],key.getKey());//holdingList[item];
//                    counterForPermutation++;

                }
            }
            StreamNodeL2= StreamNodeL2.getNext();
        }


        return  permutationArray;
    }
    public static int binarySearch(ArrayList<Offset> offsetList, int key) {
        int left = 0;
        int right = offsetList.size() - 1;

        while (left <= right) {
            int mid = (left + right) / 2;
            if (offsetList.get(mid).getKey() == key) {
                // Found the element
                return mid;
            } else if (offsetList.get(mid).getKey() < key) {
                // Search in the right half of the list
                left = mid + 1;
            } else {
                // Search in the left half of the list
                right = mid - 1;
            }
        }

        // Element not found
        return -1;
    }
    public  int Search(ArrayList<Offset> offsetArrayL2, ArrayList<Permutation> permutationArrayL1,  ArrayList<Offset> offsetArrayL1, ArrayList<Permutation> permutationArrayL2){

       bitSet= new BitSet();
    //   System.out.println(permutationArrayL1.length+"The Length is ");
        int index=1;
        for(int i=0;i<offsetArrayL2.size();i++) {

            int off2 = Math.min(offsetArrayL2.get(i).getIndex(), permutationArrayL1.size());
         //   System.out.println(offsetArrayL2.get(i).getIndex()+".."+permutationArrayL1.size());
            for (int j = index; j <= off2; j++) {
               // System.out.println(permutationArrayL2[j].getIndex());
                bitSet.set(permutationArrayL2.get(j-1).getIndex(), true);
            }
            index = off2;
            try {
            // System.out.println(permutationArrayL1.length + "The Length is " + offsetArrayL1.size());
            if((permutationArrayL1.get(i).getIndex() + 1)<offsetArrayL1.size())
                for (int j = offsetArrayL1.get(permutationArrayL1.get(i).getIndex() + 1).getIndex(); j < permutationArrayL2.size(); j++) {
//            System.out.println(bitSet);
                    if (bitSet.get(j)) {
                        //System.out.println("..."+offsetArrayL1[permutationArrayEast[i]]);
                      // System.out.println(bitSet+"...."+j);
                       //
                    }
                }

            }
            catch (ArrayIndexOutOfBoundsException e){
                System.out.println("The Exception is "+permutationArrayL1.get(i - 1) + 1);
            }
        }

       return initialCount;
    }

    public void SearchSingleTuple( int [] permutationArrayL1,  ArrayList<Offset> offsetArrayL1, int [] permutationArrayL2, int min, int max){

        for(int i=min;i<=max;i++){
            // System.out.println("..."+offsetArrayL1[permutationArrayEast[i]]);
            for(int j=offsetArrayL1.get(permutationArrayL1[i-1]+1).getIndex();j<permutationArrayL2.length;j++) {
//            System.out.println(bitSet);
             //   if (bitSet.get(j)) {
                    //System.out.println("..."+offsetArrayL1[permutationArrayEast[i]]);
                  //  System.out.println(bitSet);
                //}
            }}}

    public static ArrayList<Offset> offsetComputation(Node nodeForLeft, Node nodeForRight){
        ArrayList<Offset> offsetArrayList = new ArrayList();
       // Node nodeForLeft = leftBTree.leftMostNode();
       // Node nodeForRight = null;
        int relativeIndexOfLeftInRight = 0;
        Node intermediateNode = null;
        boolean checkIndex = false;

        while (nodeForLeft != null) {
            for (Key keyNode : nodeForLeft.getKeys()) {
                int key = keyNode.getKey();
                int valueSize= keyNode.getValues().size();
                if (!checkIndex) {
                   // nodeForRight = rightBTree.searchRelativeNode(key);
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
                            for(int size=0;size<valueSize;size++)
                            offsetArrayList.add(new Offset(key, relativeIndexOfLeftInRight + j + 1));
                            foundKey = true;
                            break;
                        } else if (nodeForRight.getNext() == null && j == nodeForRight.getKeys().size() - 1 && key > nodeForRight.getKeys().get(j).getKey()) {
                            int newIndex = relativeIndexOfLeftInRight + (nodeForRight.getKeys().size() + 1);
                            for(int size=0;size<valueSize;size++)
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

    public static int binarySearchForPermutation(ArrayList<Permutation> permutation, int key) {
        int left = 0;
        int right = permutation.size() - 1;
        int result=permutation.size() - 1;
       // int result=
        while (left <= right) {
            int mid = (left + right) / 2;
            if (permutation.get(mid).getValue() == key) {
                // Found the element
                return mid;
            } else if (permutation.get(mid).getValue() < key) {
                // Search in the right half of the list
                left = mid + 1;
//                if(permutation[mid].getValue()==(permutation.length-1)){
//
//                    return (permutation.length-1);
//                }
               // System.out.println(permutation.length);
            } else {
                // Search in the left half of the list
                right = mid - 1;
                result=mid;
            }
        }

        // Element not found
        return result;
    }


    private Node convertToObject(byte[] byteData) {
        try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(byteData);
             ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream)) {
            Object object = objectInputStream.readObject();
            if (object instanceof Node) {
                return (Node) object;
            } else {
                throw new IllegalArgumentException("Invalid object type after deserialization");
            }
        } catch (ClassNotFoundException | IOException e) {
            e.printStackTrace();
            // Handle the exception appropriately
        }
        return null; // Return null or handle the failure case accordingly
    }
    public void findNode(Node StreamNodeL2){
        int counter=0;
        while(StreamNodeL2!=null){
            for(Key key:StreamNodeL2.getKeys()){
                for(int item: key.getValues()) {
                    counter++;


                }
            }
            StreamNodeL2= StreamNodeL2.getNext();
        }
        System.out.println(counter+".....");
    }

}
