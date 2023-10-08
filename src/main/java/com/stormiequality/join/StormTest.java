package com.stormiequality.join;

import com.stormiequality.BTree.BPlusTreeUpdated;
import com.stormiequality.BTree.Key;
import com.stormiequality.BTree.Node;
import com.stormiequality.BTree.Offset;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

import java.util.ArrayList;

public class StormTest {
    public static void main(String[] args) throws Exception {
        Config config = new Config();
        config.setDebug(true);
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("testSpout", new TestSpout());
//
        builder.setBolt("testBolt", new TestBolt()).customGrouping("testSpout", new RoundRobinGrouping()).setNumTasks(10);
//                .shuffleGrouping("testSpout");
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("storm", config, builder.createTopology());
      //  BitSet bitSet= new BitSet(100);
//        BPlusTree bPlusTree = new BPlusTree();
//        bPlusTree.initialize(4);
//        bPlusTree.insert(10, 0);
//        bPlusTree.insert(5, 1);
//        bPlusTree.insert(6, 2);
//        bPlusTree.insert(11, 3);
//        bPlusTree.insert(12, 4);
//        bPlusTree.insert(7, 5);
//        bPlusTree.insert(8, 6);
//        bPlusTree.insert(13, 7);
//        bPlusTree.insert(9, 8);
//        bPlusTree.insert(19, 9);
//        bPlusTree.insert(16, 10);
//        bPlusTree.insert(20, 11);
//        bPlusTree.insert(21, 12);
//        BitSet bitset= bPlusTree.lessThenSpecificValue(20, 100);
//        System.out.println(bitset);
//        BPlusTree bPlusTree1 = new BPlusTree();
//        bPlusTree1.initialize(4);
//        bPlusTree1.insert(20, 3);
//        bPlusTree1.insert(10, 2);
//        bPlusTree1.insert(5, 1);
//        bPlusTree1.insert(3, 4);
//        bPlusTree1.insert(19, 5);
//        bPlusTree1.insert(12, 7);
//        bPlusTree1.insert(8, 9);
//        bPlusTree1.insert(13, 10);
//      ArrayList<Offset> offsetArrayList = JoinBoltForBPlusTree.offsetComputation(bPlusTree, bPlusTree1);
//       // ArrayList<Offset> offsetArrayList = StormTest.computation1(bPlusTree, bPlusTree1);
//        //  System.out.println(offsetArrayList.size());
//        for (int i = 0; i < offsetArrayList.size(); i++) {
//            System.out.println(offsetArrayList.get(i).getIndex());
//        }
    }
public static ArrayList<Offset> computation1 (BPlusTreeUpdated bPlusTree, BPlusTreeUpdated bPlusTree1) {
    ArrayList<Offset> offsetArrayList = new ArrayList();
    Node nodeForLeft = bPlusTree.leftMostNode();
    Node nodeForRight = null;
    int relativeIndexOfLeftInRight = 0;
    Node intermediateNode = null;
    boolean checkIndex = false;

    while (nodeForLeft != null) {
        for (Key keyNode : nodeForLeft.getKeys()) {
            int key = keyNode.getKey();
            if (!checkIndex) {
                nodeForRight = bPlusTree1.searchRelativeNode(key);
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

    public void testData(){
        BPlusTreeUpdated BTree1= new BPlusTreeUpdated();
        BPlusTreeUpdated BTree2=new BPlusTreeUpdated();
        BTree1.initialize(4);
        BTree2.initialize(4);
        BTree1.insert(9,1);
        BTree1.insert(12,2);
        BTree1.insert(5,3);
        BTree2.insert(6,1);
        BTree2.insert(11,2);
        BTree2.insert(10,3);
        BTree2.insert(5,4);
        ArrayList<Offset> offsetArrayList=  JoinBoltForBPlusTree.offsetComputation(BTree1,BTree2);
      //  System.out.println(offsetArrayList.size());
        for(int i=0;i<offsetArrayList.size();i++){
            System.out.println(offsetArrayList.get(i).getIndex());
        }

    }

    /*
     ArrayList<Offset> offsetArrayList= new ArrayList<Offset>();
        Node nodeForLeft =bPlusTree.leftMostNode();
        Node  nodeForRight=null;
        int relativeIndexOfLeftInRight=0;
        Node intermediateNode=null;
        boolean checkIndex=false;
        int key=0;
        while(nodeForLeft!=null){
            for(int i=0;i<nodeForLeft.getKeys().size();i++) {
                key=nodeForLeft.getKeys().get(i).getKey();
              if(checkIndex==false) {
                  System.out.println("Here");
               //   key=nodeForLeft.getKeys().get(i).getKey();
                 nodeForRight= bPlusTree1.searchRelativeNode(key);
                  intermediateNode=nodeForRight.getPrev();
                  while (intermediateNode != null) {
                      relativeIndexOfLeftInRight = relativeIndexOfLeftInRight + (intermediateNode.getKeys().size());
                      intermediateNode = intermediateNode.getPrev();
                  }
                  checkIndex = true;
                  System.out.println("...."+relativeIndexOfLeftInRight);
              }

              boolean flagForNodeForRight=false;
            label1:  while(nodeForRight!=null) {
             label2:     for (int j = 0; j < nodeForRight.getKeys().size(); j++) {
             //   System.out.println(nodeForRight);
                      if (nodeForRight.getKeys().get(j).getKey() == key) {
                        //// Add index to Object with Plus of Index
                          Offset offset= new Offset(key,(relativeIndexOfLeftInRight+(j+1)),nodeForRight.getKeys().get(j).getKey() );
                          offsetArrayList.add(offset);
                          flagForNodeForRight=true;
                          break label2;
                      }
                      else if (nodeForRight.getKeys().get(j).getKey() > key){
                          //// Add index to Object with Plus of Index
                          Offset offset= new Offset(key,(relativeIndexOfLeftInRight+(j+1)),nodeForRight.getKeys().get(j).getKey());
                          offsetArrayList.add(offset);
                          flagForNodeForRight=true;
                          break label2;
                      }
                      else if((nodeForRight.getNext()==null)&&(j==(nodeForRight.getKeys().size()-1))){
                          if(key>(nodeForRight.getKeys().get(j).getKey())){
                              Offset offset= new Offset(key,(relativeIndexOfLeftInRight+((nodeForRight.getKeys().size())+1)),nodeForRight.getKeys().get(j).getKey());
                              offsetArrayList.add(offset);
                              flagForNodeForRight=true;
                              break label2;
                          }

                      }
                     // else if() // Logic for last items
                  }
                  if(flagForNodeForRight==false){
                      relativeIndexOfLeftInRight=relativeIndexOfLeftInRight+(nodeForRight.getKeys().size());
                      nodeForRight=nodeForRight.getNext();
                  }else{
                      break label1;
                  }
              }
               // System.out.println(nodeForLeft.getKeys().get(i).getKey()+"..."+nodeForRight.getKeys());
            }
            nodeForLeft= nodeForLeft.getNext();
        }
        for(int i=0;i<offsetArrayList.size();i++){
            System.out.println(offsetArrayList.get(i).getKey()+"..."+offsetArrayList.get(i).getIndex()+"..."+offsetArrayList.get(i).getOtherKey());
        }
        System.out.println(offsetArrayList.size());
    }
     */
}
