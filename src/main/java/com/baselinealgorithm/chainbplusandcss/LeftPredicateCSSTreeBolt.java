package com.baselinealgorithm.chainbplusandcss;

import com.configurationsandconstants.iejoinandbaseworks.Configuration;
import com.configurationsandconstants.iejoinandbaseworks.Constants;
import com.stormiequality.BTree.BPlusTree;
import com.stormiequality.BTree.Key;
import com.stormiequality.BTree.Node;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;

public class LeftPredicateCSSTreeBolt extends BaseRichBolt {
    private LinkedList<CSSTree> leftStreamLinkedListCSSTree =null;
    private LinkedList<CSSTree> rightStreamLinkedListCSSTree =null;
    private BPlusTree leftStreamBPlusTree =null;
    private BPlusTree rightStreamBPlusTree =null;
    private int archiveCount;
    private int leftStreamArchiveCount;
    private int rightStreamArchiveCount;
    private int tupleRemovalCount;
    private int orderOfTreeBothBPlusTreeAndCSSTree;
    private int tupleRemovalCountForLocal;
    private OutputCollector outputCollector;
    private String leftStreamSmaller;
    private String rightStreamSmaller;
    public LeftPredicateCSSTreeBolt(){
        this.archiveCount=Constants.MUTABLE_WINDOW_SIZE;
        this.tupleRemovalCount= Constants.IMMUTABLE_WINDOW_SIZE;
        this.orderOfTreeBothBPlusTreeAndCSSTree=Constants.ORDER_OF_CSS_TREE;
        Map<String, Object> map= Configuration.configurationConstantForStreamIDs();
        this.leftStreamSmaller= (String) map.get("LeftSmallerPredicateTuple");
        this.rightStreamSmaller= (String) map.get("RightSmallerPredicateTuple");
    }
    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.leftStreamLinkedListCSSTree = new LinkedList<>();
        this.rightStreamLinkedListCSSTree = new LinkedList<>();
        this.leftStreamBPlusTree = new BPlusTree(orderOfTreeBothBPlusTreeAndCSSTree);
        this.rightStreamBPlusTree = new BPlusTree(orderOfTreeBothBPlusTreeAndCSSTree);
        this.leftStreamArchiveCount =0;
        this.rightStreamArchiveCount =0;
        this.outputCollector=outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        tupleRemovalCountForLocal++;
            if(tuple.getSourceStreamId().equals(leftStreamSmaller)){
                leftStreamPredicateEvaluation(tuple,outputCollector);
            }
            if(tuple.getSourceStreamId().equals(rightStreamSmaller)){
                rightStreamPredicateEvaluation(tuple,outputCollector);
            }
            if(tupleRemovalCountForLocal>=tupleRemovalCount){
                rightStreamLinkedListCSSTree.remove(rightStreamLinkedListCSSTree.getFirst());
                leftStreamLinkedListCSSTree.remove(leftStreamLinkedListCSSTree.getFirst());
            }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
    private void rightStreamPredicateEvaluation(Tuple tuple, OutputCollector outputCollector){
        // Insertion
        rightStreamBPlusTree.insert(tuple.getIntegerByField(Constants.TUPLE), tuple.getIntegerByField(Constants.TUPLE_ID));
        rightStreamArchiveCount++;
        HashSet<Integer> hashSetIds= new HashSet<>();
        //Results form BPlus Tree searching
        hashSetIds.addAll(leftStreamBPlusTree.lessThenSpecificValueHash(tuple.getIntegerByField(Constants.TUPLE)));
        // Results from immutable B+ CSS Tree
        if(!leftStreamLinkedListCSSTree.isEmpty())
            for(int i = 0; i< leftStreamLinkedListCSSTree.size(); i++){
                hashSetIds.addAll(leftStreamLinkedListCSSTree.get(i).searchSmaller(tuple.getIntegerByField(Constants.TUPLE)));
            }
        /// Writer Emittier of tuples

        //Archive Interval achieve Then merge the active BPlus Tree into CSS Tree
        if(rightStreamArchiveCount >=archiveCount){
            rightStreamArchiveCount =0;
            Node node= rightStreamBPlusTree.leftMostNode();
            CSSTree cssTree= new CSSTree(orderOfTreeBothBPlusTreeAndCSSTree);
            while(node!=null){
                for(Key key: node.getKeys()){
                    cssTree.insertBulkUpdate(key.getKey(),key.getValues());
                }

                node= node.getNext();
            }
            //Insert into the linkedList
            rightStreamLinkedListCSSTree.add(cssTree);
        }
    }
    private void leftStreamPredicateEvaluation(Tuple tuple, OutputCollector outputCollector){
        leftStreamBPlusTree.insert(tuple.getIntegerByField("Tuple"), tuple.getIntegerByField("ID"));
        leftStreamArchiveCount++;
        HashSet<Integer> hashSetIds= new HashSet<>();
        //Results form BPlus Tree
        hashSetIds.addAll(rightStreamBPlusTree.greaterThenSpecificValueHashSet(tuple.getIntegerByField("Tuple")));
       // Results from immutable B+ CSS Tree
        if(!rightStreamLinkedListCSSTree.isEmpty())
        for(int i = 0; i< rightStreamLinkedListCSSTree.size(); i++){
            hashSetIds.addAll(rightStreamLinkedListCSSTree.get(i).searchGreater(tuple.getIntegerByField("Tuple")));
        }
        /// Writer Emittier of tuples

        //Archive Interval achieve Then merge the active BPlus Tree into CSS Tree
        if(leftStreamArchiveCount >=archiveCount){
            leftStreamArchiveCount =0;
          Node node= leftStreamBPlusTree.leftMostNode();
          CSSTree cssTree= new CSSTree(orderOfTreeBothBPlusTreeAndCSSTree);
          while(node!=null){
              for(Key key: node.getKeys()){
                  cssTree.insertBulkUpdate(key.getKey(),key.getValues());
              }

              node= node.getNext();
          }
          //Insert into the linkedList
          leftStreamLinkedListCSSTree.add(cssTree);
        }

    }

}
