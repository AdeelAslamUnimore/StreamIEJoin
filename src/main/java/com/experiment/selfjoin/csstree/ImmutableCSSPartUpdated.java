package com.experiment.selfjoin.csstree;

import com.baselinealgorithm.chainbplusandcss.CSSTreeUpdated;
import com.configurationsandconstants.iejoinandbaseworks.Configuration;
import com.configurationsandconstants.iejoinandbaseworks.Constants;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import javax.print.DocFlavor;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.*;

public class ImmutableCSSPartUpdated extends BaseRichBolt {
    private ConcurrentLinkedDeque<CSSTreeUpdated> leftStreamCSSLinkedList;
    private ConcurrentLinkedDeque<CSSTreeUpdated> rightStreamCSSLinkedList;
    private CSSTreeUpdated leftCSSTree;
    private CSSTreeUpdated rightCSSTree;
    private OutputCollector outputCollector;
    private int taskID;
    private String hostName;
    private String leftStreamGreater;
    private String rightStreamSmaller;
    private Queue<Tuple> tuplesDuringMergeQueue;

    private int leftTupleRemovalCounter;
    private int rightTupleRemovalCounter;

    private boolean flagLeftDuringMerge;

    private boolean flagRightDuringMerge;

    private BitSet bitSetEvaluationDuringMergeLeft;
    private BitSet bitSetEvaluationDuringMergeRight;

    private long leftMergeStart;
    private long rightMergeStart;

    private StringBuilder stringBuilder;
    private  int counter;
    private BufferedWriter bufferedWriter;
    public ImmutableCSSPartUpdated(){
        Map<String, Object> map = Configuration.configurationConstantForStreamIDs();
        this.leftStreamGreater = (String) map.get("LeftGreaterPredicateTuple");
        this.rightStreamSmaller=(String) map.get("RightSmallerPredicateTuple");
    }
    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.leftStreamCSSLinkedList= new ConcurrentLinkedDeque<>();
        this.rightStreamCSSLinkedList= new ConcurrentLinkedDeque<>();
        this.leftCSSTree= new CSSTreeUpdated(Constants.ORDER_OF_CSS_TREE);
        this.rightCSSTree= new CSSTreeUpdated(Constants.ORDER_OF_CSS_TREE);
        this.tuplesDuringMergeQueue= new LinkedList<>();

        this.taskID= topologyContext.getThisTaskId();
        try{
            hostName= InetAddress.getLocalHost().getHostName();
            this.stringBuilder= new StringBuilder();
            this.bufferedWriter= new BufferedWriter(new FileWriter(new File("/home/adeel/Data/Results/RecordEvaluationCSS.csv")));
            this.bufferedWriter.write("TupleID, KafkaTime, TupleInsertionTime, ArrivalTime, EvaluatedTime, HostName, TaskID \n ");
            this.bufferedWriter.flush();
        }catch (Exception e){
            e.printStackTrace();
        }
        this.outputCollector= outputCollector;
        this.leftTupleRemovalCounter=0;
        this.rightTupleRemovalCounter=0;
        this.flagLeftDuringMerge=false;
        this.flagRightDuringMerge=false;
    }

    @Override
    public void execute(Tuple tuple) {

        if (tuple.getSourceStreamId().equals(leftStreamGreater)) {
            flagLeftDuringMerge=true;
            leftImmutableBatchInsertion( tuple);
        }
        if (tuple.getSourceStreamId().equals(rightStreamSmaller)) {
            flagRightDuringMerge=true;
             rightImmutableBatchInsertion(tuple);
        }

         if(tuple.getSourceStreamId().equals("StreamR")){

            long time=System.currentTimeMillis();
          if(flagLeftDuringMerge||flagRightDuringMerge){
              tuplesDuringMergeQueue.offer(tuple);
          }
             if(!flagLeftDuringMerge&&!flagRightDuringMerge&&!tuplesDuringMergeQueue.isEmpty()){
                 tuplesDuringMergeQueue= new LinkedList<>();
             }
          ExecutorService executor = Executors.newFixedThreadPool(2);

          Callable<BitSet> leftTask = () -> leftImmutableTupleEvaluation(tuple.getInteger(0));
          Callable<BitSet> rightTask = () -> rightImmutableTupleEvaluation(tuple.getInteger(1));

          Future<BitSet> leftResult = executor.submit(leftTask);
          Future<BitSet> rightResult = executor.submit(rightTask);

          try {
              BitSet leftStream = leftResult.get();
              BitSet rightStream = rightResult.get();
              if((leftStream!=null)&&(rightStream!=null)){
                  leftStream.and(rightStream);
                  stringBuilder.append(tuple.getValueByField("ID")+","+tuple.getValueByField("kafkaTime")+","+tuple.getValueByField("Time")+","+time+",");
                  this.stringBuilder.append(System.currentTimeMillis()+","+hostName+","+taskID+"\n");
                  this.counter++;
                  if(counter==1000){
                      this.bufferedWriter.write(stringBuilder.toString());
                      this.bufferedWriter.flush();
                      counter=0;
                      this.stringBuilder= new StringBuilder();
                  }
              }
              // Now you can work with leftStream and rightStream concurrently generated.

          } catch (Exception e) {
              e.printStackTrace();
          } finally {
              executor.shutdown();
          }
      }

         if(bitSetEvaluationDuringMergeLeft!=null&&bitSetEvaluationDuringMergeRight!=null){
             bitSetEvaluationDuringMergeLeft.and(bitSetEvaluationDuringMergeLeft);
             bitSetEvaluationDuringMergeLeft=null;
             bitSetEvaluationDuringMergeRight=null;
         }
         if(tuple.getSourceStreamId().equals("LeftCheckForMerge")){
             leftMergeStart=System.currentTimeMillis();
         }
         if(tuple.getSourceStreamId().equals("RightCheckForMerge")){
             rightMergeStart=System.currentTimeMillis();
         }



    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream("MergingTime", new Fields("StartTime","EndTime","Machine","TaskID","StreamID"));
        outputFieldsDeclarer.declareStream("QueueMergeTime", new Fields("StartTime","EndTime", "Machine","TaskID","StreamID"));
    }

    public synchronized void leftImmutableBatchInsertion(Tuple tuple){
        if (tuple.getBooleanByField(Constants.BATCH_CSS_FLAG)) {
            leftStreamCSSLinkedList.add(leftCSSTree);
            //leftMergeStart= System.currentTimeMillis()-leftMergeStart;
            this.outputCollector.emit("MergingTime", new Values(leftMergeStart, System.currentTimeMillis(), hostName, taskID,"LeftStream"));
            this.outputCollector.ack(tuple);
            flagLeftDuringMerge=false;
            bitSetEvaluationDuringMergeLeft= new BitSet();
            long queueMergeTime= System.currentTimeMillis();
            for(Tuple key: tuplesDuringMergeQueue){
                 leftCSSTree.searchSmallerBitSet(key.getInteger(0), bitSetEvaluationDuringMergeLeft);
            }
           // queueMergeTime=System.currentTimeMillis()-queueMergeTime;
            this.outputCollector.emit("QueueMergeTime",new Values(queueMergeTime, System.currentTimeMillis(),hostName,taskID,"LeftStream"));
            this.outputCollector.ack(tuple);
            leftCSSTree= new CSSTreeUpdated(Constants.ORDER_OF_CSS_TREE);

            leftTupleRemovalCounter= leftTupleRemovalCounter+Constants.MUTABLE_WINDOW_SIZE;
            if(leftTupleRemovalCounter>=Constants.IMMUTABLE_CSS_PART_REMOVAL){
                leftStreamCSSLinkedList.removeFirst();
                leftTupleRemovalCounter= leftTupleRemovalCounter-Constants.MUTABLE_WINDOW_SIZE;
            }
        }else{
           this. leftCSSTree.insert(tuple.getIntegerByField(Constants.BATCH_CSS_TREE_KEY),
                    (tuple.getIntegerByField(Constants.BATCH_CSS_TREE_VALUES)));
        }


    }
    public synchronized void rightImmutableBatchInsertion(Tuple tuple){
        if (tuple.getBooleanByField(Constants.BATCH_CSS_FLAG)) {
            rightStreamCSSLinkedList.add(rightCSSTree);
          //  rightMergeStart=System.currentTimeMillis()-rightMergeStart;
            this.outputCollector.emit("MergingTime", new Values(rightMergeStart, System.currentTimeMillis(), hostName, taskID,"RightStream"));
            this.outputCollector.ack(tuple);
            flagRightDuringMerge=false;
            bitSetEvaluationDuringMergeRight= new BitSet();
            long queueMergeTime= System.currentTimeMillis();
            for(Tuple key: tuplesDuringMergeQueue){
                rightCSSTree.searchGreaterBitSet(key.getInteger(1), bitSetEvaluationDuringMergeRight);
            }
           // queueMergeTime=System.currentTimeMillis()-queueMergeTime;
            this.outputCollector.emit("QueueMergeTime",new Values(queueMergeTime, System.currentTimeMillis(),hostName,taskID,"RightStream"));
            this.outputCollector.ack(tuple);
            rightCSSTree= new CSSTreeUpdated(Constants.ORDER_OF_CSS_TREE);
           rightTupleRemovalCounter= rightTupleRemovalCounter+Constants.MUTABLE_WINDOW_SIZE;
            if(rightTupleRemovalCounter>=Constants.IMMUTABLE_CSS_PART_REMOVAL){
                rightStreamCSSLinkedList.removeFirst();
                rightTupleRemovalCounter= rightTupleRemovalCounter-Constants.MUTABLE_WINDOW_SIZE;
            }
        }else{
            this. rightCSSTree.insert(tuple.getIntegerByField(Constants.BATCH_CSS_TREE_KEY),
                    (tuple.getIntegerByField(Constants.BATCH_CSS_TREE_VALUES)));
        }

    }
    public synchronized  BitSet leftImmutableTupleEvaluation(int tuple){
        BitSet bitSet= null;
//        HashSet<Integer> hashSet=null;
        if(!leftStreamCSSLinkedList.isEmpty()) {
           // hashSet= new HashSet<>();
            bitSet= new BitSet();
            leftTupleRemovalCounter++;
            for (CSSTreeUpdated cssTreeUpdated : leftStreamCSSLinkedList) {
               cssTreeUpdated.searchSmallerBitSet(tuple, bitSet);
               // hashSet.addAll(cssTreeUpdated.searchSmaller(tuple));
            }
        }
        return bitSet;
    }
    public synchronized BitSet rightImmutableTupleEvaluation(int tuple){
       // BitSet bitSet= new BitSet();
        BitSet bitSet= null;
        //HashSet<Integer> hashSet=null;
        if(!rightStreamCSSLinkedList.isEmpty()) {
            bitSet= new BitSet();
            rightTupleRemovalCounter++;
            for (CSSTreeUpdated cssTreeUpdated : rightStreamCSSLinkedList) {
              cssTreeUpdated.searchGreaterBitSet(tuple, bitSet);
               // hashSet.addAll(cssTreeUpdated.searchGreater(tuple));
            }
        }
        return bitSet;
    }

}
