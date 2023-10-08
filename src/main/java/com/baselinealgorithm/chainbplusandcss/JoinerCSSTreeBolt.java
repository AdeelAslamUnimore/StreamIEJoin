package com.baselinealgorithm.chainbplusandcss;

import com.configurationsandconstants.iejoinandbaseworks.Configuration;
import com.configurationsandconstants.iejoinandbaseworks.Constants;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.*;
import java.net.InetAddress;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

public class JoinerCSSTreeBolt extends BaseRichBolt {

    private String leftStreamID;
    private String rightStreamID;
    private OutputCollector outputCollector;
    private String result;
    private int taskID;
    private String hostName;
    private Map<String, BitSet> mapForRecordEvaluation;
  //  private Map<String, String > recordMap;
    private BufferedWriter writer;
    private StringBuilder stringBuilder;
    private int recordCounter;
    public JoinerCSSTreeBolt(String leftStreamID, String rightStreamID){
        Map<String, Object> map = Configuration.configurationConstantForStreamIDs();
        this.leftStreamID=leftStreamID;
        this.rightStreamID=rightStreamID;
        this.result= (String) map.get("Results");
    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        taskID= topologyContext.getThisTaskId();
        this.outputCollector=outputCollector;
        try{
            this.mapForRecordEvaluation = new HashMap<>();
         //   this.recordMap= new HashMap<>();

            hostName= InetAddress.getLocalHost().getHostName();
            this.stringBuilder= new StringBuilder();
            this.writer= new BufferedWriter(new FileWriter(new File("/home/adeel/Data/Results/"+taskID+"JoinerImmutable.csv")));
            this.writer.write("TUPLE_ID ,KAFKA_SPOUT_TIME, KAFKA_TIME, StartEvaluationTime,EndEvaluationTime,TaskId,HostName, EvaluationStartTimeBitSet, EvaluationEndTimeBitSet, TaskID, HostName \n");
           this.writer.flush();
            this.recordCounter=0;
        }catch (Exception e){

        }
    }

    @Override
    public void execute(Tuple tuple) {
        //int streamID=(Integer) tuple.getValue(1);
        long time=System.currentTimeMillis();

        if(tuple.getSourceStreamId().equals(leftStreamID)) {

           BitSet leftHashSet = convertToObject(tuple.getBinaryByField(Constants.LEFT_HASH_SET));
            if(mapForRecordEvaluation.containsKey(tuple.getStringByField(Constants.TUPLE_ID))){
                mapForRecordEvaluation.get(tuple.getStringByField(Constants.TUPLE_ID)).and(leftHashSet);
                mapForRecordEvaluation.remove(tuple.getStringByField(Constants.TUPLE_ID));
              recordCounter++;
              //  if(!recordMap.containsKey(tuple.getStringByField(Constants.TUPLE_ID))) {
                    stringBuilder.append(tuple.getStringByField(Constants.TUPLE_ID) + "," + tuple.getValue(2) + "," +
                            tuple.getValue(3) + "," + tuple
                            .getValue(4) + "," + tuple.getValue(5) + "," + tuple.getValue(6) + "," + tuple.getValue(7) + "," + time + "," +
                            System.currentTimeMillis() + "," + taskID + "," + hostName + "\n");
                }
            else{ mapForRecordEvaluation.put(tuple.getStringByField(Constants.TUPLE_ID), leftHashSet);
            }
//              recordMap.put(tuple.getStringByField(Constants.TUPLE_ID),tuple.getStringByField(Constants.TUPLE_ID)+","+tuple.getValue(2)+","+
//                      tuple.getValue(3)+","+tuple
//                      .getValue(4)+","+tuple.getValue(5)+","+tuple.getValue(6)+","+tuple.getValue(7)+","+time+","+
//                      System.currentTimeMillis()+","+taskID+","+hostName);

            }
         //System.out.println("LeftHashSet"+leftHashSet);

        if(tuple.getSourceStreamId().equals(rightStreamID)){

            BitSet rightHashSet=convertToObject(tuple.getBinaryByField(Constants.RIGHT_HASH_SET));
            if(mapForRecordEvaluation.containsKey(tuple.getStringByField(Constants.TUPLE_ID))){
                mapForRecordEvaluation.get(tuple.getStringByField(Constants.TUPLE_ID)).and(rightHashSet);
                mapForRecordEvaluation.remove(tuple.getStringByField(Constants.TUPLE_ID));
                recordCounter++;
                stringBuilder.append(tuple.getStringByField(Constants.TUPLE_ID)+","+tuple.getValue(2)+","+
                        tuple.getValue(3)+","+tuple
                        .getValue(4)+","+tuple.getValue(5)+","+tuple.getValue(6)+","+tuple.getValue(7)+","+time+","+
                        System.currentTimeMillis()+","+taskID+","+hostName+"\n");
            }else{
                mapForRecordEvaluation.put(tuple.getStringByField(Constants.TUPLE_ID), rightHashSet);
            }

          //  System.out.println("rightStreamID"+rightHashSet);
        }

   if(recordCounter==1000){
          recordCounter=0;
          try{

              this.writer.write(stringBuilder.toString());
              this.writer.flush();
              stringBuilder= new StringBuilder();
          }catch (Exception e){
              e.printStackTrace();
          }
      }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        Fields greaterFields= new Fields(  Constants.TUPLE_ID, Constants.KAFKA_TIME,
                Constants.KAFKA_SPOUT_TIME, Constants.SPLIT_BOLT_TIME, Constants.TASK_ID_FOR_SPLIT_BOLT, Constants.HOST_NAME_FOR_SPLIT_BOLT, "TupleArrivalTime",
                Constants.GREATER_PREDICATE_EVALUATION_TIME_BOLT, Constants.MUTABLE_BOLT_TASK_ID,Constants.MUTABLE_BOLT_MACHINE);
        outputFieldsDeclarer.declareStream(leftStreamID, greaterFields);
        Fields lesserFields= new Fields(  Constants.TUPLE_ID,Constants.KAFKA_TIME,
                Constants.KAFKA_SPOUT_TIME, Constants.SPLIT_BOLT_TIME, Constants.TASK_ID_FOR_SPLIT_BOLT, Constants.HOST_NAME_FOR_SPLIT_BOLT,"TupleArrivalTime",
                Constants.LESSER_PREDICATE_EVALUATION_TIME_BOLT, Constants.MUTABLE_BOLT_TASK_ID,Constants.MUTABLE_BOLT_MACHINE);
        outputFieldsDeclarer.declareStream(rightStreamID, lesserFields);
        outputFieldsDeclarer.declareStream(result, new Fields("Time","timeAfterCalculation", "streamright","TaskID","HostName"));


    }
//    public static HashSet<Integer> convertByteArrayToHashSet(byte[] byteArray) {
//        HashSet<Integer> hashSet = null;
//        try {
//            ByteArrayInputStream bis = new ByteArrayInputStream(byteArray);
//            ObjectInputStream ois = new ObjectInputStream(bis);
//            hashSet = (HashSet<Integer>) ois.readObject();
//        } catch (IOException | ClassNotFoundException e) {
//            e.printStackTrace();
//        }
//        return hashSet;
//    }
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
}
