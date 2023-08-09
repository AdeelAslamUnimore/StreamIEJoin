package com.stormiequality.join;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class IEJoinPermutationBolt extends BaseRichBolt {
    private Boolean leftFlag;
    private Boolean rightFlag;
    private ArrayList<Permutation> permutationsArrayLeft;
    private ArrayList<Permutation> permutationsArrayRight;
    private List<Integer> taskIds;
    int taskid=0;
    int taskIndex=0;
    private String downStreamTasks;
    private OutputCollector collector;
    private int count;
    private String streamID;
    private String componentID;

    public IEJoinPermutationBolt(String streamID,String downStreamTasks, int count){
        this.downStreamTasks=downStreamTasks;
      this.count=count;
        this.streamID=streamID;
    }
    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.leftFlag=false;
        this.rightFlag=false;
        this.permutationsArrayLeft= new ArrayList<>();
        this.permutationsArrayRight= new ArrayList<>();
        taskIds= topologyContext.getComponentTasks(downStreamTasks);
        this.collector=outputCollector;
        this.taskid=0;
        this.taskIndex=topologyContext.getThisTaskIndex();
        this.componentID=topologyContext.getThisComponentId();

    }


    @Override
    public void execute(Tuple tuple) {

        if(tuple.getSourceStreamId().equals("PermutationLeft")){
            if(tuple.getValueByField("Flag").equals(true)){
                leftFlag=true;
            } else{

                ///Here First Argument is the actual Key and second is
                List<Integer> ids= convertToIntegerList((byte[]) tuple.getValueByField("IDs"));
                permutationsArrayLeft.add(new Permutation(tuple.getIntegerByField("Tuple"),ids));
            }
        }
        if(tuple.getSourceStreamId().equals("PermutationRight")){
            if(tuple.getValueByField("Flag").equals(true)){
                rightFlag=true;
            }else{
                List<Integer> ids= convertToIntegerList((byte[]) tuple.getValueByField("IDs"));
                permutationsArrayRight.add(new Permutation(tuple.getIntegerByField("Tuple"),ids));
            }
        }
        if((leftFlag==true)&&(rightFlag==true)){
          // System.out.println(permutationsArrayLeft.size()+"..."+permutationsArrayRight.size()+"..........."+componentID);
               // permutationComputation(permutationsArrayLeft, permutationsArrayRight, count, taskIds.get(taskid), streamID,tuple);

               permutationComputation(permutationsArrayLeft, permutationsArrayRight, count, taskIds.get(taskid), streamID,tuple);
             // System.out.println("Here"+permutationArrayList.size()+"..."+componentID);

           taskid++;
            if(taskIds.size()==taskid){
                taskid=0;
            }

            permutationsArrayLeft= new ArrayList<>();
            permutationsArrayRight= new ArrayList<>();
            leftFlag=false;
            rightFlag=false;
          // System.exit(-1);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream("LeftStreamID",new Fields("Index", "Key","Flag","Time"));
       outputFieldsDeclarer.declareStream("RightStreamID",new Fields("Index", "Key","Flag","Time"));

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


    public  void permutationComputation( ArrayList<Permutation> permutationsArrayLeft, ArrayList<Permutation> permutationsArrayRight, int count, int taskID, String streamID, Tuple tuple){

        int [] holdingList= new int[count];
        int counter=1;
        for(int i=0;i<permutationsArrayLeft.size();i++){
            for(int ids: permutationsArrayLeft.get(i).getListOfIDs()){
                holdingList[ids]=counter;
                counter++;
            }
        }
      //ArrayList<Permutation> permutationArray= new ArrayList<Permutation>(holdingList.length);
        for(int i=0;i<permutationsArrayRight.size();i++){
            for(int ids: permutationsArrayRight.get(i).getListOfIDs()){
                //Emit these tuples at once
               collector.emitDirect(taskID,streamID,tuple, new Values(holdingList[ids],permutationsArrayRight.get(i).getTuple(),false,System.currentTimeMillis()));
             //  permutationArray.add(new Permutation(holdingList[ids],permutationsArrayRight.get(i).getIndex()));
            }
        }
        collector.emitDirect(taskID,streamID,tuple, new Values(0,0,true,System.currentTimeMillis()));

        //ystem.out.println(permutationArray.size());
      // return permutationArray;

    }

}
