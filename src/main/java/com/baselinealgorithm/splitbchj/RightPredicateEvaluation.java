package com.baselinealgorithm.splitbchj;

import com.configurationsandconstants.iejoinandbaseworks.Constants;
import com.experiment.selfjoin.broadcasthashjoin.BroadCastDataModel;
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
import java.util.Map;

public class RightPredicateEvaluation extends BaseRichBolt {
    private ArrayList<BroadCastDataModel> listForTupleFromStreamR;
    private ArrayList<BroadCastDataModel> listForTupleFromStreamS;
    private String streamRIDInsertion;
    private String streamSIDInsertion;
    private String streamRIDSearch;
    private String streamSIDSearch;
    private OutputCollector outputCollector;
    private int taskID;
    private String hostName;
    public RightPredicateEvaluation(String streamRIDInsertion, String streamSIDInsertion, String streamRIDSearch, String streamSIDSearch){
        this.streamRIDInsertion = streamRIDInsertion;
        this.streamSIDInsertion = streamSIDInsertion;
        this.streamRIDSearch=streamRIDSearch;
        this.streamSIDSearch=streamSIDSearch;
    }
    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.listForTupleFromStreamR= new ArrayList<>();
        this.listForTupleFromStreamS= new ArrayList<>();
        this.outputCollector=outputCollector;
        this.taskID=topologyContext.getThisTaskId();
        try{
            this.hostName= InetAddress.getLocalHost().getHostName();
        }catch (Exception e){
            e.printStackTrace();
        }

    }

    @Override
    public void execute(Tuple tuple) {
        long tupleArrivalTime=System.currentTimeMillis();
        if(tuple.getSourceStreamId().equals(streamRIDInsertion)){
            this.listForTupleFromStreamR.add(new BroadCastDataModel(tuple.getIntegerByField(Constants.TUPLE),
                    tuple.getIntegerByField("ID")));
        }
        if(tuple.getSourceStreamId().equals(streamRIDSearch)) {
            BitSet bitSet = new BitSet();
            int revenue= tuple.getIntegerByField(Constants.TUPLE);
            if(!listForTupleFromStreamS.isEmpty()){
            for(int i=0;i<listForTupleFromStreamS.size();i++){
                if(revenue>listForTupleFromStreamS.get(i).getKey()){
                    bitSet.set(listForTupleFromStreamS.get(i).getId());

                }
            }
                if(this.listForTupleFromStreamR.size()>=Constants.IMMUTABLE_CSS_PART_REMOVAL){
                    for(int i=0;i<Constants.MUTABLE_WINDOW_SIZE;i++){
                        this.listForTupleFromStreamR.remove(listForTupleFromStreamR.get(i));
                    }
                }
                try {
                    this.outputCollector.emit("StreamR", new Values( tuple.getIntegerByField("ID"),
                            convertToByteArray(bitSet), tuple.getLongByField(Constants.KAFKA_TIME),
                            tuple.getLongByField(Constants.KAFKA_SPOUT_TIME), tupleArrivalTime, System.currentTimeMillis(), taskID,
                            hostName));
                    this.outputCollector.ack(tuple);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }


        }
        if(tuple.getSourceStreamId().equals(streamSIDInsertion)){

            this.listForTupleFromStreamS.add(new BroadCastDataModel(tuple.getIntegerByField(Constants.TUPLE),
                    tuple.getIntegerByField("ID")));
        }
        if(tuple.getSourceStreamId().equals(streamSIDSearch)) {
            BitSet bitSet = new BitSet();
            int cost= tuple.getIntegerByField(Constants.TUPLE);
            if(!listForTupleFromStreamR.isEmpty()) {
                for (int i = 0; i < listForTupleFromStreamR.size(); i++) {
                    if (cost < listForTupleFromStreamR.get(i).getKey()) {
                        bitSet.set(listForTupleFromStreamR.get(i).getId());

                    }
                }
                if(this.listForTupleFromStreamS.size()>=Constants.IMMUTABLE_CSS_PART_REMOVAL){
                    for(int i=0;i<Constants.MUTABLE_WINDOW_SIZE;i++){
                        this.listForTupleFromStreamS.remove(listForTupleFromStreamS.get(i));
                    }
                }
                try {
                    this.outputCollector.emit("StreamS", new Values( tuple.getIntegerByField("ID"),
                            convertToByteArray(bitSet), tuple.getLongByField(Constants.KAFKA_TIME),
                            tuple.getLongByField(Constants.KAFKA_SPOUT_TIME), tupleArrivalTime, System.currentTimeMillis(), taskID,
                            hostName));
                    this.outputCollector.ack(tuple);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }


        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream("StreamR", new Fields( "ID",
                "BitArray", "KafkaTime",
                "KafkaSpoutTime", "tupleArrivalTime", "TupleEvaluationTime", "taskID",
                "hostName"));
        outputFieldsDeclarer.declareStream("StreamS", new Fields( "ID",
                "BitArray", "KafkaTime",
                "KafkaSpoutTime", "tupleArrivalTime", "TupleEvaluationTime", "taskID",
                "hostName"));

    }
    public synchronized byte[] convertToByteArray(Serializable object) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream objectOutputStream = new ObjectOutputStream(baos)) {
            objectOutputStream.writeObject(object);
            objectOutputStream.flush();
        }
        return baos.toByteArray();
    }}