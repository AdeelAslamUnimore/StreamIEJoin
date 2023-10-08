package com.stormiequality.inputdata;

import com.configurationsandconstants.iejoinandbaseworks.Constants;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.*;
import java.util.Map;
import java.util.Random;

public class RackPowerSpout extends BaseRichSpout {
    private SpoutOutputCollector spoutOutputCollector;
    private int id;
    //private int counter;

    private BufferedReader bufferedReader;
    private String filePath;
    private String fileWriterTest2;
    private BufferedWriter bufferedWriter;
    private Random random;

    public RackPowerSpout(){
        this.filePath="/home/adeel/Data/Mounted/Power_merged_file.csv";
      //  this.fileWriterTest2= "/home/adeel/Data/Results/fileEast.csv";


        // this.filePath="C://Users//Adeel//Desktop//TestFolder//14-34Power.csv";

    }
    @Override
    public void open(Map<String, Object> map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector=spoutOutputCollector;
        this.random= new Random();
        this.id=1;
        try{
            this.bufferedReader= new BufferedReader(new FileReader(new File(filePath)));
            //this.bufferedWriter= new BufferedWriter(new FileWriter(new File(fileWriterTest2)));

        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Override
    public void nextTuple() {
        id++;
        try {
            String line = bufferedReader.readLine();

            if (line != null) {
                String [] data= line.split(",");
//                this.bufferedWriter.write(line+"\n");
//                this.bufferedWriter.flush();
                try {
                    Values left = new Values(Integer.parseInt(data[0]),Integer.parseInt(data[1]),id, System.currentTimeMillis(),System.currentTimeMillis());
                    Values right = new Values(Integer.parseInt(data[2]),Integer.parseInt(data[3]),id, System.currentTimeMillis(),System.currentTimeMillis());

                    this.spoutOutputCollector.emit("StreamR",left);
                    Thread.sleep(this.random.nextInt(4));
                    this.spoutOutputCollector.emit("StreamS",right);

                }
                catch (NumberFormatException e){
                    //e.printStackTrace();
                }
            } else {
                // Sleep briefly if no more data is available to avoid busy-waiting
                Thread.sleep(100);
            }
        } catch (IOException | InterruptedException e) {

            throw new RuntimeException("Error reading data from file", e);
        }
    }
        @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declareStream("StreamR", new Fields("Duration","Revenue","ID", Constants.KAFKA_SPOUT_TIME, Constants.KAFKA_TIME));
            outputFieldsDeclarer.declareStream("StreamS", new Fields("Time","Cost","ID", Constants.KAFKA_SPOUT_TIME, Constants.KAFKA_TIME));

        }

    @Override
    public void close() {
        super.close();
        try {
            if (bufferedReader != null) {
                bufferedReader.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
