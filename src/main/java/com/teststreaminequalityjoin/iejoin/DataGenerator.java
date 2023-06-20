package com.teststreaminequalityjoin.iejoin;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Map;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class DataGenerator extends BaseRichSpout {
    public static final String PROFIT_STREAM_ID = "num";
    public static final String EMPTY_TAXIS_STREAM_ID = "den";
    boolean _feof;
    private SpoutOutputCollector collector;
    private String dataPath;
    private BufferedReader reader;
    int count=0;
    private long lastTs;
    private int tupleID = 0;
   // private TaskMonitor taskMonitor;
//    private static java.util.logging.Logger logger = Logger.getLogger("Log");
//    private static FileHandler fh;
    public DataGenerator(String dataPath) {
        this.dataPath = dataPath;
        this.lastTs = 0L;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
//        try{
//            //  writer = new BufferedWriter(new FileWriter(new File("/public/home/abrar/output/result.csv")));
//            fh = new FileHandler("/home/adeelaslam/Input/log");
//            logger.addHandler(fh);
//            SimpleFormatter formatter = new SimpleFormatter();
//            fh.setFormatter(formatter);
//        }catch (Exception e){
//
//        }
        this._feof = false;
        this.collector = collector;
        count=0;

        try {
            this.reader = new BufferedReader(new FileReader(new File(dataPath)));
        }
        catch (Exception e){

        }

        }


    @Override
    public void nextTuple() {
        try{

            //taskMonitor.checkThreadId();
            String line= reader.readLine();
            // count++;
               // System.out.println(count);
            if(line!=null){
                TaxiRide tr = TaxiRide.parse(line);
               // System.exit(0);
                Values tuple = new Values(tupleID, tr, tr.pickupCell, tr.taxiID,System.currentTimeMillis());
                long newTs = tr.dropoffTS.getTime();
                this.collector.emit(PROFIT_STREAM_ID, tuple);
                this.collector.emit(EMPTY_TAXIS_STREAM_ID, tuple);
               // logger.info(tr+"Information");
                tupleID++;
//                if (lastTs > 0) {
//                    // we have to sleep SECONDS_PER_TIME_UNIT
//                    // for each TIME_UNIT_IN_SECONDS passed from last tuple
//                    // to this one
//                    System.out.println("..."+tuple);
//                  //  System.exit(0);
//                    // long fromTupleToSystemTime = WindowBolt.TIME_UNIT_IN_SECONDS * WindowBolt.SECONDS_PER_TIME_UNIT;
////                    long sleepTime = (newTs - lastTs) /  fromTupleToSystemTime;
////                    Utils.sleep(sleepTime);
//                    lastTs = newTs;
//
//                    this.collector.emit(PROFIT_STREAM_ID, tuple);
//                    this.collector.emit(EMPTY_TAXIS_STREAM_ID, tuple);
//                    tupleID++;
//                }
//                else if (!_feof){
//                    _feof = true;
//                }
            }
        }
        catch (Exception e)
        {
            System.out.println();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        Fields fields =  new Fields("tupleID", "taxiRide", "pickupCell", "taxiID","SpoutTime");
        outputFieldsDeclarer.declareStream(PROFIT_STREAM_ID, fields);
        outputFieldsDeclarer.declareStream(EMPTY_TAXIS_STREAM_ID, fields);
    }
}
