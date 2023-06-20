package com.teststreaminequalityjoin.iejoin;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;


import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class RankingBolt extends BaseRichBolt {
    private OutputCollector collector;
   // private TaskMonitor taskMonitor;
    private BufferedWriter resultWriter;
    private Tuple[] ranking;
    private static Logger logger = Logger.getLogger("Log");
    private static FileHandler fh;
    private  long count= 0L;;
    public static int rankingLength;
    public RankingBolt(int topN) {
        rankingLength = topN;
    }
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        try{
            count=0;
            this.resultWriter= new BufferedWriter(new FileWriter(new File("/home/adeelaslam/OutputResultForScheduler/DebsOnlineSchedulerT.csv")));
            resultWriter.write("DataEnteringTime,DataCompilationTime\n");
            //  writer = new BufferedWriter(new FileWriter(new File("/public/home/abrar/output/result.csv")));
        //    fh = new FileHandler("/home/adeelaslam/Input/log");
//            logger.addHandler(fh);
//            SimpleFormatter formatter = new SimpleFormatter();
//            fh.setFormatter(formatter);
        }catch (Exception e){

        }
        this.collector = collector;
//        WorkerMonitor.getInstance().setContextInfo(context);
//        taskMonitor=new TaskMonitor(context.getThisTaskId());
        this.ranking = new Tuple[rankingLength];
    }

    @Override
    public void execute(Tuple input) {
        count++;
        //logger.info("tuples"+input);
        //taskMonitor.notifyTupleReceived(input);
        boolean changed = false;
        double currProfitability = input.getDouble(2);
        String currCell = input.getString(1);
        int windowID = input.getInteger(0);

        for (int i = 0; i < rankingLength && !changed; i++) {
            if (ranking[i] == null) {
                ranking[i] = input;
                changed = true;
            } else {
                double profitability = ranking[i].getDouble(2);
                String cell = ranking[i].getString(1);

                if (currProfitability > profitability) {
                    // shift right
                    for (int j = rankingLength - 1; j > i; j--) {
                        ranking[j] = ranking[j - 1];
                    }
                    ranking[i] = input;
                    changed = true;
                } else if (currCell.equals(cell) && currProfitability == profitability) {
                    // avoid that the same tuple received
                    // more times fills the rankings
                    break;
                }
            }
        }
        try {
            resultWriter.write(input.getLongByField("SpoutTime")+","+System.currentTimeMillis()+"\n");
            resultWriter.flush();
//            if(count==4000000000L){
//                resultWriter.close();
//            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Error in writing to " );
        }
        if (changed) {
            //LOG.info(windowID + " -> " + currCell + ": " + currProfitability);
            this.collector.emit(new Values(Arrays.asList(this.ranking),input.getLongByField("SpoutTime")));
        }

        this.collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("ranking","SpoutTime"));
    }
}
