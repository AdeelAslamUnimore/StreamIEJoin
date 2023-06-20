package com.teststreaminequalityjoin.iejoin;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;


import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class DataWriter extends BaseRichBolt {
    private String outputFileName;
    private BufferedWriter writer;
    private BufferedWriter resultWriter;
    private OutputCollector collector;
  //  private TaskMonitor taskMonitor;
    private static Logger logger = Logger.getLogger("Log");
    private static FileHandler fh;
    public DataWriter(String outputFileName) {
        this.outputFileName = outputFileName;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector outputCollector) {
                try{
            //  writer = new BufferedWriter(new FileWriter(new File("/public/home/abrar/output/result.csv")));
            fh = new FileHandler("/home/adeelaslam/Input/log");
            logger.addHandler(fh);
            SimpleFormatter formatter = new SimpleFormatter();
            fh.setFormatter(formatter);
        }catch (Exception e){

        }
        try {
//            WorkerMonitor.getInstance().setContextInfo(context);
//            taskMonitor=new TaskMonitor(context.getThisTaskId());
            this.collector=collector;
            this.resultWriter= new BufferedWriter(new FileWriter(new File("/home/adeelaslam/OutputResultForScheduler/DebsOnlineScheduler.csv")));
            resultWriter.write("DataEnteringTime,DataCompilationTime\n");
            this.writer = new BufferedWriter(
                    new OutputStreamWriter(
                            new FileOutputStream(outputFileName)
                    )
            );
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Error in opening " + outputFileName);
        }

        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple input) {
        logger.info("tuples"+input);
        //System.out.println(input);
        //taskMonitor.notifyTupleReceived(input);
        String line = getNewLine(input);
        try {
            resultWriter.write(input.getLongByField("SpoutTime")+","+System.currentTimeMillis()+"\n");
            resultWriter.flush();
            writer.write(line + "\n\n");
            writer.flush();
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Error in writing to " + outputFileName);
        }

        collector.ack(input);
    }
    protected String getNewLine(Tuple t) {
        List<Tuple> ranking = (List<Tuple>) t.getValue(0);
        String l = "{\n";

        for (Tuple r : ranking) {
            if (r != null) {
                String cell = r.getString(1);
                Double profit = r.getDouble(2);
                l += "\t\'" + cell + "\': " + profit + ",\n";
            } else {
                l += "\tNULL,\n";
            }
        }

        l = l.substring(0, l.length() - 2); // remove last comma
        l += "\n}";
        return l;
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
