package com.proposed.iejoinandbplustreebased;

import com.configurationsandconstants.iejoinandbaseworks.Configuration;
import com.configurationsandconstants.iejoinandbaseworks.Constants;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.BitSet;
import java.util.Map;

public class JoinerBoltForBitSetOperation extends BaseRichBolt {
    private BitSet predicate1BitSet = null;
    private BitSet predicate2BitSet = null;
    private String leftPredicateSourceStreamID = null;
    private String rightPredicateSourceStreamID = null;

    public JoinerBoltForBitSetOperation() {
        Map<String, Object> map = Configuration.configurationConstantForStreamIDs();
        this.leftPredicateSourceStreamID = (String) map.get("LeftPredicateSourceStreamIDBitSet");
        this.rightPredicateSourceStreamID = (String) map.get("RightPredicateSourceStreamIDBitSet");

    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
//            this.leftPredicateSourceStreamID= (String) map.get("LeftPredicateSourceStreamIDBitSet");
//            this.rightPredicateSourceStreamID= (String) map.get("RightPredicateSourceStreamIDBitSet");

    }

    /**
     * Takes two input stream as byte array convert them back to bit array and perform AND, OR, other operation for
     * Completion of results
     * This operation is performed for every new tuple from upstream processing task.
     *
     * @param tuple is a byte array;
     */
    @Override
    public void execute(Tuple tuple) {

        if (tuple.getSourceStreamId().equals(leftPredicateSourceStreamID)) {
            byte[] byteArrayPredicateLeftBitSet = tuple.getBinaryByField(Constants.BYTE_ARRAY);
            predicate1BitSet = convertToObject(byteArrayPredicateLeftBitSet);

        }
        if (tuple.getSourceStreamId().equals(rightPredicateSourceStreamID)) {

            byte[] byteArrayPredicateRightArray = tuple.getBinaryByField(Constants.BYTE_ARRAY);
            predicate2BitSet = convertToObject(byteArrayPredicateRightArray);

        }
        if ((predicate1BitSet != null) && (predicate2BitSet != null)) {
            predicate2BitSet.and(predicate1BitSet);
            predicate1BitSet = null;
            predicate2BitSet = null;
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    // Convert the byte array from source stream to original array
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
