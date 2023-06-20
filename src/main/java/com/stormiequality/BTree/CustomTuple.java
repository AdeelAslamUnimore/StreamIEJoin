package com.stormiequality.BTree;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.task.GeneralTopologyContext;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.MessageId;
import org.apache.storm.tuple.Tuple;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class CustomTuple implements Tuple, Serializable {
    private Node leftData;
    private Node rightData;
    private Node searchedData;

    public CustomTuple(Node leftData, Node rightData, Node searchedData) {
        this.leftData = leftData;
        this.rightData=rightData;
        this.searchedData=searchedData;


    }
    @Override
    public List<Object> getValues() {
        return Arrays.asList(leftData, rightData, searchedData);
    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public boolean contains(String s) {
        return false;
    }

    @Override
    public Fields getFields() {
        return null;
    }

    @Override
    public int fieldIndex(String s) {
        return 0;
    }

    @Override
    public List<Object> select(Fields fields) {
        return null;
    }

    @Override
    public Object getValue(int i) {
        return null;
    }

    @Override
    public String getString(int i) {
        return null;
    }

    @Override
    public Integer getInteger(int i) {
        return null;
    }

    @Override
    public Long getLong(int i) {
        return null;
    }

    @Override
    public Boolean getBoolean(int i) {
        return null;
    }

    @Override
    public Short getShort(int i) {
        return null;
    }

    @Override
    public Byte getByte(int i) {
        return null;
    }

    @Override
    public Double getDouble(int i) {
        return null;
    }

    @Override
    public Float getFloat(int i) {
        return null;
    }

    @Override
    public byte[] getBinary(int i) {
        return new byte[0];
    }

    @Override
    public Object getValueByField(String field) {
        if ("leftData".equals(field)) {
            return leftData;
        } else if ("rightData".equals(field)) {
            return rightData;
        } else if ("searchedData".equals(field)) {
            return searchedData;
        }
        return null;
    }

    @Override
    public String getStringByField(String s) {
        return null;
    }

    @Override
    public Integer getIntegerByField(String s) {
        return null;
    }

    @Override
    public Long getLongByField(String s) {
        return null;
    }

    @Override
    public Boolean getBooleanByField(String s) {
        return null;
    }

    @Override
    public Short getShortByField(String s) {
        return null;
    }

    @Override
    public Byte getByteByField(String s) {
        return null;
    }

    @Override
    public Double getDoubleByField(String s) {
        return null;
    }

    @Override
    public Float getFloatByField(String s) {
        return null;
    }

    @Override
    public byte[] getBinaryByField(String s) {
        return new byte[0];
    }

    @Override
    public GlobalStreamId getSourceGlobalStreamId() {
        return null;
    }

    @Override
    public String getSourceComponent() {
        return null;
    }

    @Override
    public int getSourceTask() {
        return 0;
    }

    @Override
    public String getSourceStreamId() {
        return null;
    }

    @Override
    public MessageId getMessageId() {
        return null;
    }

    @Override
    public GeneralTopologyContext getContext() {
        return null;
    }

    // Implement other methods of the Tuple interface as needed
}
