package com.experiment.selfjoin.broadcasthashjoin;

public class BroadCastDataModel {
    private int id;
    private int key;
    public BroadCastDataModel(int id, int key) {
        this.id = id;
        this.key = key;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getKey() {
        return key;
    }

    public void setKey(int key) {
        this.key = key;
    }
}
