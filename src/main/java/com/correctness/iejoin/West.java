package com.correctness.iejoin;

public class West {
private int id_West;
private int time;
private int cost;
public West(){

}
    public West(int id_West, int time, int cost) {
        this.id_West = id_West;
        this.time = time;
        this.cost = cost;
    }

    public int getId_West() {
        return id_West;
    }

    public void setId_West(int id_West) {
        this.id_West = id_West;
    }

    public int getTime() {
        return time;
    }

    public void setTime(int time) {
        this.time = time;
    }

    public int getCost() {
        return cost;
    }

    public void setCost(int cost) {
        this.cost = cost;
    }
}
