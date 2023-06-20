package com.correctness.iejoin;

public class East {
    private int id_East;
    private int duration;
    private int revenue;
 public East (){

 }
    public East(int id_East, int duration, int revenue) {
        this.id_East = id_East;
        this.duration = duration;
        this.revenue = revenue;
    }

    public int getId_East() {
        return id_East;
    }

    public void setId_East(int id_East) {
        this.id_East = id_East;
    }

    public int getDuration() {
        return duration;
    }

    public void setDuration(int duration) {
        this.duration = duration;
    }

    public int getRevenue() {
        return revenue;
    }

    public void setRevenue(int revenue) {
        this.revenue = revenue;
    }
}
