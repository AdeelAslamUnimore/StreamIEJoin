package com.stormiequality.test;

public class Domain {
    private int id;
    private String url;
    public Domain(int id, String url){
        this.id=id;
        this.url= url;
    }
    public Integer getId() { return Integer.valueOf(id); }
}
