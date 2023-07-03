package com.stormiequality.test;

public class TestingDomain {
    private int id;
    private String url;
    public TestingDomain(int id){
        this.id=id;
    }
    public TestingDomain(int id, String url){
        this.id=id;
        this.url= url;
    }
    public Integer getId() { return Integer.valueOf(id); }
}
