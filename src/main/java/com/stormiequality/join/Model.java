package com.stormiequality.join;

public class Model {
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    private String name;

    public Model(String name) {
        this.name = name;
    }
}
