package com.baselinealgorithm.chainbplusandcss;

import java.util.List;

public class Block {

    private List<Node> listOfNodes;

    private Node parentNode;

    public Node getParentNode() {
        return parentNode;
    }

    public void setParentNode(Node parentNode) {
        this.parentNode = parentNode;
    }

    public List<Node> getListOfNodes() {
        return listOfNodes;
    }

    public void setListOfNodes(List<Node> listOfNodes) {
        this.listOfNodes = listOfNodes;
    }

    @Override
    public String toString() {
        return "Block{" +
                "listOfNodes=" + listOfNodes +
                '}';
    }
}