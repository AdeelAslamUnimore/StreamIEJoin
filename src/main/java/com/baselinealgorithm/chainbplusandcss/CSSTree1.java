package com.baselinealgorithm.chainbplusandcss;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class CSSTree1 {
    private Block rootBlock;
    private List<Block> listBlocks;
    private int orderOfTree;
    public CSSTree1(int orderOfTree){
    this.listBlocks= new ArrayList<>();
    this.orderOfTree=orderOfTree;
    }
    public void insert (int key, int value){
        //Case1: When tree is empty
        if(null==this.rootBlock){
        this.rootBlock= new Block();
        Key key1= new Key();
        List<Integer> values= new ArrayList<>();
        key1.setKey(key);
        values.add(value);
        key1.setValue(values);
        Node node= new Node();
        List<Key> keys= new ArrayList<>();
        keys.add(key1);
        node.setKeys(keys);
        node.setIsleaf(true);
        node.setBlockPointer(this.rootBlock);
        List<Node> nodes= new ArrayList<>();
        nodes.add(node);
        rootBlock.setListOfNodes(nodes);
        this.listBlocks.add(rootBlock);
        }
        //Case2: Only one node
        else if ((this.rootBlock.getListOfNodes().get(0).isIsleaf() == true) && (this.rootBlock.getListOfNodes().get(0).getKeys().size() <orderOfTree)) {

            insertionNodeKey(this.rootBlock.getListOfNodes().get(0), key, value);

        }

        //Case3: Normal Insert
    }





    private void insertionNodeKey(Node node, int key, int value) {

        boolean localEqualityCheck = false;
        List<Key> keys = node.getKeys();

        loop1:
        for (int i = 0; i < keys.size(); i++) {
            if (key == keys.get(i).getKey()) {
                keys.get(i).getValue().add(value);
                localEqualityCheck = true;
                break loop1;
            }

        }
        if (localEqualityCheck == false) {
            Key key1 = new Key();
            List<Integer> values = new ArrayList<Integer>();
            key1.setKey(key);
            values.add(value);
            key1.setValue(values);
            keys.add(key1);
            node.setKeys(keys);
            //collectionSort
            Collections.sort(keys, new Comparator<Key>() {
                @Override
                public int compare(Key s1, Key s2) {
                    return Integer.compare(s1.getKey(), s2.getKey());
                }
            });


            //  System.out.println(node.isIsleaf());
        }
    }
}
