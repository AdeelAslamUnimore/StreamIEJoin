package com.baselinealgorithm.chainbplusandcss;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Made by Adeel Aslam and Giovanni Simonini
 * Cache Sensitive Search Tree
 * <p>
 * Cache sensitive search tree is an idea inspired by the paper "Making B+-Trees Cache Conscious in Main Memory."
 * It is an extension of the BPlus Tree where three dimensions exist: Blocks, Nodes, and Keys.
 * Nodes are grouped into blocks. The tree has an order that indicates the maximum size of keys inside a node.
 * <p>
 * The Cache Sensitive Search Tree provides the following features:
 * - Efficient memory access by organizing nodes into blocks.
 * - Support for range queries through getNext() and getPrev() methods in leaf nodes.
 * - Simple search using linear scan to find exact matches.
 * <p>
 * For more advanced search techniques, refer to the original paper. This documentation focuses on the simplest approach.
 */
public class CSSTree1 {
    /**
     * This is the initialization of the tree.
     * <p>
     * Steps for initialization:
     * 1. Initialize the root Block.
     * 2. Create a list that contains all blocks. The index of each block in the list helps in finding the relevant blocks and building the tree.
     * 3. Specify the order of the tree, which represents the maximum number of keys in a node.
     */
    private Block rootBlock;
    private List<Block> listBlocks;
    int orderOfTree;

    /**
     * This is the constructor of the CSSTree.
     *
     * @param orderOfTree An integer representing the order of the tree. It must be greater than or equal to 3 for ease of implementation.
     *                    <p>
     *                    Constructor steps:
     *                    1. Validate and set the order of the tree.
     *                    2. Perform initialization tasks.
     */
    public CSSTree1(int orderOfTree) {
        this.rootBlock = null;
        this.listBlocks = new ArrayList<Block>();
        this.orderOfTree = orderOfTree;
    }

    /**
     * This method is responsible for inserting keys into the tree and constructing it.
     * <p>
     * Insertion steps:
     * 1. Find the appropriate leaf node to insert the key.
     * 2. If the leaf node is not full, insert the key directly.
     * 3. If the leaf node is full, split the node and redistribute the keys.
     * 4. If necessary, split the blocks to accommodate the new nodes.
     *
     * @param key   The object representing the key to be inserted.
     * @param value The value associated with the key. In this implementation, it is a list of values since a key can contain multiple values for various runs.
     */
    public void insert(int key, int value) {
        /**
         * This condition checks if the root block is null and initializes it along with other necessary components.
         *
         * Initialization steps:
         * 1. Create a new root block.
         * 2. Add the root block to the list of blocks.
         * 3. Create a new key object and set its properties.
         * 4. Create a list of values and add the initial value.
         * 5. Set the values list as the value of the key.
         * 6. Create a list of keys and add the key to it.
         * 7. Create a new node and set its properties.
         * 8. Set the list of keys in the node.
         * 9. Set the node as a leaf node.
         * 10. Set the root block as the block pointer of the node.
         * 11. Create a list of nodes and add the node to it.
         * 12. Set the list of nodes in the root block.
         *
         * Note: Uncomment and modify the required lines of code based on the specific implementation details.
         */
        if (this.rootBlock == null) {
            this.rootBlock = new Block();
            this.listBlocks.add(this.rootBlock);
            Key key1 = new Key(); //Key object
            List<Integer> values = new ArrayList<Integer>(); // Set of Values
            key1.setKey(key);
            key1.setId(value);
            values.add(value);
            key1.setValue(values);
            List<Key> keys = new ArrayList<Key>();
            keys.add(key1);
            Node node = new Node();
            node.setKeys(keys);
            node.setIsleaf(true);
            node.setBlockPointer(this.rootBlock);
            //node.setNodeID(1);
            List<Node> nodeList = new ArrayList<Node>();
            nodeList.add(node);
            this.rootBlock.setListOfNodes(nodeList);

            //node.setCurrentBlockIndexInArrayList(this.listBlocks.indexOf(this.rootBlock)); //this.listBlocks.indexOf(this.rootBlock)

        }
        /**
         * This condition checks if the root block exists and the root node is a leaf node with available space for insertion.
         * If the condition is true, it performs the key insertion into the root node.
         *
         * Key insertion steps:
         * 1. Retrieve the root node from the root block.
         * 2. Call the insertionNodeKey() method to insert the key into the root node.
         *
         * Note: Uncomment and modify the required lines of code based on the specific implementation details.
         */

        else if ((this.rootBlock.getListOfNodes().get(0).isIsleaf() == true) && (this.rootBlock.getListOfNodes().get(0).getKeys().size() < orderOfTree)) {
            insertionNodeKey(this.rootBlock.getListOfNodes().get(0), key, value);

        }
        /**
         * This else block handles the case when none of the previous conditions are met.
         * It performs the key insertion into the appropriate node of the tree.
         *
         * Key insertion steps:
         * 1. Set the currentNode as the first node of the root block.
         * 2. If the currentNode is not a leaf node, update it to the node based on the block pointer.
         * 3. Find the relevant node for key insertion.
         * 4. If the relevant node is null, insert the key into the currentNode.
         * 5. If the relevant node is not null, insert the key into the relevant node and update the currentNode.
         * 6. Check if the currentNode exceeds the maximum number of keys allowed.
         * 7. If it exceeds, perform the split operation on the currentNode.
         *
         * Note: Uncomment and modify the required lines of code based on the specific implementation details.
         */


        else {

            Node currentNode = this.rootBlock.getListOfNodes().get(0);

            if (!(currentNode.isIsleaf())) {

                currentNode = nodeWithBlockPointer(currentNode.getBlockPointer(), key);

            }

            Node relevenetNode = findRelevenetNode(currentNode, key);
            if (relevenetNode == null) {
                insertionNodeKey(currentNode, key, value);
            } else {
                insertionNodeKey(relevenetNode, key, value);
                currentNode = relevenetNode;
            }


            if (currentNode.getKeys().size() > orderOfTree) {
                split(currentNode);

            }

        }

    }

    private void insertionNodeKey(Node node, int key, int value) {

        boolean localEqualityCheck = false;
        List<Key> keys = node.getKeys();
        /*
        Loop that checks the new key already exists
        Update only the associated value list with the value of the key
        Switch on the loop to avoid next scaning
         */
        loop1:
        for (int i = 0; i < keys.size(); i++) {
            if (key == keys.get(i).getKey()) {
                keys.get(i).getValue().add(value);
                //  System.out.println(key);
                localEqualityCheck = true;
                break loop1;
            }

        }
        if (localEqualityCheck == false) {
            Key key1 = new Key();
            List<Integer> values = new ArrayList<Integer>();
            key1.setKey(key);
            key1.setId(value);
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

    public Node nodeWithBlockPointer(Block block, int key) {

        Node node = searchNearestKey(block.getListOfNodes(), key);

        try {

            while (!node.isIsleaf()) {
                Block blockToSearch = listBlocks.get(node.getChildBlockIndex());

                //  nodes= blockToSearch.getListOfNodes();
                if (blockToSearch == null) {
                    break;
                }
                //  System.out.println(blockToSearch.getListOfNodes().size()+".....list");
                node = searchNearestKey(blockToSearch.getListOfNodes(), key);
            }

//           System.out.println(nodes.getNext().getKeys());
//         System.out.println(nodes.getNext().getNext().getKeys());

        } catch (NullPointerException e) {
            System.out.println(node);
            //  System.exit(-1);
        }
        return node;
    }

    public void split(Node currentNode) {
        int midIndex = currentNode.getKeys().size() / 2;
        int middleKey = currentNode.getKeys().get(midIndex).getKey();
        Node splitNode = new Node();
        List<Key> rightKeys = new ArrayList<>(currentNode.getKeys().subList(midIndex + 1, currentNode.getKeys().size()));
        splitNode.setKeys(rightKeys);
        splitNode.setBlockPointer(currentNode.getBlockPointer());
        splitNode.setIsleaf(currentNode.isIsleaf());
        Block block = currentNode.getBlockPointer();
        int index = block.getListOfNodes().indexOf(currentNode);
        currentNode.getBlockPointer().getListOfNodes().add((index + 1), splitNode);
        currentNode.getKeys().subList(midIndex + 1, currentNode.getKeys().size()).clear();
        Node nodeNext = currentNode.getNext();
        if (nodeNext != null) {
            //  System.out.println("I am here");
            currentNode.getNext().setPrev(splitNode);
            splitNode.setNext(nodeNext);
        }
        currentNode.setNext(splitNode);
        splitNode.setPrev(currentNode);
//    System.out.println(currentNode.getNext()+"...CurrentNodeNext");
//    System.out.println(splitNode.getPrev()+"...SplitNodeNPt");
//    System.out.println(splitNode.getNext()+"...Preciow");
        insertKeyIntoIntermediate(currentNode.getBlockPointer(), middleKey);
        // insertKeyIntoIntermediate(currentNode.getBlockPointer(),  middleKey,splitNode);
    }

    // public void insertKeyIntoIntermediate(Block block, int key, Node splitNode){
    public void insertKeyIntoIntermediate(Block block, int key) {

        if (block.getParentNode() == null) {
            Block parentBlock = new Block();
            listBlocks.add(parentBlock);
            List<Node> nodeList = new ArrayList<>();
            List<Key> keyList = new ArrayList<>();
            Node node = new Node();
            Key key1 = new Key();
            key1.setKey(key);
            keyList.add(key1);
            node.setKeys(keyList);
            node.setBlockPointer(parentBlock);
            node.setIsleaf(false);
            node.setChildBlockIndex(listBlocks.indexOf(block));
            //  node.setCurrentBlockIndexInArrayList(listBlocks.indexOf(parentBlock));
            nodeList.add(node);
            parentBlock.setListOfNodes(nodeList);
            block.setParentNode(node);
            this.rootBlock = parentBlock;
        } else {

            Key key1 = new Key();
            //// Checks for contains   .....................
            boolean keysExistance = false;
            label1:
            for (Key keyToSearch : block.getParentNode().getKeys()) {
                if (keyToSearch.getKey() == key) {
                    keysExistance = true;
                    //   System.out.println(keyToSearch.getKey()+"Common"+key);
                    break label1;
                }
            }

            if (keysExistance == false) {
                //  System.out.println("Data");
                key1.setKey(key);
                block.getParentNode().getKeys().add(key1);
                Collections.sort(block.getParentNode().getKeys(), new Comparator<Key>() {
                    @Override
                    public int compare(Key s1, Key s2) {
                        return Integer.compare(s1.getKey(), s2.getKey());
                    }
                });
                if (block.getParentNode().getKeys().size() > orderOfTree) {
                    int midIndex = block.getParentNode().getKeys().size() / 2;
                    int middleKey = block.getParentNode().getKeys().get(midIndex).getKey();
                    Node splitIntermediateNode = new Node();
                    List<Key> rightKeys = new ArrayList(block.getParentNode().getKeys().subList(midIndex + 1, block.getParentNode().getKeys().size()));
                    splitIntermediateNode.setKeys(rightKeys);
                    splitIntermediateNode.setBlockPointer(block.getParentNode().getBlockPointer());
                    splitIntermediateNode.setIsleaf(block.getParentNode().isIsleaf());
                    // splitIntermediateNode.setCurrentBlockIndexInArrayList(block.getParentNode().getCurrentBlockIndexInArrayList());


                    Block blockForIndex = block.getParentNode().getBlockPointer();
                    int index = blockForIndex.getListOfNodes().indexOf(block.getParentNode());
                    block.getParentNode().getBlockPointer().getListOfNodes().add((index + 1), splitIntermediateNode);
                    // block.getParentNode().getBlockPointer().getListOfNodes().add(splitIntermediateNode);
                    //  int childBlockIndexForSplitIntermediateNodeInBlockList = childBlockSplit(block, splitNode,splitIntermediateNode);
                    int childBlockIndexForSplitIntermediateNodeInBlockList = childBlockSplit(block, splitIntermediateNode);
                    splitIntermediateNode.setChildBlockIndex(childBlockIndexForSplitIntermediateNodeInBlockList);
                    block.getParentNode().getKeys().subList(midIndex, block.getParentNode().getKeys().size()).clear();

                    insertKeyIntoIntermediate(block.getParentNode().getBlockPointer(), middleKey);
                    //  insertKeyIntoIntermediate(block.getParentNode().getBlockPointer(), middleKey, splitIntermediateNode);
                }
            }
        }
    }

    // public int childBlockSplit(Block block, Node splitNode, Node parentNode){
    public int childBlockSplit(Block block, Node parentNode) {
        int key = parentNode.getKeys().get(0).getKey();
        Node node = searchNearestKey(block.getListOfNodes(), key);
        int childBlockIndexForEmergencyNode = 0;
        if (!(node.isIsleaf())) {
            childBlockIndexForEmergencyNode = node.getChildBlockIndex();
        }
        Block newBlock = new Block();
        listBlocks.add(newBlock);
        int startIndex = block.getListOfNodes().indexOf(node);
        List<Node> rightNodes = new ArrayList(block.getListOfNodes().subList(startIndex, block.getListOfNodes().size()));
        //List<Node> rightNodes = new ArrayList(block.getListOfNodes().subList(block.getListOfNodes().size()-1, block.getListOfNodes().size()));
        newBlock.setListOfNodes(rightNodes);
        block.getListOfNodes().subList(startIndex, block.getListOfNodes().size()).clear();
        if (block.getListOfNodes().size() == 0) {
            Node emergencyNode = new Node();
            emergencyNode.setIsleaf(newBlock.getListOfNodes().get(0).isIsleaf());
            emergencyNode.setBlockPointer(block);

            List<Key> keys = new ArrayList<>();
            emergencyNode.setKeys(keys);
            // System.out.println(emergencyNode.isIsleaf());
            if (!(node.isIsleaf())) {
                emergencyNode.setChildBlockIndex(childBlockIndexForEmergencyNode);
            }
            //   emergencyNode.setCurrentBlockIndexInArrayList(listBlocks.indexOf(block));
            ///Addd data here

            block.getListOfNodes().add(emergencyNode);
            if (newBlock.getListOfNodes().get(0).isIsleaf()) {
                // System.out.println("Grrrrrrrrrr");
                emergencyNode.setPrev(newBlock.getListOfNodes().get(0).getPrev());
                emergencyNode.setNext(newBlock.getListOfNodes().get(0));
                if (newBlock.getListOfNodes().get(0).getPrev() != null)
                    newBlock.getListOfNodes().get(0).getPrev().setPrev(emergencyNode);

            }
        }

        for (int i = 0; i < newBlock.getListOfNodes().size(); i++) {
            //  newBlock.getListOfNodes().get(i).setCurrentBlockIndexInArrayList(listBlocks.indexOf(newBlock));
            newBlock.getListOfNodes().get(i).setBlockPointer(newBlock);
        }
        newBlock.setParentNode(parentNode);

        return listBlocks.indexOf(newBlock);


    }

    public void searchGreater(int key) {
        //BitMatrixSpareseBit costHS;

        Node node = nodeWithBlockPointer(this.rootBlock, key);

        Node relevenetNode = findRelevenetNode(node, key);
        if (relevenetNode == null) {
            node = node;
        } else {

            node = relevenetNode;
        }

        //  System.out.println(",,,," + node);

        //  node=node.getPrev();
        while (node != null) {
            System.out.println(node.getKeys() + ".....");
            node = node.getNext();
            // System.out.println(node.getPrev().getKeys());

        }
    }

    public void searchSmaller(int key) {
        //BitMatrixSpareseBit costHS;

        Node node = nodeWithBlockPointer(this.rootBlock, key);

        Node relevenetNode = findRelevenetNode(node, key);
        if (relevenetNode == null) {
            node = node;
        } else {

            node = relevenetNode;
        }
        while (node != null) {
            System.out.println(node.getKeys() + ".....");
            node = node.getPrev();
            // System.out.println(node.getPrev().getKeys());

        }
    }

    public Node searchNearestKey(List<Node> nodes, int key) {
        if (nodes == null || nodes.isEmpty()) {
            System.out.println("This case");
            return null;
        }
        if (nodes.get(0).getKeys().size() == 0) {
            return nodes.get(0);
        }

        for (int i = 0; i < nodes.size(); i++) {
            int lastIndex = nodes.get(i).getKeys().size() - 1;
            //  System.out.println("Size"+nodes.get(i).getKeys().size()+"...."+key);
            if (nodes.get(i).getKeys().get(lastIndex).getKey() >= key) {
                return nodes.get(i);
//                node=nodes.get(i);
////                break;
            }

        }
        return nodes.get(nodes.size() - 1);
    }

    public void debug(Block rootBlock) {
        System.out.println(rootBlock + "RootBlock");
        Block blockRoot = listBlocks.get(rootBlock.getListOfNodes().get(0).getChildBlockIndex());
        try {
            System.out.println(blockRoot + "ist");
            System.out.println(listBlocks.get(blockRoot.getListOfNodes().get(1).getChildBlockIndex()) + "second");
//            Block thirdLevel = listBlocks.get(blockRoot.getListOfNodes().get(1).getChildBlockIndex());
//            System.out.println(listBlocks.get(thirdLevel.getListOfNodes().get(1).getChildBlockIndex()) + "Bottom");
        } catch (IndexOutOfBoundsException e) {

        }
    }

    public Node findRelevenetNode(Node node, int key) {
        Node node1 = node;
        label1:
        while (node1 != null) {
            // binary search
            for (Key key1 : node1.getKeys()) {
                if (key1.getKey() <= key) {
                    //  node = node;
                    break label1;
                }
            }

            node1 = node1.getPrev();

        }
        return node1;
    }


}