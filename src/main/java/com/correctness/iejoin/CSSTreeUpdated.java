package com.correctness.iejoin;

import com.baselinealgorithm.chainbplusandcss.Block;
import com.baselinealgorithm.chainbplusandcss.Key;
import com.baselinealgorithm.chainbplusandcss.Node;

import java.util.*;

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
public class CSSTreeUpdated {
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



    /// tuple removal counter
    private int tupleRemovalCounter=0;
    /**
     * This is the constructor of the CSSTree.
     *
     * @param orderOfTree An integer representing the order of the tree. It must be greater than or equal to 3 for ease of implementation.
     *                    <p>
     *                    Constructor steps:
     *                    1. Validate and set the order of the tree.
     *                    2. Perform initialization tasks.
     */
    public CSSTreeUpdated(int orderOfTree) {
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

            Node relevenetNode = findRelevantNode(currentNode, key);
            if (relevenetNode == null) {
                insertionNodeKey(currentNode, key, value);
            } else {
                insertionNodeKey(relevenetNode, key, value);
                currentNode = relevenetNode;
            }


            if (currentNode.getKeys().size() >=orderOfTree) {
                split(currentNode);

            }
            Node currentNode1 = this.rootBlock.getListOfNodes().get(0);
            System.out.println(rootBlock+"========RootBlcok");
            System.out.println("Child Block"+listBlocks.get(this.rootBlock.getListOfNodes().get(0).getChildBlockIndex()));
            Block block=listBlocks.get(this.rootBlock.getListOfNodes().get(0).getChildBlockIndex());
            System.out.println(listBlocks.get(block.getListOfNodes().get(0).getChildBlockIndex()));
            System.out.println(listBlocks.get(block.getListOfNodes().get(1).getChildBlockIndex()));
            currentNode1 = nodeWithBlockPointer(currentNode1.getBlockPointer(), key);
            System.out.println(currentNode1);

        }

    }

    /*
        Insertion of key to the node
        Includes equality or non-equality case
     */
    private void insertionNodeKey(Node node, int key, int value) {

        boolean localEqualityCheck = false;
        List<Key> keys = node.getKeys();
        /*
        Loop that checks the new key already exists
        Update only the associated value list with the value of the key
        Switch on the loop to avoid next scaning
         */
        for (Key existingKey : keys) {
            if (key == existingKey.getKey()) {
                existingKey.getValue().add(value);
                return;
            }
        }
        /**
         * This section of code creates a new Key object, sets its properties, adds it to the list of keys in the node,
         * and sorts the keys in ascending order based on their key values.
         *
         * Key creation and sorting steps:
         * 1. Create a new Key object.
         * 2. Create a list to store the values associated with the key.
         * 3. Set the key value and identification in the Key object.
         * 4. Add the value to the values list.
         * 5. Set the values list in the Key object.
         * 6. Add the Key object to the list of keys in the node.
         * 7. Sort the keys in ascending order based on their key values using the Collections.sort() method with a custom Comparator.
         *
         * Note: Uncomment and modify the required lines of code based on the specific implementation details.
         */

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
    }

    /**
     * This method retrieves the node with a specific block pointer from the given block and key value.
     * It performs a search to find the node that contains the nearest key to the specified key value.
     * <p>
     * Node retrieval steps:
     * 1. Initialize the node variable with the result of searching the nearest key in the given block.
     * 2. Iterate through the nodes until a leaf node is reached.
     * 3. Get the child block index from the current node and retrieve the corresponding block.
     * 4. Search for the nearest key in the child block and update the node variable.
     * 5. Repeat steps 3-4 until a leaf node is reached.
     * 6. Return the final node.
     * <p>
     * Note: Uncomment and modify the required lines of code based on the specific implementation details.
     * Handle any potential NullPointerExceptions according to the desired error-handling strategy.
     * Consider providing additional information or returning null in case of a NullPointerException.
     *
     * @param block The block from which to retrieve the node.
     * @param key   The key value used to search for the nearest key.
     * @return The node with the specified block pointer.
     */

    public Node nodeWithBlockPointer(Block block, int key) {

        Node node = searchNearestKey(block.getListOfNodes(), key);

        while (!node.isIsleaf()) {
            Block blockToSearch = listBlocks.get(node.getChildBlockIndex());
            if (blockToSearch == null) {
                break;
            }
            node = searchNearestKey(blockToSearch.getListOfNodes(), key);
        }

        return node;
    }

    /**
     * This method splits the given currentNode in the CSSTree.
     * It performs a split operation to divide the keys and create a new node.
     * <p>
     * Split operation steps:
     * 1. Determine the middle index of the keys in the currentNode.
     * 2. Retrieve the middle key value from the keys list.
     * 3. Create a new splitNode and populate it with the keys from the right half of the currentNode's keys list.
     * 4. Set the block pointer, leaf status, and connections for the splitNode.
     * 5. Get the block and index of the currentNode in the block's list of nodes.
     * 6. Insert the splitNode at the appropriate position in the block's list of nodes.
     * 7. Remove the keys from the currentNode's keys list that were moved to the splitNode.
     * 8. Update the prev, next, and connections of the currentNode and splitNode.
     * 9. Insert the middle key into the intermediate node (parent) of the currentNode.
     * <p>
     * Note: Uncomment and modify the required lines of code based on the specific implementation details.
     *
     * @param currentNode The node to be split.
     */
    public void split(Node currentNode) {
        int midIndex = currentNode.getKeys().size() / 2;
        int middleKey = currentNode.getKeys().get(midIndex).getKey();
        Node splitNode = new Node();
        List<Key> rightKeys = new ArrayList<>(currentNode.getKeys().subList(midIndex, currentNode.getKeys().size()));
        splitNode.setKeys(rightKeys);
        splitNode.setBlockPointer(currentNode.getBlockPointer());
        splitNode.setIsleaf(currentNode.isIsleaf());
        Block block = currentNode.getBlockPointer();
        int index = block.getListOfNodes().indexOf(currentNode);
        currentNode.getBlockPointer().getListOfNodes().add((index +1), splitNode);
        currentNode.getKeys().subList(midIndex , currentNode.getKeys().size()).clear();
        Node nodeNext = currentNode.getNext();
        if (nodeNext != null) {
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

    /**
     * This method inserts a key into the intermediate node of the CSSTree's block.
     * It checks if the block has a parent node. If not, it creates a new parent node and sets it as the root block.
     * If a parent node exists, it searches for the key in the parent node's keys. If the key is already present, the method returns.
     * If the key is not found, it adds the key to the parent node's keys, sorts them, and checks if the parent node exceeds the order of the tree.
     * If the parent node exceeds the order of the tree, it performs a split operation to create a new intermediate node.
     *
     * Insert key into intermediate node steps:
     * 1. Check if the block has a parent node.
     *    - If not, create a new parent block and set it as the root block.
     * 2. Search for the key in the parent node's keys.
     *    - If the key is found, return.
     * 3. Create a new key object and set its value.
     * 4. Add the key to the parent node's keys list.
     * 5. Sort the keys list in ascending order based on the key values.
     * 6. Check if the parent node exceeds the order of the tree.
     *    - If yes, perform a split operation to create a new intermediate node.
     *      - Determine the middle index of the keys in the parent node.
     *      - Retrieve the middle key value.
     *      - Create a new splitIntermediateNode and populate it with the keys from the right half of the parent node's keys list.
     *      - Set the block pointer, leaf status, and connections for the splitIntermediateNode.
     *      - Insert the splitIntermediateNode at the appropriate position in the block's list of nodes.
     *      - Update the child block index for the splitIntermediateNode.
     *      - Remove the keys from the parent node's keys list that were moved to the splitIntermediateNode.
     *      - Recursively insert the middle key into the intermediate node.
     *
     * Note: Uncomment and modify the required lines of code based on the specific implementation details.
     *
     * @param block The block where the key is to be inserted into the intermediate node.
     * @param key The key value to be inserted.
     */

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

            nodeList.add(node);
            parentBlock.setListOfNodes(nodeList);
            block.setParentNode(node);
            this.rootBlock = parentBlock;
        } else {
            Key key1 = new Key();
            for (Key keyToSearch : block.getParentNode().getKeys()) {
                if (keyToSearch.getKey() == key) {
                    return;
                }
            }
                key1.setKey(key);
                block.getParentNode().getKeys().add(key1);
                Collections.sort(block.getParentNode().getKeys(), new Comparator<Key>() {
                    @Override
                    public int compare(Key s1, Key s2) {
                        return Integer.compare(s1.getKey(), s2.getKey());
                    }
                });
                if (block.getParentNode().getKeys().size() >= orderOfTree) {
                    int midIndex = block.getParentNode().getKeys().size() / 2;
                    int middleKey = block.getParentNode().getKeys().get(midIndex).getKey();
                    Node splitIntermediateNode = new Node();
                    List<Key> rightKeys = new ArrayList(block.getParentNode().getKeys().subList(midIndex + 1, block.getParentNode().getKeys().size()));
                    splitIntermediateNode.setKeys(rightKeys);
                    splitIntermediateNode.setBlockPointer(block.getParentNode().getBlockPointer());
                    splitIntermediateNode.setIsleaf(block.getParentNode().isIsleaf());
                    Block blockForIndex = block.getParentNode().getBlockPointer();
                    int index = blockForIndex.getListOfNodes().indexOf(block.getParentNode());
                    block.getParentNode().getBlockPointer().getListOfNodes().add((index + 1), splitIntermediateNode);
                    int childBlockIndexForSplitIntermediateNodeInBlockList = childBlockSplit(block, splitIntermediateNode);
                    splitIntermediateNode.setChildBlockIndex(childBlockIndexForSplitIntermediateNodeInBlockList);
                    block.getParentNode().getKeys().subList(midIndex, block.getParentNode().getKeys().size()).clear();
                    insertKeyIntoIntermediate(block.getParentNode().getBlockPointer(), middleKey);
                }
            }
    }
    /**
     * This method performs a child block split operation when the parent node of a block exceeds the order of the tree.
     * It splits the block's list of nodes into two parts, creates a new block, and assigns the right portion of nodes to the new block.
     * It also updates the necessary connections and returns the index of the new block in the listBlocks.
     *
     * Child block split operation steps:
     * 1. Get the key value from the first key in the parent node.
     * 2. Search for the nearest key in the block's list of nodes.
     * 3. If the nearest key is not a leaf node, store the child block index of that node for emergency node creation.
     * 4. Create a new block and add it to the listBlocks.
     * 5. Determine the start index of the right nodes in the block's list of nodes.
     * 6. Create a list of right nodes by taking a sublist from the start index to the end of the list.
     * 7. Set the list of right nodes for the new block.
     * 8. Remove the right nodes from the block's list of nodes by clearing the sublist.
     * 9. If the block's list of nodes becomes empty:
     *     - Create an emergency node and set its leaf status based on the first node in the new block's list of nodes.
     *     - Set the block pointer as the original block.
     *     - Create an empty list of keys for the emergency node.
     *     - If the nearest key is not a leaf node, set the child block index for the emergency node.
     *     - Add the emergency node to the block's list of nodes.
     *     - If the first node in the new block's list of nodes is a leaf node:
     *         - Update the previous and next pointers of the emergency node.
     *         - Update the previous node's next pointer to the emergency node.
     * 10. Update the block pointers of the nodes in the new block to point to the new block.
     * 11. Set the parent node of the new block as the provided parent node.
     * 12. Return the index of the new block in the listBlocks.
     *
     * Note: Uncomment and modify the required lines of code based on the specific implementation details.
     *
     * @param block The block to be split.
     * @param parentNode The parent node of the block.
     * @return The index of the new block in the listBlocks.
     */
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

    public HashSet<Integer> searchGreater(int key) {
        //BitMatrixSpareseBit costHS;
        HashSet<Integer> hashSetForAllIdsGreaterThanAKey= new HashSet<>();
        Node node = nodeWithBlockPointer(this.rootBlock, key);
        Node referentNode = findRelevantNode(node, key);
        node = (referentNode == null) ? node : referentNode;
        for(Key keys:node.getKeys()){
            if(keys.getKey()>key){
                hashSetForAllIdsGreaterThanAKey.addAll(keys.getValue());
            }
        }
        while (node != null) {
            node = node.getNext();
            if(node!=null){
                for(Key keys:node.getKeys()){
                    hashSetForAllIdsGreaterThanAKey.addAll(keys.getValue());
                }
            }
        }
        return hashSetForAllIdsGreaterThanAKey;
    }

    public HashSet<Integer> searchSmaller(int key) {
        HashSet<Integer> smallestHashSetIds= new HashSet<>();
        Node node = nodeWithBlockPointer(this.rootBlock, key);
        Node referentNode = findRelevantNode(node, key);
        node = (referentNode == null) ? node : referentNode;
        for(Key keys:node.getKeys()){
            if(keys.getKey()<key){
                smallestHashSetIds.addAll(keys.getValue());
            }
        }
        while (node != null) {
            node = node.getPrev();
            if(node!=null){
                for(Key keys:node.getKeys()){
                        smallestHashSetIds.addAll(keys.getValue());
                }
            }
        }
        return smallestHashSetIds;
    }
    /**
     * This method searches for the nearest key in a list of nodes based on a given key.
     * It iterates through the nodes and compares the last key in each node's list of keys with the given key.
     * It returns the node whose last key is greater than or equal to the given key, or the last node in the list if no such node is found.
     *
     * Nearest key search steps:
     * 1. If the list of nodes is null or empty, return null.
     * 2. If the first node's list of keys is empty, return the first node.
     * 3. Iterate through the list of nodes:
     *     - Get the index of the last key in the current node's list of keys.
     *     - If the last key is greater than or equal to the given key, return the current node.
     * 4. If no node is found in the iteration, return the last node in the list.
     *
     * Note: Uncomment and modify the required lines of code based on the specific implementation details.
     *
     * @param nodes The list of nodes to search.
     * @param key The key value to compare.
     * @return The node with the nearest key greater than or equal to the given key, or the last node if no such node is found.
     */

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
    /**
     * This method finds the relevant node in a linked list of nodes based on a given key.
     * Starting from the provided node, it traverses the linked list backward and performs a binary search within each node's list of keys.
     * It returns the first node whose keys are less than or equal to the given key, or null if no such node is found.
     *
     * Relevant node search steps:
     * 1. Create a local variable "node1" and assign the provided node to it.
     * 2. Use a labeled loop to iterate while "node1" is not null.
     *     - Perform a binary search within the current node's list of keys.
     *     - If a key is found that is less than or equal to the given key, break the labeled loop.
     *     - Otherwise, move to the previous node by updating "node1" to its previous node.
     * 3. Return the value of "node1".
     *
     * Note: Uncomment and modify the required lines of code based on the specific implementation details.
     *
     * @param node The starting node for the search.
     * @param key The key value to compare.
     * @return The first node whose keys are less than or equal to the given key, or null if no such node is found.
     */

    public Node findRelevantNode(Node node, int key) {
        Node node1 = node;
       boolean check=false;
        while (node1 != null) {
            // binary search
            label2: for (int i=0;i< node1.getKeys().size();i++) {
                if(node1.getKeys().get(0).getKey()>key){
                    break ;
                }
                else{
//                    System.out.println("Here"+key);
                    check=true;
                    break ;
                }
//                if (node1.getKeys().get(i).getKey() <= key) {
//                    //  node = node;
//                    check=true;
//                    break ;
//                }
            }
            if(check){
                break;
            }
            node1 = node1.getPrev();

        }
        return node1;
    }

    public void insertBulkUpdate(int key, List<Integer> value) {
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
           // key1.setId(value);
            values.addAll(value);
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
            insertionNodeKeyMerge(this.rootBlock.getListOfNodes().get(0), key, value);

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

            Node relevenetNode = findRelevantNode(currentNode, key);
            if (relevenetNode == null) {
                insertionNodeKeyMerge(currentNode, key, value);
            } else {
                insertionNodeKeyMerge(relevenetNode, key, value);
                currentNode = relevenetNode;
            }


            if (currentNode.getKeys().size() > orderOfTree) {
                split(currentNode);

            }

        }

    }
    /**
     * This section of code creates a new Key object, sets its properties, adds it to the list of keys in the node,
     * and sorts the keys in ascending order based on their key values.
     *
     * Key creation and sorting steps:
     * 1. Create a new Key object.
     * 2. Create a list to store the values associated with the key.
     * 3. Set the key value and identification in the Key object.
     * 4. Add the value to the values list.
     * 5. Set the values list in the Key object.
     * 6. Add the Key object to the list of keys in the node.
     * 7. Sort the keys in ascending order based on their key values using the Collections.sort() method with a custom Comparator.
     *
     * Note: Uncomment and modify the required lines of code based on the specific implementation details.
     */

    private void insertionNodeKeyMerge(Node node, int key, List<Integer> value) {

        boolean localEqualityCheck = false;
        List<Key> keys = node.getKeys();
        /*
        Loop that checks the new key already exists
        Update only the associated value list with the value of the key
        Switch on the loop to avoid next scaning
         */
        for (Key existingKey : keys) {
            if (key == existingKey.getKey()) {
                existingKey.getValue().addAll(value);
                return;
            }
        }


        Key key1 = new Key();
        List<Integer> values = new ArrayList<Integer>();
        key1.setKey(key);

        values.addAll(value);
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



    }


    public int getTupleRemovalCounter() {
        return tupleRemovalCounter;
    }

    public void setTupleRemovalCounter(int tupleRemovalCounter) {
        this.tupleRemovalCounter = tupleRemovalCounter;
    }
}