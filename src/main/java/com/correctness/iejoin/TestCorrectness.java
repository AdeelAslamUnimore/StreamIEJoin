package com.correctness.iejoin;

import com.stormiequality.BTree.BPlusTree;
import com.stormiequality.BTree.Key;
import com.stormiequality.BTree.Node;
import com.stormiequality.BTree.Offset;
import com.stormiequality.join.Permutation;
import com.testcomparatorclasses.ieJoin.Cost;
import com.testcomparatorclasses.ieJoin.Duration;
import com.testcomparatorclasses.ieJoin.Rev;
import com.testcomparatorclasses.ieJoin.Time;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;

import java.sql.*;
import java.util.*;

public class TestCorrectness {
    static BPlusTree durationBPlusTree = null;
    private BPlusTree revenueBplusTree = null;
    private BPlusTree timeBPlusTree = null;
    private BPlusTree costBPlusTree = null;
    static BitSet bitSet = null;
    private Connection conn;
    private int[] permutationArrayEast = null;
    private int[] permutationArrayWest = null;
    private int[] offsetArrayOfDurationWithRespectToTime;
    private int[] offsetArrayOfRevenueWithRespectToCost;

    public static void main(String[] args) throws Exception {

        try {
            Class.forName("com.mysql.cj.jdbc.Driver").newInstance();

        } catch (Exception ex) {

        }


//
//        new TestCorrectness().insertEast(100000);
//        new TestCorrectness().insertWest(100000);

    //    new TestCorrectness().test();
        new TestCorrectness().permutationAndComputationArray();


    }

    private Connection testConnection() throws Exception {
        conn = DriverManager.getConnection("jdbc:mysql://131.175.204.209/transaction_schema?" +
                "user=root&password=5EK^g?`<D4w2P^(Z");
        BPlusTree bPlusTree = new BPlusTree(4);
        //  bPlusTree.initialize(4);
        String query = "select * from east";
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(query);
        int count = 0;
        while (rs.next()) {
            count++;
            int duration = rs.getInt("duration");
            int id = rs.getInt("id");

            bPlusTree.insert(duration, id);
        }
        BitSet bitSet1 = bPlusTree.greaterThenSpecificValue(500);
        BitSet bitSet2 = bPlusTree.lessThenSpecificValue(750);
        bitSet1.and(bitSet2);
//        System.out.println("HEre" + count);
//        System.out.println(bitSet1);


        // Statement stmt = null;
//        ResultSet rs = null;
        //  stmt = conn.createStatement();
//        int id_East=400;
//        int id=0;
//        int duration=0;
//        int revenue=0;
//        int core=0;
//        Random random= new Random();
//        PreparedStatement preparedStatement= null;
//        String insertQuery="INSERT INTO east (id_East,duration,revenue,core,id) values (?,?,?,?,?)";
//        for(int i=0;i<1000;i++){
//            id_East=id_East+1;
//            id=id+1;
//            duration= random.nextInt(2000);
//            revenue=random.nextInt(1000);
//            core=random.nextInt(1500);
//            preparedStatement=conn.prepareStatement(insertQuery);
//            preparedStatement.setInt(1,id_East);
//            preparedStatement.setInt(2,duration);
//            preparedStatement.setInt(3,revenue);
//            preparedStatement.setInt(4,core);
//            preparedStatement.setInt(5,id);
//            preparedStatement.executeUpdate();
//        }
////        rs = stmt.executeQuery("SELECT * FROM east");
////        while (rs.next()){
////            System.out.println(rs.getInt("duration"));
////        }
////        System.out.println(rs+".......");
////        rs.close();
//        preparedStatement .close();
//        conn.close();
        return conn;

    }

    public ArrayList<East> eastArrayList() throws Exception {
        ArrayList<East> eastList = new ArrayList<East>();
        conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/transaction_schema?" +
                "user=testuser&password=testpassword");
        ResultSet rs = null;
        Statement stmt = conn.createStatement();
        rs = stmt.executeQuery("SELECT * FROM east");
        while (rs.next()) {
            East east = new East();
            east.setId_East(rs.getInt("id"));
            east.setDuration(rs.getInt("duration"));

            east.setRevenue(rs.getInt("revenue"));
            eastList.add(east);
        }
        return eastList;
    }

    public ArrayList<West> westArrayList() throws Exception {
        ArrayList<West> westArrayList = new ArrayList<West>();
        conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/transaction_schema?" +
                "user=testuser&password=testpassword");
        ResultSet rs = null;
        Statement stmt = conn.createStatement();
        rs = stmt.executeQuery("SELECT * FROM west");
        while (rs.next()) {
            West west = new West();
            west.setId_West(rs.getInt("id"));
            west.setTime(rs.getInt("time"));
            west.setCost(rs.getInt("cost"));
            westArrayList.add(west);
        }


//
//
//        West west1 = new West();
//        west1.setId_West(0);
//        west1.setCost(6);
//        west1.setTime(100);
//        westArrayList.add(west1);
//        West west2 = new West();
//        west2.setId_West(1);
//        west2.setCost(11);
//        west2.setTime(140);
//        westArrayList.add(west2);
//        West west3 = new West();
//        west3.setId_West(2);
//        west3.setCost(10);
//        west3.setTime(80);
//        westArrayList.add(west3);
//        West west4 = new West();
//        west4.setId_West(3);
//        west4.setCost(5);
//        west4.setTime(90);
//        westArrayList.add(west4);
        return westArrayList;

    }

    public void test() throws Exception {
        ArrayList<East> eastArrayList = new TestCorrectness().eastArrayList();
        ArrayList<West> westArrayList = new TestCorrectness().westArrayList();
//        for(int i=0;i<eastArrayList.size();i++){
//            int index= find_indexWestTime(westArrayList, eastArrayList.get(i).getRevenue());
//            System.out.println((index+1));
//        }


        BPlusTree bPlusTreeEastDuration = new BPlusTree(4);
        BPlusTree bPlusTreeEastRevenue = new BPlusTree(4);
        BPlusTree bPlusTreeWestTime = new BPlusTree(4);
        BPlusTree bPlusTreeWestCost = new BPlusTree(4);
        for (East east : eastArrayList) {
            bPlusTreeEastDuration.insert(east.getDuration(), east.getId_East());
            bPlusTreeEastRevenue.insert(east.getRevenue(), east.getId_East());
        }
        for (West west : westArrayList) {
            bPlusTreeWestTime.insert(west.getTime(), west.getId_West());
            bPlusTreeWestCost.insert(west.getCost(), west.getId_West());
        }
        ArrayList<Permutation> permutationsArrayLeftDuration = new ArrayList<>();
        ArrayList<Permutation> permutationArrayListRevenue = new ArrayList<>();
        ArrayList<Permutation> permutationArrayListTime = new ArrayList<>();
        ArrayList<Permutation> permutationArrayListCost = new ArrayList<>();
        Node nodeLeftDuration = bPlusTreeEastDuration.leftMostNode();
        while (nodeLeftDuration != null) {
            //   System.out.println(nodeLeftDuration);
            for (int i = 0; i < nodeLeftDuration.getKeys().size(); i++) {
                permutationsArrayLeftDuration.add(new Permutation(nodeLeftDuration.getKeys().get(i).getKey(), nodeLeftDuration.getKeys().get(i).getValues()));

            }
            nodeLeftDuration = nodeLeftDuration.getNext();
        }
        Node nodeLeftRevenue = bPlusTreeEastRevenue.leftMostNode();
        while (nodeLeftRevenue != null) {

            for (int i = 0; i < nodeLeftRevenue.getKeys().size(); i++) {
                permutationArrayListRevenue.add(new Permutation(nodeLeftRevenue.getKeys().get(i).getKey(), nodeLeftRevenue.getKeys().get(i).getValues()));

            }
            nodeLeftRevenue = nodeLeftRevenue.getNext();
        }

        Node nodeRightTime = bPlusTreeWestTime.leftMostNode();
        while (nodeRightTime != null) {

            for (int i = 0; i < nodeRightTime.getKeys().size(); i++) {
                permutationArrayListTime.add(new Permutation(nodeRightTime.getKeys().get(i).getKey(), nodeRightTime.getKeys().get(i).getValues()));
            }
            nodeRightTime = nodeRightTime.getNext();
        }

        Node nodeRightCost = bPlusTreeWestCost.leftMostNode();
        while (nodeRightCost != null) {
            for (int i = 0; i < nodeRightCost.getKeys().size(); i++) {
                permutationArrayListCost.add(new Permutation(nodeRightCost.getKeys().get(i).getKey(), nodeRightCost.getKeys().get(i).getValues()));
            }
            nodeRightCost = nodeRightCost.getNext();
            //  System.out.println("Here");
        }
        Node nodeLeftDuration1 = bPlusTreeEastDuration.leftMostNode();
        Node nodeLeftRevenue1 = bPlusTreeEastRevenue.leftMostNode();
        // Node nodeRightTime1 = bPlusTreeWestTime.leftMostNode();
        // Node nodeRightCost1 = bPlusTreeWestCost.leftMostNode();


        ArrayList<Permutation> eastPermutation = new TestCorrectness().permutationComputation(permutationsArrayLeftDuration, permutationArrayListRevenue, 200004, 0, null, null);
        ArrayList<Permutation> westPermutation = new TestCorrectness().permutationComputation(permutationArrayListTime, permutationArrayListCost, 200004, 0, null, null);
        ArrayList<Offset> offsetEast = new TestCorrectness().offsetComputationExtremeCase(nodeLeftDuration1, bPlusTreeWestTime);
        ArrayList<Offset> offsetWest = new TestCorrectness().offsetComputationExtremeCase(nodeLeftRevenue1, bPlusTreeWestCost);
//
//     for(int i=0;i<eastPermutation.size();i++){
//         System.out.println(eastPermutation.get(i).getIndex());
//     }
//      System.out.println(offsetEast);
//  System.out.println(offsetWest);
////  // System.exit(-1);
//  System.out.println(eastPermutation);
// System.out.println(westPermutation);
//
        new TestCorrectness().lookup(offsetWest, eastPermutation, offsetEast, westPermutation);
    }

    public ArrayList<Permutation> permutationComputation(ArrayList<Permutation> permutationsArrayLeft, ArrayList<Permutation> permutationsArrayRight, int count, int taskID, String streamID, Tuple tuple) {
        ArrayList<Permutation> arrayAListPermutation = new ArrayList<>();
        int[] holdingList = new int[count];
        int counter = 1;
        for (int i = 0; i < permutationsArrayLeft.size(); i++) {
            for (int ids : permutationsArrayLeft.get(i).getListOfIDs()) {
                holdingList[ids] = counter;
                counter++;
            }
        }
        //ArrayList<Permutation> permutationArray= new ArrayList<Permutation>(holdingList.length);
        for (int i = 0; i < permutationsArrayRight.size(); i++) {
            for (int ids : permutationsArrayRight.get(i).getListOfIDs()) {
                //Emit these tuples at once
//                System.out.println(holdingList[ids]);
                arrayAListPermutation.add(new Permutation(holdingList[ids], permutationsArrayRight.get(i).getId(), ids));
                // System.out.println(holdingList[ids]+"....."+permutationsArrayRight.get(i).getIndex());
                //collector.emitDirect(taskID,streamID,tuple, new Values(holdingList[ids],permutationsArrayRight.get(i).getIndex(),false,System.currentTimeMillis()));
                //  permutationArray.add(new Permutation(holdingList[ids],permutationsArrayRight.get(i).getIndex()));
            }
        }
        //  collector.emitDirect(taskID,streamID,tuple, new Values(0,0,true,System.currentTimeMillis()));

        //ystem.out.println(permutationArray.size());
        // return permutationArray;
        return arrayAListPermutation;
    }

    public ArrayList<Offset> offsetComputation(Node nodeForLeft, BPlusTree rightBTree, OutputCollector collector, int taskId, String streamID, String nodeName, Tuple
            tuple) {
        offsetComputationExtremeCase(nodeForLeft, rightBTree);
        //   System.exit(-1);


        ArrayList<Offset> listOffset = new ArrayList<>();
        int relativeIndexOfLeftInRight = 0;
        Node intermediateNode = null;
        boolean checkIndex = false;
        Node nodeForRight = null;

        while (nodeForLeft != null) {

            for (Key keyNode : nodeForLeft.getKeys()) {
                int key = keyNode.getKey();
                int valueSize = keyNode.getValues().size();
                if (!checkIndex) {
                    nodeForRight = rightBTree.searchRelativeNode(key);
                    intermediateNode = nodeForRight.getPrev();
                    while (intermediateNode != null) {
                        for (Key key1 : intermediateNode.getKeys()) {
                            relativeIndexOfLeftInRight += key1.getValues().size();
                        }
                        //  relativeIndexOfLeftInRight += intermediateNode.getKeys().size();
                        intermediateNode = intermediateNode.getPrev();
                    }
                    checkIndex = true;
                }

                boolean foundKey = false;
                int val = 0;
                while (nodeForRight != null) {

                    for (int j = 0; j < nodeForRight.getKeys().size(); j++) {


                        if (nodeForRight.getKeys().get(j).getKey() >= key) {
                            BitSet bitSet1 = new BitSet();
                            if (nodeForRight.getKeys().get(j).getKey() == key) {
                                bitSet1.set(0, true);
                            }
                            for (int size = 0; size < valueSize; size++)
                                listOffset.add(new Offset(key, ((relativeIndexOfLeftInRight + j) + 1), bitSet1));


                            foundKey = true;
                            break;
                        } else if (nodeForRight.getNext() == null && j == nodeForRight.getKeys().size() - 1 && key > nodeForRight.getKeys().get(j).getKey()) {
                            Key key1 = nodeForRight.getKeys().get(j);
                            //int val=key1.getValues().size();
                            int newIndex = relativeIndexOfLeftInRight + (nodeForRight.getKeys().size() + 1);
                            // int sized=nodeForRight.getKeys().get(j).getValues().size();
                            BitSet bitSet = new BitSet(1);
                            bitSet.set(0, false);
                            for (int size = 0; size < valueSize; size++)
                                listOffset.add(new Offset(key, newIndex, bitSet));
                            foundKey = true;
                            break;
                        }
                    }

                    if (foundKey) {
                        break;
                    } else {
                        relativeIndexOfLeftInRight += nodeForRight.getKeys().size();
                        nodeForRight = nodeForRight.getNext();
                    }
                }
            }
            nodeForLeft = nodeForLeft.getNext();
        }
        //collector.emitDirect(taskId,streamID,tuple, new Values(0, 0,true, System.currentTimeMillis()));
        return listOffset;
    }

    public int Search(ArrayList<Offset> offsetArrayL2, ArrayList<Permutation> permutationArrayL1, ArrayList<Offset> offsetArrayL1, ArrayList<Permutation> permutationArrayL2) {

        bitSet = new BitSet();
        //   System.out.println(permutationArrayL1.length+"The Length is ");
        int index = 1;
        for (int i = 0; i < offsetArrayL2.size(); i++) {
            System.out.println(offsetArrayL2.get(i).getIndex());
            int off2 = Math.min(offsetArrayL2.get(i).getIndex(), permutationArrayL1.size());
            System.out.println(off2);
            //   System.out.println(offsetArrayL2.get(i).getIndex()+".."+permutationArrayL1.size());
            for (int j = index; j <= off2; j++) {
                // System.out.println(permutationArrayL2[j].getIndex());
                bitSet.set(permutationArrayL2.get(j - 1).getTuple(), true);
            }
            // System.out.println(bitSet);
            index = off2;
            try {
                // System.out.println(permutationArrayL1.length + "The Length is " + offsetArrayL1.size());
                if ((permutationArrayL1.get(i).getTuple() + 1) < offsetArrayL1.size())
                    for (int j = offsetArrayL1.get(permutationArrayL1.get(i).getTuple() + 1).getIndex(); j < permutationArrayL2.size(); j++) {
//            System.out.println(bitSet);
                        if (bitSet.get(j)) {
                            //System.out.println("..."+offsetArrayL1[permutationArrayEast[i]]);
                            System.out.println(bitSet + "...." + j);
                            //
                        }
                    }

            } catch (ArrayIndexOutOfBoundsException e) {
                System.out.println("The Exception is " + permutationArrayL1.get(i - 1) + 1);
            }
        }

        return 0;
    }

    public void lookup(ArrayList<Offset> offsetArrayL2, ArrayList<Permutation> permutationArrayL1, ArrayList<Offset> offsetArrayL1, ArrayList<Permutation> permutationArrayL2) {

        int count = 0;

        BitSet bitSet = new BitSet();
        // for(int i=0;i<1;i++){
        long initialTime = System.currentTimeMillis();
        for (int i = 0; i < offsetArrayL2.size(); i++) {
            int offset = offsetArrayL2.get(i).getIndex() - 1;

            offset = offset - 1;
            // System.exit(-1);
            if (offset >= 0) {

                for (int j = 0; j <= offset; j++) {

                    try {
                        bitSet.set(permutationArrayL2.get(j).getTuple() - 1, true);

                    } catch (IndexOutOfBoundsException e) {
                        System.out.println(permutationArrayL2.size() + "..." + offset);
                    }
                }
                int permutationArray = permutationArrayL1.get(i).getTuple() - 1;
                Offset offset1 = offsetArrayL1.get(permutationArray);
                int off = offset1.getIndex() - 1;

                if (offset1.getBitSet().get(0)) {
                    for (int k = off + offset1.getSize(); k < permutationArrayL2.size(); k++) {
                        if (bitSet.get(k)) {
                            count++;

                            //   System.out.println("I am here"+k);
                        }
                    }

                } else {
                    for (int k = off; k < permutationArrayL2.size(); k++) {

                        if (bitSet.get(k)) {
                            //System.out.println(k);
                            count++;
                            //System.out.println(permutationArrayL1.get(i).getIdsForTest() + "....." + (k));


                        }
                    }
                    // System.out.println(bitSet+"....Count");
//                  System.exit(-1);
                }
            }
            //  System.out.println(bitSet+"..."+offsetArrayL2.get(i).getKey()+"..."+offsetArrayL2.get(i));
        }
        long finalTime = System.currentTimeMillis() - initialTime;
        System.out.println("Count==   " + count + "    Time in ms==   " + finalTime);

    }

    public Connection insertEast(int size) throws Exception {
        // Statement stmt = null;
        try {
            conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/transaction_schema?" +
                    "user=testuser&password=testpassword");
        } catch (Exception e) {
            System.out.println(" The exception is e" + e);
        }
        System.out.println(conn);
        ResultSet rs = null;
        Statement stmt = conn.createStatement();
        int id_East = 400;
        int id = 0;
        int duration = 0;
        int revenue = 0;
        int core = 0;
        Random random = new Random();
        PreparedStatement preparedStatement = null;
        String truncateQuery = "truncate table east";
        stmt.executeUpdate(truncateQuery);
        String insertQuery = "INSERT INTO east (id_East,duration,revenue,core,id) values (?,?,?,?,?)";
        for (int i = 0; i < size; i++) {

            id_East = id_East + 1;
            id = id + 1;
            duration = random.nextInt(100000);
            revenue = random.nextInt(100000);
            core = random.nextInt(100000);
            preparedStatement = conn.prepareStatement(insertQuery);
            preparedStatement.setInt(1, id_East);
            preparedStatement.setInt(2, duration);
            preparedStatement.setInt(3, revenue);
            preparedStatement.setInt(4, core);
            preparedStatement.setInt(5, id);
            preparedStatement.executeUpdate();
        }
        preparedStatement.close();
        conn.close();
        return conn;
    }

    public Connection insertWest(int size) throws Exception {
        // Statement stmt = null;
        conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/transaction_schema?" +
                "user=testuser&password=testpassword");
        ResultSet rs = null;
        Statement stmt = conn.createStatement();
        int id_West = 400;
        int id = 0;
        int time = 0;
        int cost = 0;
        int core = 0;
        Random random = new Random();
        PreparedStatement preparedStatement = null;
        String truncateQuery = "truncate table west";
        stmt.executeUpdate(truncateQuery);
        String insertQuery = "INSERT INTO west (id_West,time,cost,core,id) values (?,?,?,?,?)";
        for (int i = 0; i < size; i++) {

            id_West = id_West + 1;
            id = id + 1;
            time = random.nextInt(100000);
            cost = random.nextInt(100000);
            core = random.nextInt(100000);
            preparedStatement = conn.prepareStatement(insertQuery);
            preparedStatement.setInt(1, id_West);
            preparedStatement.setInt(2, time);
            preparedStatement.setInt(3, cost);
            preparedStatement.setInt(4, core);
            preparedStatement.setInt(5, id);
            preparedStatement.executeUpdate();
        }
        preparedStatement.close();
        conn.close();
        return conn;
    }

    public ArrayList<Offset> offsetComputationExtremeCase(Node nodeForLeft, BPlusTree rightBTree) {
        ArrayList<Offset> offsetArrayList = new ArrayList<>();
        int key = nodeForLeft.getKeys().get(0).getKey(); // FirstKEy Added
        boolean check = false;
        List<Integer> values = nodeForLeft.getKeys().get(0).getValues();
        Node node = rightBTree.searchRelativeNode(key);
        // System.out.println("NodeForRight"+node+"..."+key);
        int globalCount = 0;
        int startingIndexForNext = 0;
        BitSet bitset1 = null;
        int sizeOfvalues = 0;
        for (int i = 0; i < node.getKeys().size(); i++) {
            if (node.getKeys().get(i).getKey() < key) {
                globalCount += node.getKeys().get(i).getValues().size();
                //System.out.println("NodeForRight"+globalCount+"..."+key);
                //New
                sizeOfvalues = node.getKeys().get(i).getValues().size();

            }
            if ((node.getKeys().get(i).getKey() >= key) || (i == (node.getKeys().size() - 1))) {
                bitset1 = new BitSet();
                if (node.getKeys().get(i).getKey() == key) {
                    bitset1.set(0, true);
                }
                sizeOfvalues = node.getKeys().get(i).getValues().size();
                if ((i == (node.getKeys().size() - 1)) && (key > node.getKeys().get(i).getKey())) {
                    startingIndexForNext = 0;
                    // node =node.getNext();
                    check = true;
                } else {
                    startingIndexForNext = i;
                }
                break;
            }

        }
        //System.out.println("NodeForRight"+globalCount+"..."+calculatePreviousNode(node.getPrev()));
        globalCount = globalCount + calculatePreviousNode(node.getPrev());
        for (int j = 0; j < values.size(); j++) {
            offsetArrayList.add(new Offset(key, (globalCount + 1), bitset1, sizeOfvalues));
        }
        // Add to the Offset Array with key
        if (check) {
            // System.out.println(node+"....");

            linearScanning(nodeForLeft, node.getNext(), startingIndexForNext, globalCount, offsetArrayList);
        } else {
            // System.out.println(node+"....");

            linearScanning(nodeForLeft, node, startingIndexForNext, globalCount, offsetArrayList);
        }

        return offsetArrayList;
    }

    public int calculatePreviousNode(Node node) {
        int count = 0;
        while (node != null) {
            for (int i = 0; i < node.getKeys().size(); i++) {
                count += node.getKeys().get(i).getValues().size();
            }
            node = node.getPrev();
        }
        return count;
    }

    public int linearScanning(Node nodeForLeft, Node nodeForRight, int indexForStartingScanningFromRightNode, int globalCount, ArrayList<Offset> offsetArrayList) {
        boolean counterCheckForOverFlow = false;
        int counterGlobalCheck = 0;
        int startIndexForNodeForLeft = 1;
        // int startIndexForNodeForRight=indexForStartingScanningFromRightNode;
        while (nodeForLeft != null) {
            for (int i = startIndexForNodeForLeft; i < nodeForLeft.getKeys().size(); i++) {
                int key = nodeForLeft.getKeys().get(i).getKey();
                List<Integer> valuesForSearchingKey = nodeForLeft.getKeys().get(i).getValues();
                label1:
                while (nodeForRight != null) {
                    for (int j = indexForStartingScanningFromRightNode; j < nodeForRight.getKeys().size(); j++) {
                        int sizeOfValue = nodeForRight.getKeys().get(j).getValues().size();
                        if ((nodeForRight.getNext() == null) && (j == nodeForRight.getKeys().size() - 1) && (key > nodeForRight.getKeys().get(j).getKey())) {
                            if (!counterCheckForOverFlow) {
                                counterGlobalCheck = globalCount;
                                counterCheckForOverFlow = true;
                            }

                            if (counterCheckForOverFlow) {
                                int values = nodeForRight.getKeys().get(j).getValues().size(); //values in relative Index
                                BitSet bitset1 = new BitSet();
                                bitset1.set(0, false);
                                for (int k = 0; k < valuesForSearchingKey.size(); k++) {
                                    int gc = counterGlobalCheck + (values + 1);
                                    // System.out.println(counterGlobalCheck + "After"+gc);
                                    offsetArrayList.add(new Offset(key, gc, bitset1, sizeOfValue));

                                }
                            }
                            // Add here
                            break label1;
                        }

                        //System.out.println(j+"Indexxxx"+key);
                        // System.exit(-1);
                        if (nodeForRight.getKeys().get(j).getKey() < key) {
                            //System.out.println(nodeForRight.getKeys().get(j).getKey()+"...."+key);
                            globalCount = globalCount + (nodeForRight.getKeys().get(j).getValues().size());
                        }
                        if (nodeForRight.getKeys().get(j).getKey() >= key) {
                            BitSet bitset1 = new BitSet();
                            if (nodeForRight.getKeys().get(j).getKey() == key) {

                                bitset1.set(0, true);
                            }
                            for (int k = 0; k < valuesForSearchingKey.size(); k++) {
                                offsetArrayList.add(new Offset(key, (globalCount + 1), bitset1, sizeOfValue));
                                // System.out.println((globalCount + 1)+"...... "+nodeForRight);
                            }
                            indexForStartingScanningFromRightNode = j;
                            break label1;
                        }
                    }
                    indexForStartingScanningFromRightNode = 0;
                    nodeForRight = nodeForRight.getNext();
                }
            }
            nodeForLeft = nodeForLeft.getNext();
            startIndexForNodeForLeft = 0;
        }


        return 0;
    }

    public int find_indexWestTime(ArrayList<West> arr, int K) {
        int n = arr.size();
        // Traverse the array
        for (int i = 0; i < n; i++) {

            // If K is found
            if (arr.get(i).getCost() == K)
                return i;

                // If current array element
                // exceeds K
            else if (arr.get(i).getCost() > K)
                return i;
        }
        // If all elements are smaller
        // than K
        return n;
    }

    public void computeComputationForBPlusTree() throws Exception {
        ArrayList<East> eastArrayList = new TestCorrectness().eastArrayList();
        ArrayList<West> westArrayList = new TestCorrectness().westArrayList();
        BPlusTree bPlusTreeWestTime = new BPlusTree(4);
        BPlusTree bPlusTreeWestCost = new BPlusTree(4);
        BPlusTree bPlusTreeEastDuration = new BPlusTree(4);
        BPlusTree bPlusTreeEastRevenue = new BPlusTree(4);
        long time1 = System.currentTimeMillis();

        for (int k = 0; k < 50000; k++) {
            bPlusTreeWestTime.insert(westArrayList.get(k).getTime(), westArrayList.get(k).getId_West());
            bPlusTreeWestCost.insert(westArrayList.get(k).getCost(), westArrayList.get(k).getId_West());


        }
        for (int k = 0; k < 50000; k++) {
            bPlusTreeEastDuration.insert(eastArrayList.get(k).getDuration(), eastArrayList.get(k).getId_East());
            bPlusTreeEastRevenue.insert(eastArrayList.get(k).getRevenue(), eastArrayList.get(k).getId_East());
        }
        System.out.println("Time" + (System.currentTimeMillis() - time1));
        System.exit(-1);
        long time = System.currentTimeMillis();

        for (East east : eastArrayList) {
            BitSet duration = bPlusTreeWestTime.greaterThenSpecificValue(east.getDuration());
            BitSet revenue = bPlusTreeWestCost.lessThenSpecificValue(east.getRevenue());
            revenue.and(duration);


        }
        System.out.println(System.currentTimeMillis() - time);
    }

    public void computeComputationForCSSTree() throws Exception {
        ArrayList<East> eastArrayList = new TestCorrectness().eastArrayList();
        ArrayList<West> westArrayList = new TestCorrectness().westArrayList();
        CSSTreeUpdated CSSTreeWestTime = new CSSTreeUpdated(10);
        CSSTreeUpdated CSSTreeWestCost = new CSSTreeUpdated(10);
        //Auxialiarly
        CSSTreeUpdated CSSTreeEastDuration = new CSSTreeUpdated(4);
        CSSTreeUpdated CSSTreeEastRevenue = new CSSTreeUpdated(4);
        long time1 = System.currentTimeMillis();
        for (int i = 0; i < 50000; i++) {
            CSSTreeWestTime.insert(westArrayList.get(i).getTime(), westArrayList.get(i).getId_West());
            CSSTreeWestCost.insert(westArrayList.get(i).getCost(), westArrayList.get(i).getId_West());
        }
        for (int i = 0; i < 50000; i++) {
            CSSTreeEastDuration.insert(eastArrayList.get(i).getDuration(), eastArrayList.get(i).getId_East());
            CSSTreeEastRevenue.insert(eastArrayList.get(i).getRevenue(), eastArrayList.get(i).getId_East());
        }
        System.out.println("Time" + (System.currentTimeMillis() - time1));
        System.exit(-1);
        long time = System.currentTimeMillis();
        int i = 0;
        for (East east : eastArrayList) {
            BitSet duration = CSSTreeWestTime.searchGreaterBitSet(east.getDuration());
            BitSet revenue = CSSTreeWestCost.searchSmallerBitSet(east.getRevenue());
            revenue.and(duration);
            i++;

        }
        System.out.println(System.currentTimeMillis() - time);

    }

    public void permutationAndComputationArray() throws Exception {
        ArrayList<East> eastArrayList = new TestCorrectness().eastArrayList();
        ArrayList<West> westArrayList = new TestCorrectness().westArrayList();
        ArrayList<East> duration = new ArrayList<>(eastArrayList);
        ArrayList<East> revenue = new ArrayList<>(eastArrayList);
        ArrayList<West> time = new ArrayList<>(westArrayList);
        ArrayList<West> cost = new ArrayList<>(westArrayList);
        Collections.sort(duration, new Duration());
        Collections.sort(revenue, new Rev());
        Collections.sort(time, new Time());
        Collections.sort(cost, new Cost());
        Long time1 = System.currentTimeMillis();
        Thread t1 = new Thread(() -> {
            // Write emit method, stream id, down stream task
            permutationArrayEast(duration, revenue);
        });

        Thread t2 = new Thread(() -> {
            permutationArrayWest(time, cost);
        });
        Thread t3 = new Thread(() -> {
            try {
                offsetArrayOfDurationWithRespectToTime(duration, time);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        Thread t4 = new Thread(() -> {
            try {
                offsetArrayOfRevenueWithRespectToCost(revenue, cost);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        t1.start();
        t2.start();
        t3.start();
        t4.start();

        try {
            t1.join();
            t2.join();
            t3.join();
            t4.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.print(("Time=" + (System.currentTimeMillis() - time1)));


//        Long time1=System.currentTimeMillis();
//        permutationArrayEast(duration, revenue);
//        permutationArrayWest(time, cost);
//        System.out.print(("Time="+(System.currentTimeMillis()-time1)));
//        long time2= System.currentTimeMillis();
//        offsetArrayOfDurationWithRespectToTime(duration, time);
//        offsetArrayOfRevenueWithRespectToCost(revenue, cost);
//        System.out.print(("Time="+(System.currentTimeMillis()-time2)));
    }

    public void permutationArrayEast(ArrayList<East> duration, ArrayList<East> revenue) {
        permutationArrayEast = new int[revenue.size()];
        revenue:
        for (int i = 0; i < revenue.size(); i++) {
            duration:
            for (int j = 0; j < duration.size(); j++) {
                if ((revenue.get(i).getId_East()) == (duration.get(j).getId_East())) {
                    permutationArrayEast[i] = j;
                    //     System.out.println(permutationArray[i]);
                    break duration;
                }

            }
            continue revenue;
        }
        // return permutationArrayEast;
    }

    public void permutationArrayWest(ArrayList<West> time, ArrayList<West> cost) {
        permutationArrayWest = new int[cost.size()];
        cost:
        for (int i = 0; i < cost.size(); i++) {
            time:
            for (int j = 0; j < time.size(); j++) {
                if ((cost.get(i).getId_West()) == (time.get(j).getId_West())) {
                    permutationArrayWest[i] = j;
                    //     System.out.println(permutationArray[i]);
                    break time;
                }

            }
            continue cost;
        }

    }

    public void offsetArrayOfDurationWithRespectToTime(ArrayList<East> durationList, ArrayList<West> timeList) {
        offsetArrayOfDurationWithRespectToTime = new int[durationList.size()];
        for (int i = 0; i < durationList.size(); i++) {
            offsetArrayOfDurationWithRespectToTime[i] = find_indexWestTime(timeList, durationList.get(i).getDuration());
        }
        // return offsetArrayOfDurationWithRespectToTime;
    }

    public void offsetArrayOfRevenueWithRespectToCost(ArrayList<East> reveuneList, ArrayList<West> costList) {
        offsetArrayOfRevenueWithRespectToCost = new int[reveuneList.size()];
        for (int i = 0; i < reveuneList.size(); i++) {
            offsetArrayOfRevenueWithRespectToCost[i] = find_indexWestCost(costList, reveuneList.get(i).getRevenue());
        }
        //  return offsetArrayOfRevenueWithRespectToCost;
    }

    public int find_indexWestCost(ArrayList<West> arr, int K) {
        int n = arr.size();
        // Traverse the array
        for (int i = 0; i < n; i++)

            // If K is found
            if (arr.get(i).getCost() == K)
                return i;

                // If current array element
                // exceeds K
            else if (arr.get(i).getCost() > K)
                return i;

        // If all elements are smaller
        // than K
        return n;
    }

    public void permutationAndComputationArrayWithBplusTree() throws Exception {
        ArrayList<East> eastArrayList = new TestCorrectness().eastArrayList();
        ArrayList<West> westArrayList = new TestCorrectness().westArrayList();
//        for(int i=0;i<eastArrayList.size();i++){
//            int index= find_indexWestTime(westArrayList, eastArrayList.get(i).getRevenue());
//            System.out.println((index+1));
//        }


        BPlusTree bPlusTreeEastDuration = new BPlusTree(4);
        BPlusTree bPlusTreeEastRevenue = new BPlusTree(4);
        BPlusTree bPlusTreeWestTime = new BPlusTree(4);
        BPlusTree bPlusTreeWestCost = new BPlusTree(4);
        for (East east : eastArrayList) {
            bPlusTreeEastDuration.insert(east.getDuration(), east.getId_East());
            bPlusTreeEastRevenue.insert(east.getRevenue(), east.getId_East());
        }
        for (West west : westArrayList) {
            bPlusTreeWestTime.insert(west.getTime(), west.getId_West());
            bPlusTreeWestCost.insert(west.getCost(), west.getId_West());
        }
        ArrayList<Permutation> permutationsArrayLeftDuration = new ArrayList<>();
        ArrayList<Permutation> permutationArrayListRevenue = new ArrayList<>();
        ArrayList<Permutation> permutationArrayListTime = new ArrayList<>();
        ArrayList<Permutation> permutationArrayListCost = new ArrayList<>();
        Node nodeLeftDuration = bPlusTreeEastDuration.leftMostNode();
        while (nodeLeftDuration != null) {
            //   System.out.println(nodeLeftDuration);
            for (int i = 0; i < nodeLeftDuration.getKeys().size(); i++) {
                permutationsArrayLeftDuration.add(new Permutation(nodeLeftDuration.getKeys().get(i).getKey(), nodeLeftDuration.getKeys().get(i).getValues()));

            }
            nodeLeftDuration = nodeLeftDuration.getNext();
        }
        Node nodeLeftRevenue = bPlusTreeEastRevenue.leftMostNode();
        while (nodeLeftRevenue != null) {

            for (int i = 0; i < nodeLeftRevenue.getKeys().size(); i++) {
                permutationArrayListRevenue.add(new Permutation(nodeLeftRevenue.getKeys().get(i).getKey(), nodeLeftRevenue.getKeys().get(i).getValues()));

            }
            nodeLeftRevenue = nodeLeftRevenue.getNext();
        }

        Node nodeRightTime = bPlusTreeWestTime.leftMostNode();
        while (nodeRightTime != null) {

            for (int i = 0; i < nodeRightTime.getKeys().size(); i++) {
                permutationArrayListTime.add(new Permutation(nodeRightTime.getKeys().get(i).getKey(), nodeRightTime.getKeys().get(i).getValues()));
            }
            nodeRightTime = nodeRightTime.getNext();
        }

        Node nodeRightCost = bPlusTreeWestCost.leftMostNode();
        while (nodeRightCost != null) {
            for (int i = 0; i < nodeRightCost.getKeys().size(); i++) {
                permutationArrayListCost.add(new Permutation(nodeRightCost.getKeys().get(i).getKey(), nodeRightCost.getKeys().get(i).getValues()));
            }
            nodeRightCost = nodeRightCost.getNext();
            //  System.out.println("Here");
        }
        Node nodeLeftDuration1 = bPlusTreeEastDuration.leftMostNode();
        Node nodeLeftRevenue1 = bPlusTreeEastRevenue.leftMostNode();
        // Node nodeRightTime1 = bPlusTreeWestTime.leftMostNode();
        // Node nodeRightCost1 = bPlusTreeWestCost.leftMostNode();

        Long time2 = System.currentTimeMillis();
        new TestCorrectness().permutationComputation(permutationsArrayLeftDuration, permutationArrayListRevenue, 200004, 0, null, null);
        new TestCorrectness().permutationComputation(permutationArrayListTime, permutationArrayListCost, 200004, 0, null, null);
        new TestCorrectness().offsetComputationExtremeCase(nodeLeftDuration1, bPlusTreeWestTime);
        new TestCorrectness().offsetComputationExtremeCase(nodeLeftRevenue1, bPlusTreeWestCost);
        System.out.print(("Time=" + (System.currentTimeMillis() - time2)));

        Long time1 = System.currentTimeMillis();
        Thread t1 = new Thread(() -> {
            // Write emit method, stream id, down stream task
            new TestCorrectness().permutationComputation(permutationsArrayLeftDuration, permutationArrayListRevenue, 200004, 0, null, null);

        });

        Thread t2 = new Thread(() -> {
            new TestCorrectness().permutationComputation(permutationArrayListTime, permutationArrayListCost, 200004, 0, null, null);

        });
        Thread t3 = new Thread(() -> {
            try {
                new TestCorrectness().offsetComputationExtremeCase(nodeLeftDuration1, bPlusTreeWestTime);

            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        Thread t4 = new Thread(() -> {
            try {
                new TestCorrectness().offsetComputationExtremeCase(nodeLeftRevenue1, bPlusTreeWestCost);

            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        t1.start();
        t2.start();
        t3.start();
        t4.start();

        try {
            t1.join();
            t2.join();
            t3.join();
            t4.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.print(("Time=" + (System.currentTimeMillis() - time1)));
    }



}