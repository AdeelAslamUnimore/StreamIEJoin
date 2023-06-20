package com.stormiequality.join;

import com.stormiequality.BTree.Offset;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TupleWindow;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;

public class TestBolt extends BaseRichBolt {
    int counter=0;
    private Boolean leftStreamOffset;
    private Boolean rightStreamOffset;
    private Boolean leftStreamPermutation;
    private Boolean rightStreamPermutation;
    private ArrayList<Permutation> listLeftPermutation;
    private ArrayList<Permutation> listRightPermutation;
    private ArrayList<Offset> listOffsetArrayLeft;
    private ArrayList<Offset> listOffsetArrayRight;
    private BitSet bitSet=null;

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.counter=0;
        this.leftStreamOffset=false;
        this.rightStreamOffset=false;
        this.leftStreamPermutation=false;
        this.rightStreamPermutation=false;
        this.listRightPermutation= new ArrayList<>();
        this.listLeftPermutation= new ArrayList<>();
        this.listOffsetArrayLeft= new ArrayList<>();
        this.listOffsetArrayRight= new ArrayList<>();

    }

    @Override
    public void execute(Tuple tuple) {

        if(tuple.getSourceStreamId().equals("LeftStreamTuples")||tuple.getSourceStreamId().equals("RightStream")){
            // if(tuple.getSourceComponent().equals("testSpout")){
            if(bitSet!=null){
                // int locationInOffset= binarySearch(offsetRight, tuple.getInteger(0));
                int permutationLocation=binarySearchForPermutation(listLeftPermutation,tuple.getInteger(1));
                // System.out.println(permutationLocation+".."+tuple.getInteger(1));
                int index= listOffsetArrayLeft.get(permutationLocation).getIndex();
                for (int j = index+1; j < listRightPermutation.size(); j++) {

                    if (bitSet.get(j)) {
                        //  System.out.println(bitSet);
                        //System.out.println("..."+offsetArrayL1[permutationArrayEast[i]]);
                        // System.out.println(bitSet+"...."+j);
                        //
                    }
                }

            }

            this.counter++;
            if(this.counter==2000){
            this.bitSet= null;

            this.listRightPermutation= new ArrayList<>();
            this.listLeftPermutation= new ArrayList<>();
            this.listOffsetArrayLeft= new ArrayList<>();
            this.listOffsetArrayRight= new ArrayList<>();
            }


//         SearchSingleTuple(permutationLeft, offsetLeft, permutationRight, offset1 > offset2 ? offset2 : offset1, offset1 > offset2 ? offset1 : offset2);
//          counter++;
        }



        //   System.out.println(tuple);
        if(tuple.getSourceStreamId().equals("LeftStreamOffset")){
            if(tuple.getValueByField("Flag").equals(true)){
                leftStreamOffset=true;
            }
            else{
                listOffsetArrayLeft.add(new Offset(tuple.getIntegerByField("Tuple"), tuple.getIntegerByField("Index")));

            }

        }
        if(tuple.getSourceStreamId().equals("RightStreamOffset")){
            if(tuple.getValueByField("Flag").equals(true)){
                rightStreamOffset=true;
            }
            else{
                listOffsetArrayRight.add(new Offset(tuple.getIntegerByField("Tuple"), tuple.getIntegerByField("Index")));

            }
        }
        if(tuple.getSourceStreamId().equals("LeftStreamID")){

            if(tuple.getValueByField("Flag").equals(true)){
                leftStreamPermutation=true;
            }
            else{
                listLeftPermutation.add(new Permutation(tuple.getIntegerByField("Index"),tuple.getIntegerByField("Key")));

            }

        }
        if(tuple.getSourceStreamId().equals("RightStreamID")){
            if(tuple.getValueByField("Flag").equals(true)){
                rightStreamPermutation=true;
            }
            else{
                listRightPermutation.add(new Permutation(tuple.getIntegerByField("Index"),tuple.getIntegerByField("Key")));
                // System.out.println("Here");

            }
        }
        if((rightStreamOffset==true)&&(leftStreamOffset==true)&&(leftStreamPermutation==true)&(rightStreamPermutation==true)){
            System.out.println(listLeftPermutation.size());
            System.out.println(listRightPermutation.size());
            System.out.println(listOffsetArrayLeft.size());
            System.out.println(listOffsetArrayRight.size());
            // System.exit(-1);
            long searchStartTime= System.currentTimeMillis();
            Search(listOffsetArrayRight,listLeftPermutation,listOffsetArrayLeft,listRightPermutation);
            long endSearchTime=System.currentTimeMillis()-searchStartTime;

            rightStreamOffset=false;
            leftStreamOffset=false;
            leftStreamPermutation=false;
            rightStreamPermutation=false;
            counter=200;
//            this.listRightPermutation= new ArrayList<>();
//            this.listLeftPermutation= new ArrayList<>();
//            this.listOffsetArrayLeft= new ArrayList<>();
//            this.listOffsetArrayRight= new ArrayList<>();


        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
    public  void  Search(ArrayList<Offset> offsetArrayL2, ArrayList<Permutation> permutationArrayL1,  ArrayList<Offset> offsetArrayL1, ArrayList<Permutation> permutationArrayL2){

        bitSet= new BitSet();
        //   System.out.println(permutationArrayL1.length+"The Length is ");
        int index=1;
        for(int i=0;i<offsetArrayL2.size();i++) {

            int off2 = Math.min(offsetArrayL2.get(i).getIndex(), permutationArrayL1.size());
            //   System.out.println(offsetArrayL2.get(i).getIndex()+".."+permutationArrayL1.size());
            for (int j = index; j <= off2; j++) {
                // System.out.println(permutationArrayL2[j].getIndex());
                bitSet.set(permutationArrayL2.get(j-1).getIndex(), true);
            }
            index = off2;
            try {
                // System.out.println(permutationArrayL1.length + "The Length is " + offsetArrayL1.size());
                if((permutationArrayL1.get(i).getIndex() + 1)<offsetArrayL1.size())
                    for (int j = offsetArrayL1.get(permutationArrayL1.get(i).getIndex() + 1).getIndex(); j < permutationArrayL2.size(); j++) {
//            System.out.println(bitSet);
                        if (bitSet.get(j)) {
                            //System.out.println("..."+offsetArrayL1[permutationArrayEast[i]]);
                            // System.out.println(bitSet+"...."+j);
                            //
                        }
                    }

            }
            catch (ArrayIndexOutOfBoundsException e){
                System.out.println("The Exception is "+permutationArrayL1.get(i - 1) + 1);
            }
        }

//        return initialCount;
    }


    public static int binarySearchForPermutation(ArrayList<Permutation> permutation, int key) {
        int left = 0;
        int right = permutation.size() - 1;
        int result=permutation.size() - 1;
        // int result=
        while (left <= right) {
            int mid = (left + right) / 2;
            if (permutation.get(mid).getValue() == key) {
                // Found the element
                return mid;
            } else if (permutation.get(mid).getValue() < key) {
                // Search in the right half of the list
                left = mid + 1;
//                if(permutation[mid].getValue()==(permutation.length-1)){
//
//                    return (permutation.length-1);
//                }
                // System.out.println(permutation.length);
            } else {
                // Search in the left half of the list
                right = mid - 1;
                result=mid;
            }
        }

        // Element not found
        return result;
    }

}
