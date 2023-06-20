package com.stormiequality.join;

import com.stormiequality.BTree.Offset;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Map;

public class IEJoinComputationBolt extends BaseRichBolt {
    int counter;
    int count;

    private Boolean leftStreamOffset;
    private Boolean rightStreamOffset;
    private Boolean leftStreamPermutation;
    private Boolean rightStreamPermutation;
    private ArrayList<Permutation> listLeftPermutation;
    private ArrayList<Permutation> listRightPermutation;
    private ArrayList<Offset> listOffsetArrayLeft;
    private ArrayList<Offset> listOffsetArrayRight;
    private BitSet bitSet=null;
    private BufferedWriter LeftStreamOffsetWrite;
    private BufferedWriter RightStreamOffsetWrite;
    private BufferedWriter LeftStreamPermutationWrite;
    private BufferedWriter RightStreamPermutationWrite;
    private BufferedWriter IEJoinStructure;
    private BufferedWriter tupleSearchWriter;
    private int windowSizeCount;
    private int taskID;
    public IEJoinComputationBolt(int count, int windowSizeCount){
        this.count= count;
        this.windowSizeCount=windowSizeCount;
    }
    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.leftStreamOffset=false;
        this.rightStreamOffset=false;
        this.leftStreamPermutation=false;
        this.rightStreamPermutation=false;
        this.listRightPermutation= new ArrayList<>();
        this.listLeftPermutation= new ArrayList<>();
        this.listOffsetArrayLeft= new ArrayList<>();
        this.listOffsetArrayRight= new ArrayList<>();
        this.taskID=topologyContext.getThisTaskId();
        try{
            LeftStreamOffsetWrite= new BufferedWriter(new FileWriter(new File("/home/adeel/Data/Results/LeftStreamOffsetWrite"+taskID+".csv")));
            RightStreamOffsetWrite= new BufferedWriter(new FileWriter(new File("/home/adeel/Data/Results/RightStreamOffsetWrite"+taskID+".csv")));
            LeftStreamPermutationWrite= new BufferedWriter(new FileWriter(new File("/home/adeel/Data/Results/LeftStreamPermutationWrite"+taskID+".csv")));
            RightStreamPermutationWrite= new BufferedWriter(new FileWriter(new File("/home/adeel/Data/Results/RightStreamPermutationWrite"+taskID+".csv")));
            IEJoinStructure=new BufferedWriter(new FileWriter(new File("/home/adeel/Data/Results/IEJoinStructure."+taskID+".csv")));
            tupleSearchWriter= new BufferedWriter(new FileWriter(new File("/home/adeel/Data/Results/tupleSearchWriter"+taskID+".csv")));
        }catch (Exception e){

        }

    }

    @Override
    public void execute(Tuple tuple) {

        if(tuple.getSourceStreamId().equals("LeftStreamTuples")||tuple.getSourceStreamId().equals("RightStream")){
            // if(tuple.getSourceComponent().equals("testSpout")){
            if(bitSet!=null){
                // int locationInOffset= binarySearch(offsetRight, tuple.getInteger(0));
                long initialTime=System.currentTimeMillis();
                int permutationLocation=binarySearchForPermutation(listLeftPermutation,tuple.getInteger(1));
                // System.out.println(permutationLocation+".."+tuple.getInteger(1));
                int index= listOffsetArrayLeft.get(permutationLocation).getIndex();
                int check=0;
                for (int j = index+1; j < listRightPermutation.size(); j++) {

                    if (bitSet.get(j)) {
                        check++;
                        //  System.out.println(bitSet);
                        //System.out.println("..."+offsetArrayL1[permutationArrayEast[i]]);
                        // System.out.println(bitSet+"...."+j);
                        //
                    }
                }
                long finalTime=System.currentTimeMillis()-initialTime;
                try{
                    tupleSearchWriter.write(finalTime+","+permutationLocation+"\n");
                    tupleSearchWriter.flush();
                }catch (Exception e){

                }

            }

            this.counter++;
            if(this.counter==windowSizeCount){
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
                try{
                    LeftStreamOffsetWrite.write(tuple.getLongByField("Time")+"\n");
                    LeftStreamOffsetWrite.flush();
                }catch (Exception e){

                }
            }

        }
        if(tuple.getSourceStreamId().equals("RightStreamOffset")){
            if(tuple.getValueByField("Flag").equals(true)){
                rightStreamOffset=true;
            }
            else{
                listOffsetArrayRight.add(new Offset(tuple.getIntegerByField("Tuple"), tuple.getIntegerByField("Index")));
                try{
                    RightStreamOffsetWrite.write(tuple.getLongByField("Time")+"\n");
                    RightStreamOffsetWrite.flush();
                }catch (Exception e){

                }
            }
        }
        if(tuple.getSourceStreamId().equals("LeftStreamID")){

            if(tuple.getValueByField("Flag").equals(true)){
                leftStreamPermutation=true;
            }
            else{
                listLeftPermutation.add(new Permutation(tuple.getIntegerByField("Index"),tuple.getIntegerByField("Key")));
                try{
                    LeftStreamPermutationWrite.write(tuple.getLongByField("Time")+"\n");
                    LeftStreamPermutationWrite.flush();
                }catch (Exception e){

                }
            }

        }
        if(tuple.getSourceStreamId().equals("RightStreamID")){
            if(tuple.getValueByField("Flag").equals(true)){
                rightStreamPermutation=true;
            }
            else{
                listRightPermutation.add(new Permutation(tuple.getIntegerByField("Index"),tuple.getIntegerByField("Key")));
               // System.out.println("Here");
                try{
                    RightStreamPermutationWrite.write(tuple.getLongByField("Time")+"\n");
                    RightStreamPermutationWrite.flush();
                }catch (Exception e){

                }
            }
        }
        if((rightStreamOffset==true)&&(leftStreamOffset==true)&&(leftStreamPermutation==true)&(rightStreamPermutation==true)){
//            System.out.println(listLeftPermutation.size());
//            System.out.println(listRightPermutation.size());
//            System.out.println(listOffsetArrayLeft.size());
//            System.out.println(listOffsetArrayRight.size());
//           System.exit(-1);
            long searchStartTime= System.currentTimeMillis();
            Search(listOffsetArrayRight,listLeftPermutation,listOffsetArrayLeft,listRightPermutation);
            long endSearchTime=System.currentTimeMillis()-searchStartTime;

            rightStreamOffset=false;
            leftStreamOffset=false;
            leftStreamPermutation=false;
            rightStreamPermutation=false;
            this.counter=count;
//            this.listRightPermutation= new ArrayList<>();
//            this.listLeftPermutation= new ArrayList<>();
//            this.listOffsetArrayLeft= new ArrayList<>();
//            this.listOffsetArrayRight= new ArrayList<>();
            try{
                IEJoinStructure.write(endSearchTime+"\n");
                IEJoinStructure.flush();
                LeftStreamOffsetWrite.write("\n");
                LeftStreamOffsetWrite.flush();
                RightStreamOffsetWrite.write("\n");
                RightStreamOffsetWrite.flush();
                LeftStreamPermutationWrite.write("\n");
                LeftStreamPermutationWrite.flush();
                RightStreamPermutationWrite.write("\n");
                RightStreamPermutationWrite.flush();

            }catch (Exception e){

            }

        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
    public  void  Search(ArrayList<Offset> offsetArrayL2, ArrayList<Permutation> permutationArrayL1,  ArrayList<Offset> offsetArrayL1, ArrayList<Permutation> permutationArrayL2){

        bitSet= new BitSet(count);
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
