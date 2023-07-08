package com.configurationsandconstants.iejoinandbaseworks;

import org.apache.storm.Config;

import java.util.HashMap;
import java.util.Map;

public class Configuration {
    /*
    These constants for JoinerBoltForBitSetOperation
     */
  public static Map<String, Object> configurationConstantForStreamIDs(){
    //  Config mapJoinerBoltConstant= new Config();

     Map<String, Object> mapJoinerBoltConstant= new HashMap<>();
      mapJoinerBoltConstant.put("LeftPredicateTuple","LeftStreamTuples");
      mapJoinerBoltConstant.put("RightPredicateTuple","RightStream");


      mapJoinerBoltConstant.put("LeftSmallerPredicateTuple","LeftSmaller");
      mapJoinerBoltConstant.put("RightSmallerPredicateTuple","RightSmaller");
      mapJoinerBoltConstant.put("LeftGreaterPredicateTuple","LeftGreater");
      mapJoinerBoltConstant.put("RightGreaterPredicateTuple","RightGreater");


      mapJoinerBoltConstant.put("LeftPredicateSourceStreamIDBitSet","PredicateLeft");
      mapJoinerBoltConstant.put("RightPredicateSourceStreamIDBitSet","PredicateRight");

      mapJoinerBoltConstant.put("LeftBatchPermutation", "PermutationLeft");
      mapJoinerBoltConstant.put("RightBatchPermutation","PermutationRight");
      mapJoinerBoltConstant.put("LeftBatchOffset", "OffsetLeft");
      mapJoinerBoltConstant.put("RightBatchOffset","OffsetRight");
      mapJoinerBoltConstant.put("MergingFlag","mergeOperationInitiator");
      /// For HashSet Evaluation

      mapJoinerBoltConstant.put("LeftPredicateSourceStreamIDHashSet","PredicateLeft");
      mapJoinerBoltConstant.put("RightPredicateSourceStreamIDHashSet","PredicateRight");
      return mapJoinerBoltConstant;
    }

}
