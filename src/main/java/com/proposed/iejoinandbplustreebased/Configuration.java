package com.proposed.iejoinandbplustreebased;

import org.apache.storm.Config;

import java.util.HashMap;
import java.util.Map;

public class Configuration {
    /*
    These constants for JoinerBoltForBitSetOperation
     */
  public static Config configurationConstantForStreamIDs(){
      Config mapJoinerBoltConstant= new Config();
    //  Map<String, Object> mapJoinerBoltConstant= new HashMap<>();
      mapJoinerBoltConstant.put("LeftPredicateSourceStreamIDBitSet","PredicateLeft");
      mapJoinerBoltConstant.put("RightPredicateSourceStreamIDBitSet","PredicateRight");
      mapJoinerBoltConstant.put("LeftPredicateTuple","Left");
      mapJoinerBoltConstant.put("RightPredicateTuple","Right");

      mapJoinerBoltConstant.put("LeftSmallerPredicateTuple","LeftSmaller");
      mapJoinerBoltConstant.put("RightSmallerPredicateTuple","RightSmaller");
      mapJoinerBoltConstant.put("LeftGreaterPredicateTuple","LeftGreater");
      mapJoinerBoltConstant.put("RightGreaterPredicateTuple","RightGreater");

      mapJoinerBoltConstant.put("LeftBatchPermutation", "PermutationLeft");
      mapJoinerBoltConstant.put("RightBatchPermutation","PermutationRight");
      mapJoinerBoltConstant.put("LeftBatchOffset", "OffsetLeft");
      mapJoinerBoltConstant.put("RightBatchOffset","OffsetRight");
      mapJoinerBoltConstant.put("MergingFlag","mergeOperationInitiator");
      return mapJoinerBoltConstant;
    }

}
