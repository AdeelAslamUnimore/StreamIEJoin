package com.proposed.iejoinandbplustreebased;

import java.util.HashMap;
import java.util.Map;

public class Configuration {
    /*
    These constants for JoinerBoltForBitSetOperation
     */
  public Map<String,Object> configurationConstantForStreamIDs(){
      Map<String, Object> mapJoinerBoltConstant= new HashMap<>();
      mapJoinerBoltConstant.put("LeftPredicateSourceStreamIDBitSet","PredicateLeft");
      mapJoinerBoltConstant.put("RightPredicateSourceStreamIDBitSet","PredicateRight");
      mapJoinerBoltConstant.put("LeftPredicateTuple","Left");
      mapJoinerBoltConstant.put("RightPredicateTuple","Right");
      mapJoinerBoltConstant.put("LeftBatchPermutation", "PermutationLeft");
      mapJoinerBoltConstant.put("RightBatchPermutation","PermutationRight");
      return mapJoinerBoltConstant;
    }

}
