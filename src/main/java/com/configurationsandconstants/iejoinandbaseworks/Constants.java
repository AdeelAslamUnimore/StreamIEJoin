package com.configurationsandconstants.iejoinandbaseworks;

public class Constants {
    /*
    Constants for mutableBPlusTreeBolt
     */
    public static final String TUPLE = "tuple";
    public static final String TUPLE_ID = "ID";
    public static final String BYTE_ARRAY = "byte_array";
    public static final int ORDER_OF_B_PLUS_TREE = 4;
    //It can be any batch permutation or offset from every relation
    public static final String BATCH_COMPLETION_FLAG = "flag_after_batch_completion";
    public static final String PERMUTATION_TUPLE_IDS = "permutation_tuple_IDS";
    public static final String PERMUTATION_COMPUTATION_BOLT_ID = "permutation_bolt";
    public static final String OFFSET_AND_IE_JOIN_BOLT_ID = "offset_and_IEJoin";
    public static final String OFFSET_TUPLE_INDEX = "offset_tuple_index";
    public static final String OFFSET_SIZE_OF_TUPLE = "size_of_tuples";
    public static final String MERGING_OPERATION_FLAG = "flag_during_merging";
    public static final String PERMUTATION_COMPUTATION_INDEX = "permutation_index";
    public static final String LEFT_PREDICATE_BOLT = "left_stream";
    public static final String RIGHT_PREDICATE_BOLT = "right_stream";
    public static final String BIT_SET_EVALUATION_BOLT ="bitset_evaluation";
    public static final int MUTABLE_WINDOW_SIZE =1000;
    public static final int IMMUTABLE_WINDOW_SIZE =10000;

    //// Constant for benchMarks///
    public static final int ORDER_OF_CSS_TREE = 4;
    public static final String LEFT_HASH_SET="left_hash_set";
    public static final String RIGHT_HASH_SET="right_hash_set";
    public static final String LEFT_PREDICATE_CSS_TREE_BOLT="leftPredicateCSSTreeBolt";
    public static final String RIGHT_PREDICATE_CSS_TREE_BOLT="rightPredicateCSSTreeBolt";
    public static final String MUTABLE_PART_EVALUATION_BOLT="mutablePartEvaluationBolt";
    public static final int IMMUTABLE_CSS_PART_REMOVAL=10000;
    public static final String BATCH_CSS_TREE_KEY="batchCSSTreeKey";
    public static final String BATCH_CSS_TREE_VALUES="batchCSSTreeValues";
    public static final String BATCH_CSS_FLAG="flag";
    public static final String LEFT_PREDICATE_IMMUTABLE_CSS= "leftPredicateImmutableCSS";
    public static final String RIGHT_PREDICATE_IMMUTABLE_CSS="rightPredicateImmutableCSS";
    public static final String IMMUTABLE_HASH_SET_EVALUATION="immutableHashSetEvaluation";
    public static final String MERGE_BOLT_EVALUATION_CSS="mergeBoltEvaluationCSS";
    // For BPlusTree Linked Tree and RedBlack tree
    public static final int TUPLE_ARCHIVE_THRESHOLD =500;
    public static final int TUPLE_REMOVAL_THRESHOLD =1000;
    public static final String LEFT_PREDICATE_BPLUS_TREE_AND_RBS_BOLT ="leftPredicateBplusAndRedBlackTreeBolt";
    public static final String RIGHT_PREDICATE_BPLUS_TREE_AND_RBST_BOLT ="rightPredicateBPlusTreeBolt";
    public static final String HASH_SET_EVALUATION="hashSetEvaluation";

    public static final String KAFKA_SPOUT="kafkaSpout";
    public static final String SPLIT_BOLT="distributorBolt";

}
