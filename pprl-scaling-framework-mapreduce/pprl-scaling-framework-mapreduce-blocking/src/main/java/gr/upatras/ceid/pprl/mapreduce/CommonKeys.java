package gr.upatras.ceid.pprl.mapreduce;

/**
 * Common keys on configuration.
 */
public class CommonKeys {
    public static final String FREQUENT_PAIR_LIMIT_KEY = "frequent.pair.limit";
    public static final String BLOCKING_GROUP_COUNT_KEY = "blocking.groups.count";

    public static final String RECORD_PAIR_DELIMITER = "_#_";

    public static final String COUNTER_GROUP_NAME = "HammingLSHBlocking Counters";
    public static final String TOTAL_PAIR_COUNTER_KEY = "total.pairs";
    public static final String PAIR_PER_BLOCKING_GROUP_COUNTER_KEY = "group.pairs";

    public static final String ALICE_SCHEMA_KEY = "alice.encoding.schema";
    public static final String BOB_SCHEMA_KEY = "bob.encoding.schema";

    public static final String ALICE_UID_KEY = "alice.uid.field.name";
    public static final String BOB_UID_KEY = "bob.uid.field.name";

    public static final String BLOCKING_KEYS_KEY = "blocking.keys";

    public static final String SIMILARITY_METHOD_NAME_KEY = "similarity.method.name";
    public static final String SIMILARITY_THRESHOLD_KEY = "similarity.threshold";
}
