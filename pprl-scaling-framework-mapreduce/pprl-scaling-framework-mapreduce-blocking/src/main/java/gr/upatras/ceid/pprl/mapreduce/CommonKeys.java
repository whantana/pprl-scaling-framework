package gr.upatras.ceid.pprl.mapreduce;

/**
 * Common keys on configuration.
 */
public class CommonKeys {
    public static final String FREQUENT_PAIR_LIMIT = "frequent.pair.limit";
    public static final String BLOCKING_GROUP_COUNT = "blocking.groups.count";

    public static final String COUNTER_GROUP_NAME = "PPRL Blocking Counters";
    public static final String TOTAL_PAIR_COUNTER = "total.pairs.count";
    public static final String TOTAL_BLOCKING_KEYS_COUNTER = "total.blockingkeys.count";
    public static final String MAX_BLOCKING_KEYS_IN_ANY_GROUP_COUNTER = "max.blockingkeys.at.any.group";
    public static final String MIN_BLOCKING_KEYS_IN_ANY_GROUP_COUNTER = "min.blockingkeys.at.any.group";
    public static final String FREQUENT_PAIR_COUNTER = "frequent.pairs.count";
    public static final String MATCHED_PAIR_COUNTER = "matched.pairs.count";
    public static final String RECORD_COUNT_COUNTER = "record.count";
    public static final String ALICE_RECORD_COUNT_COUNTER = "record.count.A";
    public static final String BOB_RECORD_COUNT_COUNTER = "record.count.B";
    public static final String TOTAL_BYTES_ON_PPRL = "mem.total.bytes";

    public static final String ALICE_SCHEMA = "alice.encoding.schema";
    public static final String BOB_SCHEMA = "bob.encoding.schema";
    public static final String ALICE_UID = "alice.uid.field.name";
    public static final String BOB_UID = "bob.uid.field.name";

    public static final String BLOCKING_KEYS = "blocking.keys";
    public static final String HAMMING_THRESHOLD = "hamming.threshold";
    public static final String BUCKET_INITIAL_CAPACITY = "blocking.bucket.capacity";
    public static final String BOB_DATA_PATH = "bob.avro.path";
}
