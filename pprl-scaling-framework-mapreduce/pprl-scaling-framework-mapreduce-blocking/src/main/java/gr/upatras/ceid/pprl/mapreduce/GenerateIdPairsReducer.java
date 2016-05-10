package gr.upatras.ceid.pprl.mapreduce;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

/**
 * Hamming LSH Blocking Reducer class.
 */
public class GenerateIdPairsReducer extends Reducer<Text,Text,Text,Text> {

    public static final String COUNTER_GROUP_NAME = "HammingLSHBlocking Counters";
    public static final String TOTAL_PAIR_COUNTER_KEY = "total.pairs";
    public static final String PAIR_PER_BLOCKING_GROUP_COUNTER_KEY = "group.pairs";

    private final Queue<Text> keyQ = new LinkedList<Text>();
    private final Queue<List<Text>> valQ = new LinkedList<List<Text>>();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        if(keyQ.isEmpty()) {
            if(key.toString().endsWith("_B")) return;
            keyQ.add(key);
            valQ.add(formValuesList(values));
            return;
        } else if(keyQ.size() == 2 && valQ.size() == 2) {
            keyQ.remove();
            valQ.remove();
        }

        final boolean isAliceIds = key.toString().endsWith("_A");
        final boolean isBobIds = key.toString().endsWith("_B");
        assert isAliceIds != isBobIds;

        if(isAliceIds) {
            keyQ.add(key);
            valQ.add(formValuesList(values));
        } else {
            Text previousKey = keyQ.remove();
            List<Text> previousValues = valQ.remove();
            if(!previousKey.toString().endsWith("_A"))
                throw new IllegalStateException("This must be an Alice entry.");
            if(equalBlockingKeys(previousKey,key)) {
                int pairsCounter = 0;
                for (Text bv : values) {
                    for (Text av : previousValues) {
                        context.write(av, bv);
                        pairsCounter++;
                    }
                }
                increaseTotalPairCounter(context,pairsCounter);
                increacePerBlockingGroupCounter(key,context,pairsCounter);
            }
        }
    }

    private static List<Text> formValuesList(final Iterable<Text> values) {
        List<Text> vlist = new ArrayList<Text>();
        for (Text v : values) vlist.add(new Text(v));
        return vlist;
    }

    private static boolean equalBlockingKeys(final Text t1, final Text t2) {
        final String s1 = t1.toString();
        final String s2 = t2.toString();
        return s1.substring(0,s1.lastIndexOf("_")).equals(s2.substring(0,s2.lastIndexOf("_")));
    }

    private static void increacePerBlockingGroupCounter(final Text t, final Context context, long value) {
        final String s = t.toString().substring(0,t.toString().indexOf('_'));
        context.getCounter(COUNTER_GROUP_NAME,s + "." + PAIR_PER_BLOCKING_GROUP_COUNTER_KEY).increment(value);
    }

    private static void increaseTotalPairCounter(final Context context, long value) {
        context.getCounter(COUNTER_GROUP_NAME,TOTAL_PAIR_COUNTER_KEY).increment(value);
    }
}
