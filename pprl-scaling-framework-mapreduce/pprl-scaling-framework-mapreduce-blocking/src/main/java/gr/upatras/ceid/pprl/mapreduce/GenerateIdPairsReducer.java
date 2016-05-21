package gr.upatras.ceid.pprl.mapreduce;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import static gr.upatras.ceid.pprl.mapreduce.BlockingKeyStatistics.setMaxMinBlockingKeysCounters;
import static gr.upatras.ceid.pprl.mapreduce.CommonUtil.increaseTotalBlockingKeyCount;
import static gr.upatras.ceid.pprl.mapreduce.CommonUtil.increaseTotalPairCounter;

/**
 * Hamming LSH Blocking Reducer class.
 */
public class GenerateIdPairsReducer extends Reducer<Text,Text,Text,Text> {

    private Queue<Text> keyQ;
    private Queue<List<Text>> valQ;
    private BlockingKeyStatistics statistics;


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        keyQ = new LinkedList<Text>();
        valQ = new LinkedList<List<Text>>();
        statistics = new BlockingKeyStatistics();
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        statistics.refreshOnBlockingGroupUpdate(
                Integer.parseInt(key.toString().substring(0, key.toString().indexOf('_')))
        );

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
                increaseTotalPairCounter(context, pairsCounter);
                statistics.increaseCurrentBlockingKeysCount();
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        final int id = context.getTaskAttemptID().getTaskID().getId();
        statistics.refresh();
        increaseTotalBlockingKeyCount(context, statistics.getTotalBlockingKeyCount());
        setMaxMinBlockingKeysCounters(context, id,
                statistics.getMaxBlockingKeys(),
                statistics.getMinBlockingKeys()
        );
    }

    /**
     * Form a values list from the iterable.
     *
     * @param values iterable.
     * @return a values list from the iterable.
     */
    private List<Text> formValuesList(final Iterable<Text> values) {
        List<Text> vlist = new ArrayList<Text>();
        for (Text v : values) vlist.add(new Text(v));
        return vlist;
    }

    /**
     * True if blocking keys are equal, false otherwise.
     *
     * @param key1 key 1.
     * @param key2 key 2.
     * @return true if blocking keys are equal, false otherwise.
     */
    private boolean equalBlockingKeys(final Text key1, final Text key2) {
        final String s1 = key1.toString();
        final String s2 = key2.toString();
        return s1.substring(0,s1.lastIndexOf("_")).equals(s2.substring(0,s2.lastIndexOf("_")));
    }
}
