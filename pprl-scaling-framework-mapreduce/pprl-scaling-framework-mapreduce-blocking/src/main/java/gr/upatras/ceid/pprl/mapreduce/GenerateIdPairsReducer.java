package gr.upatras.ceid.pprl.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

import static gr.upatras.ceid.pprl.mapreduce.BlockingKeyStatistics.setMaxMinBlockingKeysCounters;
import static gr.upatras.ceid.pprl.mapreduce.BlockingKeyWritable.sameBlockingKey;
import static gr.upatras.ceid.pprl.mapreduce.CommonUtil.increaseTotalPairCounter;

/**
 * Generate Id Pairs Reducer class.
 */
public class GenerateIdPairsReducer extends Reducer<BlockingKeyWritable,Text,Text,Text> {
    public BlockingKeyWritable previousKey;
    public TextArrayWritable previousValue;

    private BlockingKeyStatistics statistics ;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        statistics = new BlockingKeyStatistics();
        initPrevious();
    }

    @Override
    protected void reduce(BlockingKeyWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        statistics.refreshOnBlockingGroupUpdate(key.blockingGroupId);

        // if we dont set previous key set it and its value
        if(previousKey == null && previousValue == null) {
            if(key.datasetId == 'B') return;
            updatePrevious(key,values);
            return;
        }

        // if we have a previous key (only from A) and the current one is from A swap em
        if(key.datasetId == 'A') {
            previousKey = new BlockingKeyWritable(key.blockingGroupId, key.hash, key.datasetId);
            updatePrevious(key, values);
        } else { // if current one is from B and have the same blocking key (group and hash)
            if(sameBlockingKey(previousKey, key)) {
                int pairsCounter = 0;
                for (Text bv : values) {
                    for (Text av : previousValue.get()) {
                        context.write(av, bv);
                        pairsCounter++;
                    }
                }
                increaseTotalPairCounter(context, pairsCounter);
                statistics.increaseCurrentBlockingKeysCount();
            }
            initPrevious();
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        final int id = context.getTaskAttemptID().getTaskID().getId();
        statistics.refresh();
        setMaxMinBlockingKeysCounters(context, id,
                statistics.getMaxBlockingKeys(),
                statistics.getMinBlockingKeys()
        );
    }

    /**
     * Updates previous key and values with the current ones.
     *
     * @param key a blocking key.
     * @param values values of a blocking key.
     */
    private void updatePrevious(final BlockingKeyWritable key, final Iterable<Text> values) {
        previousKey = new BlockingKeyWritable(key.blockingGroupId, key.hash, key.datasetId);
        previousValue = new TextArrayWritable();
        ArrayList<Text> textList = new ArrayList<Text>();
        for (Text v : values) { textList.add(new Text(v)); }
        previousValue.set(textList.toArray(new Text[textList.size()]));

    }

    private void initPrevious() {
        previousKey = null;
        previousValue = null;
    }
}
