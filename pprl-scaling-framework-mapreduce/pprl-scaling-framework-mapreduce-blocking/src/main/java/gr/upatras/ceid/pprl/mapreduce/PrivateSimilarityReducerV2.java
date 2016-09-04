package gr.upatras.ceid.pprl.mapreduce;

import gr.upatras.ceid.pprl.datasets.DatasetsUtil;
import gr.upatras.ceid.pprl.encoding.BloomFilter;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncoding;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncodingException;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncodingUtil;
import gr.upatras.ceid.pprl.matching.PrivateSimilarityUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import static gr.upatras.ceid.pprl.mapreduce.CommonUtil.*;

/**
 * Private Similarity Reducer class (used in V2).
 */
public class PrivateSimilarityReducerV2 extends Reducer<AvroKey<GenericRecord>,Text,Text,Text> {
    private double similarityThreshold;
    private String similarityMethodName;

    private GenericRecord[] bobRecords;
    private Map<String,Integer> bobId2IndexMap;

    private String aliceUidFieldname;
    private String aliceEncodingFieldName;
    private String bobUidFieldname;
    private String bobEncodingFieldName;

    private int N;

    private long matchedPairsCount;


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        final String aliceSchemaString = context.getConfiguration().get(CommonKeys.ALICE_SCHEMA);
        if (aliceSchemaString == null) throw new IllegalStateException("Alice schema not set.");
        final String bobSchemaString = context.getConfiguration().get(CommonKeys.BOB_SCHEMA);
        if (bobSchemaString == null) throw new IllegalStateException("Bob schema not set.");
        aliceUidFieldname = context.getConfiguration().get(CommonKeys.ALICE_UID);
        if (aliceUidFieldname == null) throw new IllegalStateException("Alice uid not set.");
        bobUidFieldname = context.getConfiguration().get(CommonKeys.BOB_UID);
        if (bobUidFieldname == null) throw new IllegalStateException("Bob uid not set.");
        try {
            BloomFilterEncoding aliceEncoding = BloomFilterEncodingUtil.setupNewInstance(
                    ((new Schema.Parser()).parse(aliceSchemaString)));
            aliceEncodingFieldName = aliceEncoding.getEncodingFieldName();

            BloomFilterEncoding bobEncoding = BloomFilterEncodingUtil.setupNewInstance(
                    ((new Schema.Parser()).parse(bobSchemaString)));
            bobEncodingFieldName = bobEncoding.getEncodingFieldName();
            if(bobEncoding.getBFN() != aliceEncoding.getBFN())
                throw new IllegalStateException("Encoding schemes dont have same bloom filter size.");
            N = aliceEncoding.getBFN();
        } catch (BloomFilterEncodingException e) {
            throw new InterruptedException(e.getMessage());
        }
        similarityMethodName = "hamming";
        similarityThreshold = (double) context.getConfiguration().getInt(CommonKeys.HAMMING_THRESHOLD, 100);
        matchedPairsCount = 0;
        try {
            loadBobRecords(context);
        } catch (URISyntaxException e) {
            throw new InterruptedException(e.getMessage());
        }
    }

    @Override
    protected void reduce(AvroKey<GenericRecord> key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        GenericRecord aliceRecord = key.datum();

        for (Text v : values) {
            final GenericRecord bobRecord = bobRecords[bobId2IndexMap.get(v.toString())];
            final BloomFilter aliceBf = BloomFilterEncodingUtil.retrieveBloomFilter(aliceRecord,
                    aliceEncodingFieldName, N);
            final BloomFilter bobBf = BloomFilterEncodingUtil.retrieveBloomFilter(bobRecord,
                    bobEncodingFieldName, N);
            final boolean matches =
                    PrivateSimilarityUtil.similarity(similarityMethodName, aliceBf, bobBf, similarityThreshold);
            if (matches) {
                context.write(
                        new Text(String.valueOf(aliceRecord.get(aliceUidFieldname))),
                        new Text(String.valueOf(bobRecord.get(bobUidFieldname)))
                );
                matchedPairsCount++;
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        increaseMatchedPairsCounter(context, matchedPairsCount);
    }

    /**
     * Load bob's records.
     *
     * @param context context
     * @throws IOException
     */
    private void loadBobRecords(Reducer.Context context) throws URISyntaxException, IOException {
        final String bobSchemaString = context.getConfiguration().get(CommonKeys.BOB_SCHEMA);
        if (bobSchemaString == null) throw new IllegalStateException("Bob schema not set.");
        final Schema bobSchema = (new Schema.Parser()).parse(bobSchemaString);
        final String bobAvroPathUri = context.getConfiguration().get(CommonKeys.BOB_DATA_PATH,null);
        if (bobAvroPathUri == null) throw new IllegalStateException("Bob avro path not set.");
        final Path bobAvroPath = new Path(new URI(bobAvroPathUri));

        final int bobRecordCount = context.getConfiguration().getInt(CommonKeys.BOB_RECORD_COUNT_COUNTER, -1);
        if(bobRecordCount < 0) throw new IllegalStateException("Bob record count not set.");

        String bobUidFieldName = context.getConfiguration().get(CommonKeys.BOB_UID, null);
        if (bobUidFieldName == null) throw new IllegalStateException("Bob uid not set.");

        final FileSystem fs = FileSystem.get(context.getConfiguration());
        final DatasetsUtil.DatasetRecordReader reader =
                new DatasetsUtil.DatasetRecordReader(fs,bobSchema,bobAvroPath);
        int i = 0;
        bobRecords = new GenericRecord[bobRecordCount];
        bobId2IndexMap = new HashMap<String, Integer>((int)(bobRecordCount/0.75f + 1),0.75f);
        try {
            while (reader.hasNext()) {
                bobRecords[i] = reader.next();
                bobId2IndexMap.put(String.valueOf(bobRecords[i].get(bobUidFieldName)), i);
                i++;
            }
        } finally {
            reader.close();
        }
    }
}
