package gr.upatras.ceid.pprl.mapreduce;

import gr.upatras.ceid.pprl.encoding.BloomFilter;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncoding;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncodingException;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncodingUtil;
import gr.upatras.ceid.pprl.matching.PrivateSimilarityUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

import static gr.upatras.ceid.pprl.mapreduce.CommonUtil.*;

/**
 * Private Similarity Reducer class.
 */
public class PrivateSimilarityReducer extends Reducer<TextPairWritable,AvroValue<GenericRecord>,Text,Text> {

    private double similarityThreshold;
    private String similarityMethodName;

    private Schema aliceEncodingSchema;
    private Schema bobEncodingSchema;

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
            aliceEncodingSchema = aliceEncoding.getEncodingSchema();

            BloomFilterEncoding bobEncoding = BloomFilterEncodingUtil.setupNewInstance(
                    ((new Schema.Parser()).parse(bobSchemaString)));
            bobEncodingFieldName = bobEncoding.getEncodingFieldName();
            bobEncodingSchema = bobEncoding.getEncodingSchema();
            if(bobEncoding.getBFN() != aliceEncoding.getBFN())
                throw new IllegalStateException("Encoding schemes dont have same bloom filter size.");
            N = aliceEncoding.getBFN();
        } catch (BloomFilterEncodingException e) {
            throw new InterruptedException(e.getMessage());
        }
        similarityMethodName = "hamming";
        similarityThreshold = (double) context.getConfiguration().getInt(CommonKeys.HAMMING_THRESHOLD, 100);
        matchedPairsCount = 0;
    }

    @Override
    protected void reduce(TextPairWritable key, Iterable<AvroValue<GenericRecord>> values, Context context)
            throws IOException, InterruptedException {
        GenericRecord aliceRecord = null;
        GenericRecord bobRecord = null;

        for (AvroValue<GenericRecord> v : values) {
            final GenericRecord record = v.datum();
            if(record.getSchema().getName().equals(aliceEncodingSchema.getName()))
                aliceRecord = new GenericData.Record((GenericData.Record)record,true);
            else if(record.getSchema().getName().equals(bobEncodingSchema.getName()))
                bobRecord = new GenericData.Record((GenericData.Record)record,true);
        }

        if(aliceRecord == null || bobRecord == null)
            throw new InterruptedException("Missing record from pair." +
                    " alice found ? " +  (aliceRecord != null) +
                    " bob found ? " + (bobRecord != null));

        final BloomFilter aliceBf = BloomFilterEncodingUtil.retrieveBloomFilter(aliceRecord,
                aliceEncodingFieldName, N);
        final BloomFilter bobBf = BloomFilterEncodingUtil.retrieveBloomFilter(bobRecord,
                bobEncodingFieldName, N);

        final boolean matches =
                PrivateSimilarityUtil.similarity(similarityMethodName,aliceBf,bobBf,similarityThreshold);

        if(matches) {
            context.write(
                    new Text(String.valueOf(aliceRecord.get(aliceUidFieldname))),
                    new Text(String.valueOf(bobRecord.get(bobUidFieldname)))
            );
            matchedPairsCount++;
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        increaseMatchedPairsCounter(context, matchedPairsCount);
    }
}

