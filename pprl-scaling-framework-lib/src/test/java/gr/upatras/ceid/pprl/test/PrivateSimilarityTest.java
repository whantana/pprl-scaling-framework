package gr.upatras.ceid.pprl.test;

import gr.upatras.ceid.pprl.datasets.DatasetException;
import gr.upatras.ceid.pprl.datasets.DatasetsUtil;
import gr.upatras.ceid.pprl.encoding.BloomFilter;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncodingException;
import gr.upatras.ceid.pprl.encoding.CLKEncoding;
import gr.upatras.ceid.pprl.matching.PrivateSimilarityUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class PrivateSimilarityTest {

    private static Logger LOG = LoggerFactory.getLogger(PrivateSimilarityTest.class);

    private static int N = 100;


    @Test
    public void test1() {
        byte[] bytes = new byte[(int) Math.ceil(N/(double)8)];
        bytes[0] = 1;
        bytes[1] = 1;
        bytes[2] = 4;
        LOG.info("BF1 : {}",DatasetsUtil.prettyBinary(bytes));
        final BloomFilter bf1 = new BloomFilter(N,bytes);
        LOG.info("BF2 : {}",DatasetsUtil.prettyBinary(bytes));
        final BloomFilter bf2 = new BloomFilter(N,bytes);

        LOG.info("Hamming : {} ", PrivateSimilarityUtil.hamming(bf1, bf2));
        LOG.info("Hamming1 : {} ",PrivateSimilarityUtil.hamming1(bf1, bf2));
        LOG.info("Jaccard : {} ",PrivateSimilarityUtil.jaccard(bf1, bf2));
        LOG.info("Dice : {} ",PrivateSimilarityUtil.dice(bf1, bf2));
    }

    @Test
    public void test2() {
        byte[] bytes = new byte[(int) Math.ceil(N/(double)8)];
        bytes[0] = 1;
        bytes[1] = 1;
        bytes[2] = 4;
        LOG.info("BF1 : {}",DatasetsUtil.prettyBinary(bytes));
        final BloomFilter bf1 = new BloomFilter(N,bytes);

        bytes = new byte[(int) Math.ceil(N/(double)8)];
        bytes[0] = 1;
        bytes[1] = 1;
        bytes[2] = 5;
        LOG.info("BF2 : {}",DatasetsUtil.prettyBinary(bytes));
        final BloomFilter bf2 = new BloomFilter(N,bytes);


        LOG.info("Hamming : {} ", PrivateSimilarityUtil.hamming(bf1, bf2));
        LOG.info("Hamming1 : {} ",PrivateSimilarityUtil.hamming1(bf1, bf2));
        LOG.info("Jaccard : {} ",PrivateSimilarityUtil.jaccard(bf1, bf2));
        LOG.info("Dice : {} ",PrivateSimilarityUtil.dice(bf1,bf2));
    }

    @Test
    public void test3() {
        byte[] bytes = new byte[(int) Math.ceil(N/(double)8)];
        bytes[0] = 1;
        bytes[1] = 1;
        bytes[2] = 4;
        LOG.info("BF1 : {}",DatasetsUtil.prettyBinary(bytes));
        final BloomFilter bf1 = new BloomFilter(N,bytes);

        bytes = new byte[(int) Math.ceil(N/(double)8)];
        bytes[0] = 0;
        bytes[1] = 0;
        bytes[2] = 4;
        LOG.info("BF2 : {}",DatasetsUtil.prettyBinary(bytes));
        final BloomFilter bf2 = new BloomFilter(N,bytes);


        LOG.info("Hamming : {} ", PrivateSimilarityUtil.hamming(bf1, bf2));
        LOG.info("Hamming1 : {} ",PrivateSimilarityUtil.hamming1(bf1, bf2));
        LOG.info("Jaccard : {} ",PrivateSimilarityUtil.jaccard(bf1, bf2));
        LOG.info("Dice : {} ",PrivateSimilarityUtil.dice(bf1,bf2));
    }

    @Test
    public void test4() throws IOException, DatasetException, BloomFilterEncodingException {
        final FileSystem fs = FileSystem.getLocal(new Configuration());
        final String[] SELECTED_FIELDS = {"surname","name","address","city"};
        final String[] REST_FIELDS = {"id"};
        final Schema schemaVotersA =
                DatasetsUtil.loadSchemaFromFSPath(fs,new Path("data/voters_a/schema/voters_a.avsc"));
        final GenericRecord[] recordsA =
                DatasetsUtil.loadAvroRecordsFromFSPaths(
                        fs,20,schemaVotersA,new Path("data/voters_a/avro/voters_a.avro"));
        final Schema schemaVotersB =
                DatasetsUtil.loadSchemaFromFSPath(fs,new Path("data/voters_b/schema/voters_b.avsc"));
        final GenericRecord[] recordsB =
                DatasetsUtil.loadAvroRecordsFromFSPaths(
                        fs,1,schemaVotersA,new Path("data/voters_b/avro/voters_b.avro"));
        assert recordsB.length == 1;
        final GenericRecord recordB = recordsB[0];

        final CLKEncoding encodingA = new CLKEncoding(1024,10,2);
        encodingA.makeFromSchema(schemaVotersA, SELECTED_FIELDS, REST_FIELDS);
        encodingA.initialize();
        final GenericRecord[] encodedRecordsA = new GenericRecord[recordsA.length];
        for (int i = 0 ; i < recordsA.length; i++)
            encodedRecordsA[i] = encodingA.encodeRecord(recordsA[i]);

        final CLKEncoding encodingB = new CLKEncoding(1024,10,2);
        encodingB.makeFromSchema(schemaVotersB, SELECTED_FIELDS, REST_FIELDS);
        encodingB.initialize();
        final GenericRecord encodedRecordB = encodingB.encodeRecord(recordB);

        for (GenericRecord encodedRecordA : encodedRecordsA) {
            LOG.info("hamming({}) = {}",
                    String.format("%s,%s", encodedRecordA.get("id"), encodedRecordB.get("id")),
                    PrivateSimilarityUtil.hamming(
                            encodingA.retrieveBloomFilter(encodedRecordA),
                            encodingB.retrieveBloomFilter(encodedRecordB)
                    )
            );
            LOG.info("hamming1({}) = {}",
                    String.format("%s,%s", encodedRecordA.get("id"), encodedRecordB.get("id")),
                    PrivateSimilarityUtil.hamming1(
                            encodingA.retrieveBloomFilter(encodedRecordA),
                            encodingB.retrieveBloomFilter(encodedRecordB)
                    )
            );
            LOG.info("jaccard({}) = {}",
                    String.format("%s,%s",encodedRecordA.get("id"),encodedRecordB.get("id")),
                    PrivateSimilarityUtil.jaccard(
                            encodingA.retrieveBloomFilter(encodedRecordA),
                            encodingB.retrieveBloomFilter(encodedRecordB)
                    )
            );
        }
    }
}
