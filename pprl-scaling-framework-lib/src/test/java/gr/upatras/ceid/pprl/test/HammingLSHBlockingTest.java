package gr.upatras.ceid.pprl.test;


import gr.upatras.ceid.pprl.blocking.BlockingException;
import gr.upatras.ceid.pprl.blocking.BlockingUtil;
import gr.upatras.ceid.pprl.blocking.HammingLSHBlocking;
import gr.upatras.ceid.pprl.datasets.DatasetException;
import gr.upatras.ceid.pprl.datasets.DatasetsUtil;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncoding;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncodingException;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncodingUtil;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class HammingLSHBlockingTest {
    private static Logger LOG = LoggerFactory.getLogger(HammingLSHBlockingTest.class);

    final String[] ENCODING_NAMES = {
            "clk",
            "static_fbf","dynamic_fbf",
            "uniform_rbf_static_fbf","uniform_rbf_dynamic_fbf",
            "weighted_rbf_static_fbf","weighted_rbf_dynamic_fbf"
    };
    @Test
    public void test0()
            throws IOException, DatasetException,
            BloomFilterEncodingException, BlockingException {
        final FileSystem fs = FileSystem.getLocal(new Configuration());
        for (String encName : ENCODING_NAMES) {
            LOG.info("Working with " + encName );
            BloomFilterEncoding encodingA = BloomFilterEncodingUtil.setupNewInstance(
                    DatasetsUtil.loadSchemaFromFSPath(fs, new Path("data/voters_a/"+ encName +".avsc")));
            BloomFilterEncoding encodingB = BloomFilterEncodingUtil.setupNewInstance(
                    DatasetsUtil.loadSchemaFromFSPath(fs, new Path("data/voters_b/"+ encName +".avsc")));

            final GenericRecord[] recordsA = DatasetsUtil.loadAvroRecordsFromFSPaths(fs, encodingA.getEncodingSchema(),
                    new Path("data/voters_a/"+ encName +".avro"));
            final GenericRecord[] recordsB = DatasetsUtil.loadAvroRecordsFromFSPaths(fs, encodingB.getEncodingSchema(),
                    new Path("data/voters_b/"+ encName +".avro"));

            final int LC = 36;
            final int K = 5;
			final String similarityMethodName = "jaccard";
            final double similarityThreshold = 0.7;
            final short C = 5;
            final HammingLSHBlocking blocking = new HammingLSHBlocking(LC, K, encodingA, encodingB);

            blocking.initialize();
            final HammingLSHBlocking.HammingLSHBlockingResult result =
                    blocking.runFPS(recordsA, "id", recordsB, "id", C, similarityMethodName, similarityThreshold);
            LOG.info("Matched pairs list size : {}", result.getMatchedPairsCount());
            LOG.info("Frequent pairs list size : {}", result.getFrequentPairsCount());
            final Path blockingOutputPath = new Path("data/blocking_" + encName + "_voters_a_voters_b.pairs");
            LOG.info("Saving at : {}", blockingOutputPath);
            BlockingUtil.saveBlockingResult(FileSystem.get(new Configuration()),blockingOutputPath,result);
        }
    }
}
