package gr.upatras.ceid.pprl.test;


import gr.upatras.ceid.pprl.blocking.BlockingException;
import gr.upatras.ceid.pprl.blocking.HammingLSHBlocking;
import gr.upatras.ceid.pprl.blocking.HammingLSHBlockingResult;
import gr.upatras.ceid.pprl.blocking.HammingLSHBlockingUtil;
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
import java.util.Arrays;

public class HammingLSHBlockingTest {
    private static Logger LOG = LoggerFactory.getLogger(HammingLSHBlockingTest.class);

    final String[] ENCODING_NAMES = {
                "clk",
                "static_fbf",
                "dynamic_fbf",
                "uniform_rbf_static_fbf",
                "uniform_rbf_dynamic_fbf",
                "weighted_rbf_static_fbf",
                "weighted_rbf_dynamic_fbf"
        };

    private static final int HAMMING_THRESHOLD = 120;
    private static final int HAMMING_LSH_K = 30;
    private static final double[] DELTAS = {0.01,0.001};


    @Test
    public void test1()
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


            for(int i = 0 ; i < DELTAS.length; i++) {
                double delta = DELTAS[i];
                final int[] fpsParams =
                        HammingLSHBlockingUtil.optimalParameters(HAMMING_THRESHOLD,encodingA.getBFN(),delta,HAMMING_LSH_K);
                final short C = (short) fpsParams[0];
                final int L = fpsParams[1];
                final int Lopt = fpsParams[2];
                final int Lc = fpsParams[3];
                LOG.info("Hamming LSH/FPS with {}", String.format("L = %d , C = %d , [Lopt,Lc] = %s, ",
                        L, C,Arrays.toString(new int[]{Lopt,Lc})));

                final HammingLSHBlocking blocking = new HammingLSHBlocking(L, HAMMING_LSH_K, encodingA, encodingB);

                blocking.runHLSH(recordsB, "id");
                blocking.runFPS(recordsA, "id", C, HAMMING_THRESHOLD);

                final HammingLSHBlockingResult result = blocking.getResult();
                final Path blockingOutputPath = new Path("data/blocking_"+ i + "_" + encName + "_voters_a_voters_b.result");
                LOG.info("Saving at : {}", blockingOutputPath);
                HammingLSHBlockingResult.saveBlockingResult(FileSystem.get(new Configuration()), blockingOutputPath, result);
            }
        }
    }
}
