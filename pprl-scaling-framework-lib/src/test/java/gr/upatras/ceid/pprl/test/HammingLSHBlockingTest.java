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

//    final String[] ENCODING_NAMES = {
//            "clk",
//            "fbf_s","fbf_d",
//            "rbf_us","rbf_ud",
//            "rbf_ws","rbf_wd"
//    };
    final String[] ENCODING_NAMES = {
            "clk",
    };

    private static final String SIMILARITY_METHOD_NAME = "hamming";
    private static final double SIMILAIRTY_THRESHOLD = 120;
    private static final int HAMMING_LSH_K = 30;
    private static final double[] DELTAS = {0.01,0.005,0.001,0.0005};


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

            double ptheta = HammingLSHBlockingUtil.probOfBaseHashMatch((int)SIMILAIRTY_THRESHOLD,encodingA.getBFN());
            double pthetaK = HammingLSHBlockingUtil.probHashMatch(ptheta,HAMMING_LSH_K);

            for(int i = 0 ; i < DELTAS.length; i++) {
                double delta = DELTAS[i];
                final int[] limits = HammingLSHBlockingUtil.optimalBlockingGroupCountLimits(delta, pthetaK);
                LOG.info("Limits = {}", Arrays.toString(limits));
                final int Lopt = limits[0];
                final int Lc = limits[1];
                final short C = HammingLSHBlockingUtil.frequentPairLimit(Lopt, pthetaK);
                final int L = HammingLSHBlockingUtil.optimalBlockingGroupCountIter(delta,pthetaK);
                LOG.info("Pr[C < {} ]= {}",C,String.format("%.5f",HammingLSHBlockingUtil.cdf(Lopt,pthetaK,C)));
                LOG.info("Pr[C < {} ]= {}",C,String.format("%.5f",HammingLSHBlockingUtil.cdf(L,pthetaK,C)));
                LOG.info("Pr[C < {} ]= {}",C,String.format("%.5f",HammingLSHBlockingUtil.cdf(Lc,pthetaK,C)));
                LOG.info("Hamming LSH/FPS with {}",
                        String.format("[Lopt,Lc] = %s, L = %d , C = %d", Arrays.toString(limits), L, C));

                final HammingLSHBlocking blocking = new HammingLSHBlocking(L, HAMMING_LSH_K, encodingA, encodingB);

                blocking.initialize(recordsB);
                final HammingLSHBlockingResult result =
                        blocking.runFPS(recordsA, "id", recordsB, "id", C, SIMILARITY_METHOD_NAME, SIMILAIRTY_THRESHOLD);
                final Path blockingOutputPath = new Path("data/blocking_"+ i + "_" + encName + "_voters_a_voters_b.result");
                LOG.info("Saving at : {}", blockingOutputPath);
                HammingLSHBlockingResult.saveBlockingResult(FileSystem.get(new Configuration()), blockingOutputPath, result);
            }
        }
    }
}
