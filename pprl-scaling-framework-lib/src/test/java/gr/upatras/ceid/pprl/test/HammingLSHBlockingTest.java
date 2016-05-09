package gr.upatras.ceid.pprl.test;


import gr.upatras.ceid.pprl.blocking.BlockingException;
import gr.upatras.ceid.pprl.blocking.HammingLSHBlocking;
import gr.upatras.ceid.pprl.blocking.RecordIdPair;
import gr.upatras.ceid.pprl.datasets.DatasetException;
import gr.upatras.ceid.pprl.datasets.DatasetsUtil;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncoding;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncodingException;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncodingUtil;
import gr.upatras.ceid.pprl.encoding.CLKEncoding;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.AvroFSInput;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collections;
import java.util.Set;

public class HammingLSHBlockingTest {
    private static Logger LOG = LoggerFactory.getLogger(HammingLSHBlockingTest.class);

    @Test
    public void test0()
            throws IOException, DatasetException,
            BloomFilterEncodingException, BlockingException {
        final FileSystem fs = FileSystem.getLocal(new Configuration());
        BloomFilterEncoding encodingA = BloomFilterEncodingUtil.setupNewInstance(
                DatasetsUtil.loadSchemaFromFSPath(fs, new Path("data/voters_a/clk.avsc")));
        BloomFilterEncoding encodingB = BloomFilterEncodingUtil.setupNewInstance(
                DatasetsUtil.loadSchemaFromFSPath(fs, new Path("data/voters_b/clk.avsc")));

        final GenericRecord[] recordsA = DatasetsUtil.loadAvroRecordsFromFSPaths(fs, encodingA.getEncodingSchema(),
                new Path("data/voters_a/clk.avro"));
        final GenericRecord[] recordsB = DatasetsUtil.loadAvroRecordsFromFSPaths(fs,encodingB.getEncodingSchema(),
                new Path("data/voters_b/clk.avro"));

        final int LC = 36;
        final int K = 5;
        final int hammingThreshold = 100;
        final int C = 5;
        final HammingLSHBlocking blocking = new HammingLSHBlocking(LC,K,encodingA,encodingB);

        blocking.initialize();
        final HammingLSHBlocking.HammingLSHBlockingResult result =
                blocking.runFPS(recordsA,recordsB,C,hammingThreshold);
        LOG.info("Matched pairs list size : {}", result.getMatchedPairsCount());
        LOG.info("Frequent pairs list size : {}", result.getFrequentPairsCount());
    }
}
