package gr.upatras.ceid.pprl.test;


import gr.upatras.ceid.pprl.datasets.DatasetException;
import gr.upatras.ceid.pprl.datasets.DatasetsUtil;
import gr.upatras.ceid.pprl.matching.ExpectationMaximization;
import gr.upatras.ceid.pprl.matching.NaiveExpectationMaximization;
import gr.upatras.ceid.pprl.matching.SimilarityMatrix;
import gr.upatras.ceid.pprl.matching.SimilarityUtil;
import gr.upatras.ceid.pprl.matching.SimilarityVectorFrequencies;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ExpectationMaximazationTest {

    private static Logger LOG = LoggerFactory.getLogger(ExpectationMaximazationTest.class);
    private String[] fieldNames = {"name","surname","location"};
    private GenericRecord[] records;

    @Before
    public void setup() throws IOException, DatasetException {
        FileSystem fs = FileSystem.getLocal(new Configuration());
        Schema schema = DatasetsUtil.loadSchemaFromFSPath(fs, new Path("data/person_small/schema/person_small.avsc"));
        records = DatasetsUtil.loadAvroRecordsFromFSPaths(fs, schema, new Path("data/person_small/avro/person_small.avro"));
    }

    @Test
    public void test1() throws IOException {
        DescriptiveStatistics stats = new DescriptiveStatistics();
        SimilarityMatrix matrix = null;
        NaiveExpectationMaximization estimator = null;
        for (int i = 0; i < 5; i++) {
            long start = System.nanoTime();
            matrix = SimilarityUtil.matrix(records, fieldNames);
            estimator = new NaiveExpectationMaximization (fieldNames,0.9,0.1,0.01);
            estimator.runAlgorithm(matrix);
            long end = System.nanoTime();
            long time = end - start;
            stats.addValue(time);
        }
        LOG.info("matrix={} , estimator={}",matrix,estimator);
        LOG.info("Took {} ns.",stats.getPercentile(50));
    }

    @Test
    public void test2() throws IOException {
        DescriptiveStatistics stats = new DescriptiveStatistics();
        SimilarityVectorFrequencies frequencies = null;
        ExpectationMaximization estimator = null;
        for (int i = 0; i < 5; i++) {
            long start = System.nanoTime();
            frequencies = SimilarityUtil.vectorFrequencies(records, fieldNames);
            estimator = new ExpectationMaximization (fieldNames,0.9,0.1,0.01);
            estimator.runAlgorithm(frequencies);
            long end = System.nanoTime();
            long time = end - start;
            stats.addValue(time);
        }
        LOG.info("matrix={} , estimator={}",frequencies,estimator);
        LOG.info("Took {} ns.",stats.getPercentile(50));
    }
}
