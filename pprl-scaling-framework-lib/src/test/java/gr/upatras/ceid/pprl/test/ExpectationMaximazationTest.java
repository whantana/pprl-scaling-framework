package gr.upatras.ceid.pprl.test;


import gr.upatras.ceid.pprl.datasets.DatasetException;
import gr.upatras.ceid.pprl.datasets.DatasetsUtil;
import gr.upatras.ceid.pprl.matching.ExpectationMaximization;
import gr.upatras.ceid.pprl.matching.SimilarityUtil;
import gr.upatras.ceid.pprl.matching.SimilarityVectorFrequencies;
import gr.upatras.ceid.pprl.matching.NaiveExpectationMaximization;
import gr.upatras.ceid.pprl.matching.SimilarityMatrix;
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
    private Schema schema;
    private FileSystem fs;


    @Before
    public void setup() throws IOException, DatasetException {
        fs = FileSystem.getLocal(new Configuration());
        schema = DatasetsUtil.loadSchemaFromFSPath(fs,new Path("data/person_small/schema/person_small.avsc"));
        records = DatasetsUtil.loadAvroRecordsFromFiles(fs,schema, new Path[]{
                new Path("data/person_small/avro/person_small.avro")});
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

    @Test
    public void test3() throws IOException {
        DescriptiveStatistics stats =  new DescriptiveStatistics();
        SimilarityMatrix matrix = null;
        SimilarityVectorFrequencies frequencies = null;
        int iterations = 10;
        for (int i = 1; i < 5; i++) {
            GenericRecord[] bigRecords = new GenericRecord[i*records.length];
            for (int j = 0 ; j < i ; j++)
                System.arraycopy(records, 0, bigRecords, j * records.length, records.length);
            stats.clear();
            for (int it = 0; it < iterations; it++) {
                long start = System.nanoTime();
                matrix = SimilarityUtil.matrix(bigRecords, fieldNames);
                long stop = System.nanoTime();
                long time = stop - start;
                stats.addValue(time);
            }
			LOG.info("--Person_small x {}--",i);
            assert matrix != null;
            LOG.info(matrix.toString());

            LOG.info(String.format("Naive similarity matrix records[%d,%d] time %.2f ns",
                    bigRecords.length, fieldNames.length, stats.getPercentile(50)));

            stats.clear();
            for (int it = 0; it < iterations; it++) {
                long start = System.nanoTime();
                frequencies = SimilarityUtil.vectorFrequencies(bigRecords, fieldNames);
                long stop = System.nanoTime();
                long time = stop - start;
                stats.addValue(time);
            }
            assert frequencies != null;
            LOG.info(frequencies.toString());

			LOG.info(String.format("Similarity matrix records[%d,%d] time %.2f ns",
                    bigRecords.length, fieldNames.length, stats.getPercentile(50)));

            stats.clear();
            NaiveExpectationMaximization naiveEstimator  = new NaiveExpectationMaximization(fieldNames,0.9,0.1,0.01);
            for (int it = 0; it < iterations; it++) {
				naiveEstimator = new NaiveExpectationMaximization(fieldNames,0.9,0.1,0.01);
                long start = System.nanoTime();
                naiveEstimator.runAlgorithm(matrix);
                long stop = System.nanoTime();
                long time = stop - start;
                stats.addValue(time);
            }
			LOG.info(naiveEstimator.toString());
            LOG.info(String.format("Naive Expectation Maximation on records[%d,%d] time %.2f ns",
                    bigRecords.length, fieldNames.length, stats.getPercentile(50)));

            stats.clear();
			ExpectationMaximization estimator = new ExpectationMaximization(fieldNames,0.9,0.1,0.01);
            for (int it = 0; it < iterations; it++) {
                estimator = new ExpectationMaximization(fieldNames,0.9,0.1,0.01);
                long start = System.nanoTime();
                estimator.runAlgorithm(frequencies);
                long stop = System.nanoTime();
                long time = stop - start;
                stats.addValue(time);
            }
			LOG.info(estimator.toString());
            LOG.info(String.format("Expectation Maximation matrix records[%d,%d] time %.2f ns",
                    bigRecords.length, fieldNames.length, stats.getPercentile(50)));
        }
    }
}
