package gr.upatras.ceid.pprl.matching.test;


import gr.upatras.ceid.pprl.matching.ExpectationMaximization;
import gr.upatras.ceid.pprl.matching.SimilarityMatrix;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class ExpectationMaximazationTest {

    private static Logger LOG = LoggerFactory.getLogger(ExpectationMaximazationTest.class);

    private String[][] records = SimilarityMatrixTest.records;
    private String[] fieldNames = SimilarityMatrixTest.fieldNames;
    private NaiveSimilarityMatrix matrix1;
    private SimilarityMatrix matrix2;

    @Before
    public void setup() {
        long start = System.currentTimeMillis();
        matrix1 = NaiveSimilarityMatrix.createMatrix(records);
        long end = System.currentTimeMillis();
        long time = end - start;
        LOG.info("{} took {} ms.",matrix1,time);

        start = System.currentTimeMillis();
        matrix2 = SimilarityMatrixTest.createSimilarityMatrix(records);
        end = System.currentTimeMillis();
        time = end - start;
        LOG.info("{} took {} ms.",matrix2,time);

    }
    @Test
    public void test1() throws IOException {
        NaiveExpectationMaximization estimator = new NaiveExpectationMaximization (fieldNames,0.9,0.1,0.01);
        long start = System.currentTimeMillis();
        estimator.runAlgorithm(matrix1);
        long end= System.currentTimeMillis();
        long time = end - start;
        LOG.info(estimator.toString() + " took " + time + " ms.");
    }

    @Test
    public void test2() throws IOException {
        ExpectationMaximization estimator = new ExpectationMaximization(fieldNames,0.9,0.1,0.01);
        long start = System.currentTimeMillis();
        estimator.runAlgorithm(matrix2);
        long end= System.currentTimeMillis();
        long time = end - start;
        LOG.info(estimator.toString() + " took " + time + " ms.");
    }

    @Test
    public void test3() throws IOException {
        DescriptiveStatistics stats =  new DescriptiveStatistics();
        int iterations = 5;
        for (int i = 1; i < 5; i++) {
            String[][] bigRecords = new String[i*records.length][fieldNames.length];
            for (int j = 0 ; j < i ; j++)
                System.arraycopy(records, 0, bigRecords, j * records.length, records.length);

            stats.clear();
            for (int it = 0; it < iterations; it++) {
                long start = System.currentTimeMillis();
                matrix1 = NaiveSimilarityMatrix.createMatrix(bigRecords);
                long stop = System.currentTimeMillis();
                long time = stop - start;
                stats.addValue(time);
            }
            LOG.info(String.format("Naive similarity matrix records[%d,%d] time %.2f ms",
                    bigRecords.length, fieldNames.length, stats.getPercentile(50)));

            stats.clear();
            for (int it = 0; it < iterations; it++) {
                long start = System.currentTimeMillis();
                matrix2 = SimilarityMatrixTest.createSimilarityMatrix(bigRecords);
                long stop = System.currentTimeMillis();
                long time = stop - start;
                stats.addValue(time);
            }
            LOG.info(String.format("Similarity matrix records[%d,%d] time %.2f ms",
                    bigRecords.length, fieldNames.length, stats.getPercentile(50)));

            stats.clear();
            for (int it = 0; it < iterations; it++) {
                NaiveExpectationMaximization estimator = new NaiveExpectationMaximization(fieldNames,0.9,0.1,0.01);
                long start = System.currentTimeMillis();
                estimator.runAlgorithm(matrix1);
                long stop = System.currentTimeMillis();
                long time = stop - start;
                stats.addValue(time);
            }
            LOG.info(String.format("Naive Expectation Maximation on records[%d,%d] time %.2f ms",
                    bigRecords.length, fieldNames.length, stats.getPercentile(50)));

            stats.clear();
            for (int it = 0; it < iterations; it++) {
                ExpectationMaximization estimator = new ExpectationMaximization(fieldNames,0.9,0.1,0.01);
                long start = System.currentTimeMillis();
                estimator.runAlgorithm(matrix2);
                long stop = System.currentTimeMillis();
                long time = stop - start;
                stats.addValue(time);
            }
            LOG.info(String.format("Expectation Maximation matrix records[%d,%d] time %.2f ms",
                    bigRecords.length, fieldNames.length, stats.getPercentile(50)));
        }

    }
}
