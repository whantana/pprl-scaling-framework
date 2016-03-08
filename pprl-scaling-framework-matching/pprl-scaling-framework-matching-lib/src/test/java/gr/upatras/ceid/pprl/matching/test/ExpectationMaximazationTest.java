package gr.upatras.ceid.pprl.matching.test;


import gr.upatras.ceid.pprl.matching.ExpectationMaximization;
import gr.upatras.ceid.pprl.matching.SimilarityMatrix;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


public class ExpectationMaximazationTest {

    private static Logger LOG = LoggerFactory.getLogger(ExpectationMaximazationTest.class);

    private String[][] records = SimilarityMatrixTest.records;
    private String[] fieldNames = SimilarityMatrixTest.fieldNames;

    @Test
    public void test1() throws IOException {
        DescriptiveStatistics stats = new DescriptiveStatistics();
        NaiveSimilarityMatrix matrix = null;
        NaiveExpectationMaximization estimator = null;
        for (int i = 0; i < 5; i++) {
            long start = System.currentTimeMillis();
            matrix = NaiveSimilarityMatrix.createMatrix(records);
            estimator = new NaiveExpectationMaximization (fieldNames,0.9,0.1,0.01);
            estimator.runAlgorithm(matrix);
            long end = System.currentTimeMillis();
            long time = end - start;
            stats.addValue(time);
        }
        LOG.info("matrix={} , estimator={}",matrix,estimator);
        LOG.info("Took {} ms.",stats.getPercentile(50));
    }

    @Test
    public void test2() throws IOException {
        DescriptiveStatistics stats = new DescriptiveStatistics();
        SimilarityMatrix matrix = null;
        ExpectationMaximization estimator = null;
        for (int i = 0; i < 5; i++) {
            long start = System.currentTimeMillis();
            matrix = SimilarityMatrixTest.createSimilarityMatrix(records);
            estimator = new ExpectationMaximization (fieldNames,0.9,0.1,0.01);
            estimator.runAlgorithm(matrix);
            long end = System.currentTimeMillis();
            long time = end - start;
            stats.addValue(time);
        }
        LOG.info("matrix={} , estimator={}",matrix,estimator);
        LOG.info("Took {} ms.",stats.getPercentile(50));
    }

    @Test
    public void test3() throws IOException {
        DescriptiveStatistics stats =  new DescriptiveStatistics();
        NaiveSimilarityMatrix matrix1 = null;
        SimilarityMatrix matrix2 = null;
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
