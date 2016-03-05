package gr.upatras.ceid.pprl.datasets.test;

import gr.upatras.ceid.pprl.base.CombinatoricsUtil;
import gr.upatras.ceid.pprl.datasets.DatasetStatistics;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertTrue;

public class DatasetsStatisticsTest {

    private static final Logger LOG = LoggerFactory.getLogger(DatasetsStatisticsTest.class);


    DatasetStatistics stats1 = new DatasetStatistics();

    @Before
    public void setup() {
        stats1.setRecordCount(10);
        final String[] fieldNames = new String[]{"f1","f2"};
        stats1.setFieldNames(fieldNames);
        stats1.setEmPairs(CombinatoricsUtil.twoCombinationsCount(10));
        stats1.setEmAlgorithmIterations(3);
        stats1.setP(0.5);
        stats1.getFieldStatistics().get("f1").setLength(5);
        stats1.getFieldStatistics().get("f1").setQgramCount(new double[]{10, 20, 30});
        stats1.getFieldStatistics().get("f1").setUniqueQgramCount(new double[]{1, 2, 3});
        stats1.getFieldStatistics().get("f2").setLength(4);
        stats1.getFieldStatistics().get("f2").setQgramCount(new double[]{9, 19, 29});
        stats1.getFieldStatistics().get("f2").setUniqueQgramCount(new double[]{0, 1, 2});
        DatasetStatistics.calculateStatsUsingEstimates(stats1, fieldNames,new double[]{0.8,0.8},new double[]{0.009,0.00009});
    }

    @Test
    public void test0(){
        DatasetStatistics stats2 = new DatasetStatistics();
        stats2.fromProperties(stats1.toProperties());
        LOG.info("\n" + stats1.toString() + "\n" + stats2.toString());
        assertTrue(stats2.equals(stats1));
    }
}
