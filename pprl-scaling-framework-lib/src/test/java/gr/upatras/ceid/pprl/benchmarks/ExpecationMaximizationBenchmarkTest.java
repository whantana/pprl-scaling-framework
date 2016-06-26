package gr.upatras.ceid.pprl.benchmarks;

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

public class ExpecationMaximizationBenchmarkTest {

    private static Logger LOG = LoggerFactory.getLogger(ExpecationMaximizationBenchmarkTest.class);
    private static final String[] SMALL_HEADER = {"id","name","surname","location"};

    private static Schema.Type[] SMALL_TYPES = {
            Schema.Type.STRING,Schema.Type.STRING,
            Schema.Type.STRING,Schema.Type.STRING
    };
    private static String[] SMALL_DOCS = {
            "unique id","First name","Last name","State"
    };

	private static final String[] SELECTED_FIELD_NAMES = {"name","surname","location"};
	// private static final String[] SELECTED_FIELD_NAMES = {"name","surname"};

    private double[] initialM = {0.99,0.99,0.99};
	private double[] initialU = {0.01,0.001,0.99};
	// private double[] initialM = {0.99,0.99};
    // private double[] initialU = {0.01,0.01};
    
	private double initialP = 0.005;
    GenericRecord[] records;

    @Before
    public void setup() throws IOException, DatasetException {
        FileSystem fs = FileSystem.getLocal(new Configuration());
        Schema schema = DatasetsUtil.avroSchema("person_small","People","pprl.datasets",
                SMALL_HEADER,SMALL_TYPES,SMALL_DOCS);
        final Path p = DatasetsUtil.csv2avro(fs,schema,"person_small",new Path(fs.getWorkingDirectory(),"data"),
                new Path(fs.getWorkingDirectory(), "data/person_small/csv/person_small.csv"));
        LOG.info("Saved at path {} ", p);
        records = DatasetsUtil.loadAvroRecordsFromFSPaths(fs, schema,
                new Path("data/person_small/avro/person_small.avro"));
    }

    @Test
    public void test3() throws IOException {
        DescriptiveStatistics statsM =  new DescriptiveStatistics();
        DescriptiveStatistics stats =  new DescriptiveStatistics();
        DescriptiveStatistics statsP =  new DescriptiveStatistics();
        int iterations = 100;
		int sizes = 4;
        for (int i = 1; i <= sizes; i++) {
            
			GenericRecord[] bigRecords = new GenericRecord[i*records.length];
            for (int j = 0 ; j < i ; j++)
                System.arraycopy(records, 0, bigRecords, j * records.length, records.length);
			
			
			LOG.info("Warming up...",i);
			for (int it = 0; it < iterations/4; it++) {
				final SimilarityMatrix mtx = 
					SimilarityUtil.matrix(bigRecords, SELECTED_FIELD_NAMES);
                NaiveExpectationMaximization naiveEstimator = 
					new NaiveExpectationMaximization(SELECTED_FIELD_NAMES,initialM,initialU,initialP);
				naiveEstimator.runAlgorithm(mtx);
				final SimilarityVectorFrequencies fqs = 
					SimilarityUtil.vectorFrequencies(bigRecords, SELECTED_FIELD_NAMES);
	            ExpectationMaximization estimator = 
					new ExpectationMaximization(SELECTED_FIELD_NAMES,initialM,initialU,initialP);
				estimator.runAlgorithm(fqs);
			}

            LOG.info("--Person_small x {}--",i);



            statsM.clear();
            for (int it = 0; it < iterations; it++) {
                long start = System.nanoTime();
                SimilarityUtil.matrix(bigRecords, SELECTED_FIELD_NAMES);
                long stop = System.nanoTime();
                long time = stop - start;
                statsM.addValue(time);
            }
            LOG.info(String.format("Get Gamma Matrix [%d,%d] time %d ns",
                    bigRecords.length, SELECTED_FIELD_NAMES.length, (int)getCorrectMean(statsM)));

            statsM.clear();
            for (int it = 0; it < iterations; it++) {
                long start = System.nanoTime();
                SimilarityUtil.vectorFrequencies(bigRecords, SELECTED_FIELD_NAMES);
                long stop = System.nanoTime();
                long time = stop - start;
                statsM.addValue(time);
            }
            LOG.info(String.format("Get Gamma counters [%d,%d] time %d ns",
                    bigRecords.length, SELECTED_FIELD_NAMES.length, (int)getCorrectMean(statsM)));

            SimilarityMatrix matrix = SimilarityUtil.matrix(bigRecords, SELECTED_FIELD_NAMES);
            LOG.info(matrix.toString());
            NaiveExpectationMaximization naiveEstimator = new NaiveExpectationMaximization(SELECTED_FIELD_NAMES,
                    initialM,initialU,initialP);
            naiveEstimator.runAlgorithm(matrix);
            LOG.info(naiveEstimator.toString());

            SimilarityVectorFrequencies frequencies = SimilarityUtil.vectorFrequencies(bigRecords, SELECTED_FIELD_NAMES);
            LOG.info(frequencies.toString());
            ExpectationMaximization estimator = new ExpectationMaximization(SELECTED_FIELD_NAMES,
                    initialM,initialU,initialP);
            estimator.runAlgorithm(frequencies);
            LOG.info(estimator.toString());


            stats.clear();
            statsP.clear();
            for (int it = 0; it < iterations; it++) {
                NaiveExpectationMaximization ne =
                        new NaiveExpectationMaximization(SELECTED_FIELD_NAMES,
                        initialM,initialU,initialP);
                long start = System.nanoTime();
                ne.runAlgorithm(matrix);
                long stop = System.nanoTime();
                long time = stop - start;
                stats.addValue(time);
                statsP.addValue(naiveEstimator.getP());
            }
            LOG.info(String.format("Naive EM [%d,%d] time %d ns, p=%.3f.",
                    bigRecords.length, SELECTED_FIELD_NAMES.length,
                    (int)getCorrectMean(stats), getCorrectMean(statsP)));

            stats.clear();
            statsP.clear();
            for (int it = 0; it < iterations; it++) {
                ExpectationMaximization e =
                        new ExpectationMaximization(SELECTED_FIELD_NAMES,
                        initialM,initialU,initialP);
                long start = System.nanoTime();
                e.runAlgorithm(frequencies);
                long stop = System.nanoTime();
                long time = stop - start;
                stats.addValue(time);
                statsP.addValue(estimator.getP());
            }
            LOG.info(String.format("Proper EM [%d,%d] time %d ns,p=%.3f.",
                    bigRecords.length, SELECTED_FIELD_NAMES.length,
                    (int)getCorrectMean(stats), getCorrectMean(statsP)));
        }
    }


    public double getCorrectMean(final DescriptiveStatistics stats) {
        double q1 = stats.getPercentile(25);
        double q3 = stats.getPercentile(75);
        assert q1 <= q3;
        double iqr = q3 - q1;
        double upper = q3 + 1.5*iqr;
        double lower = q1 - 1.5*iqr;
        final DescriptiveStatistics noOutliersStats = new DescriptiveStatistics();
        for (double v : stats.getValues())
            if(v >= lower && v <= upper) noOutliersStats.addValue(v);
        return noOutliersStats.getMean();
    }
}
