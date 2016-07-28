package gr.upatras.ceid.pprl.benchmarks;

import gr.upatras.ceid.pprl.combinatorics.CombinatoricsUtil;
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
import org.apache.hadoop.fs.FSDataOutputStream;
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

	private static final String[][] SELECTED_FIELD_NAMES = {{"name","surname","location"},{"name","surname"}};

    private double[][] initialM = {{0.99,0.99,0.99},{0.99,0.99}};
	private double[][] initialU = {{0.01,0.01,0.01},{0.01,0.01}};
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
        FileSystem fs = FileSystem.getLocal(new Configuration());

        DescriptiveStatistics statsM =  new DescriptiveStatistics();
        DescriptiveStatistics stats =  new DescriptiveStatistics();
        int iterations = 100;
		int sizes = 4;
        final String sHeader = "data,record_count,field_count,pair_count,matrix_count,counters_count";
        final String[] pHeader = {
                "data,m_name,u_name,m_surname,u_surname,m_location,u_location,p,iterations",
                "data,m_name,m_surname,u_name,u_surname,p,iterations"
        };
        final String tHeader = "data,make_gamma_mtx,make_gamma_counter,naive_em,smart_em";

        for(int set=0; set <= 1;set++) {
            final FSDataOutputStream fsdosSZ = fs.create(new Path("data/benchmarks",String.format("set_%d_sizes.csv",set)));
            fsdosSZ.writeBytes(sHeader+"\n");
            final FSDataOutputStream fsdosPB = fs.create(new Path("data/benchmarks",String.format("set_%d_probs.csv",set)));
            fsdosPB.writeBytes(pHeader[set]+"\n");
            final FSDataOutputStream fsdosTI = fs.create(new Path("data/benchmarks",String.format("set_%d_times.csv",set)));
            fsdosTI.writeBytes(tHeader+"\n");
            for (int i = 1; i <= sizes; i++) {
                LOG.info(String.format("Dataset : d%d",i));
                GenericRecord[] bigRecords = new GenericRecord[i * records.length];
                for (int j = 0; j < i; j++)
                    System.arraycopy(records, 0, bigRecords, j * records.length, records.length);

                for (int it = 0; it < iterations / 4; it++) {
                    final SimilarityMatrix mtx =
                            SimilarityUtil.matrix(bigRecords, SELECTED_FIELD_NAMES[set]);
                    NaiveExpectationMaximization naiveEstimator =
                            new NaiveExpectationMaximization(SELECTED_FIELD_NAMES[set], initialM[set], initialU[set], initialP);
                    naiveEstimator.runAlgorithm(mtx);
                    final SimilarityVectorFrequencies fqs =
                            SimilarityUtil.vectorFrequencies(bigRecords, SELECTED_FIELD_NAMES[set]);
                    ExpectationMaximization estimator =
                            new ExpectationMaximization(SELECTED_FIELD_NAMES[set], initialM[set], initialU[set], initialP);
                    estimator.runAlgorithm(fqs);
                }

                final long pairCount = CombinatoricsUtil.twoCombinationsCount(bigRecords.length);
                final int fieldCount = SELECTED_FIELD_NAMES[set].length;
                fsdosSZ.writeBytes(String.format("d%d,%d,%d,%d,%d,%d\n",
                        i,bigRecords.length,fieldCount,pairCount,pairCount*fieldCount,(1 << fieldCount)
                ));

                StringBuilder sb = new StringBuilder(String.format("d%d",i));

                statsM.clear();
                for (int it = 0; it < iterations; it++) {
                    long start = System.nanoTime();
                    SimilarityUtil.matrix(bigRecords, SELECTED_FIELD_NAMES[set]);
                    long stop = System.nanoTime();
                    long time = stop - start;
                    statsM.addValue(time);
                }
                sb.append(String.format(",%d",(int) getCorrectMean(statsM)));

                statsM.clear();
                for (int it = 0; it < iterations; it++) {
                    long start = System.nanoTime();
                    SimilarityUtil.vectorFrequencies(bigRecords, SELECTED_FIELD_NAMES[set]);
                    long stop = System.nanoTime();
                    long time = stop - start;
                    statsM.addValue(time);
                }
                sb.append(String.format(",%d", (int) getCorrectMean(statsM)));

                SimilarityMatrix matrix = SimilarityUtil.matrix(bigRecords, SELECTED_FIELD_NAMES[set]);
                NaiveExpectationMaximization naiveEstimator = new NaiveExpectationMaximization(SELECTED_FIELD_NAMES[set],
                        initialM[set], initialU[set], initialP);
                naiveEstimator.runAlgorithm(matrix);

                SimilarityVectorFrequencies frequencies = SimilarityUtil.vectorFrequencies(bigRecords, SELECTED_FIELD_NAMES[set]);
                ExpectationMaximization estimator = new ExpectationMaximization(SELECTED_FIELD_NAMES[set],
                        initialM[set], initialU[set], initialP);
                estimator.runAlgorithm(frequencies);

                final double[] m = estimator.getM();
                final double[] u = estimator.getU();
                final double p = estimator.getP();
                final int iter = estimator.getIteration();
                final StringBuilder nsb = new StringBuilder(String.format("d%d",i));
                for (int l = 0 ; l < SELECTED_FIELD_NAMES[set].length; l++) {
                    nsb.append(String.format(",%.3f", m[l]));
                    nsb.append(String.format(",%.3f", u[l]));
                }
                nsb.append(String.format(",%.5f",p));
                nsb.append(String.format(",%d\n",iter));
                fsdosPB.writeBytes(nsb.toString());


                stats.clear();
                for (int it = 0; it < iterations; it++) {
                    NaiveExpectationMaximization ne =
                            new NaiveExpectationMaximization(SELECTED_FIELD_NAMES[set],
                                    initialM[set], initialU[set], initialP);
                    long start = System.nanoTime();
                    ne.runAlgorithm(matrix);
                    long stop = System.nanoTime();
                    long time = stop - start;
                    stats.addValue(time);
                }
                sb.append(String.format(",%d", (int) getCorrectMean(statsM)));


                stats.clear();
                for (int it = 0; it < iterations; it++) {
                    ExpectationMaximization e =
                            new ExpectationMaximization(SELECTED_FIELD_NAMES[set],
                                    initialM[set], initialU[set], initialP);
                    long start = System.nanoTime();
                    e.runAlgorithm(frequencies);
                    long stop = System.nanoTime();
                    long time = stop - start;
                    stats.addValue(time);
                }
                sb.append(String.format(",%d\n", (int) getCorrectMean(statsM)));
                fsdosTI.writeBytes(sb.toString());
            }
            fsdosSZ.close();
            fsdosPB.close();
            fsdosTI.close();
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
