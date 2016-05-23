package gr.upatras.ceid.pprl.test;

import gr.upatras.ceid.pprl.datasets.DatasetException;
import gr.upatras.ceid.pprl.datasets.DatasetStatistics;
import gr.upatras.ceid.pprl.datasets.DatasetsUtil;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncoding;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncodingException;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncodingUtil;
import gr.upatras.ceid.pprl.encoding.CLKEncoding;
import gr.upatras.ceid.pprl.encoding.FieldBloomFilterEncoding;
import gr.upatras.ceid.pprl.encoding.RowBloomFilterEncoding;
import gr.upatras.ceid.pprl.matching.ExpectationMaximization;
import gr.upatras.ceid.pprl.matching.SimilarityUtil;
import gr.upatras.ceid.pprl.matching.SimilarityVectorFrequencies;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.AvroFSInput;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Set;
import java.util.SortedSet;

import static org.junit.Assert.assertTrue;

public class BloomFilterEncodingTest {
    // TODO Benchmark the accuracy of each format with the sorted neighborhood
    private static Logger LOG = LoggerFactory.getLogger(BloomFilterEncodingTest.class);

    private static final int N = 1024;
    private static final int K = 10;
    private static final int Q = 2;
    private FileSystem fs;


    @Before
    public void setUp() throws URISyntaxException, IOException, DatasetException {
        fs = FileSystem.getLocal(new Configuration());
    }

    @Test
    public void test0() {
        for (int i = 1; i <= 100; i++) {
            double g = Math.pow(0.5, (double) 1 / (i * K));
            int sz = FieldBloomFilterEncoding.dynamicsize(i, K);
            LOG.info(String.format("i=%d - %.6f - %d",i, g,sz));
        }
    }

    @Test
    public void test00() {

    }

    @Test
    public void test1()
            throws DatasetException, BloomFilterEncodingException, IOException {

        final String[] REST_FIELDS = new String[]{"id","location"};
        final String[] SELECTED_FIELDS = new String[]{"name","surname"};

        final SortedSet<Path> avroPaths = DatasetsUtil.getAllAvroPaths(fs,new Path("data/person_small/avro"));
        final Path schemaPath = new Path("data/person_small/schema/person_small.avsc");

        final DatasetStatistics statistics = calcDatasetStatistics(fs,avroPaths,schemaPath,SELECTED_FIELDS);

        final double[] avgQGrams = new double[SELECTED_FIELDS.length];
        final double[] weights = new double[SELECTED_FIELDS.length];
        for (int i = 0; i < SELECTED_FIELDS.length; i++) {
            avgQGrams[i] = statistics.getFieldStatistics().get(SELECTED_FIELDS[i]).getQgramCount(Q);
            weights[i] = statistics.getFieldStatistics().get(SELECTED_FIELDS[i]).getNormalizedRange();
        }

        int Narray[] = new int [SELECTED_FIELDS.length];
        Arrays.fill(Narray,N);

        encodeOriginal(new CLKEncoding(N,K,Q),"clk",
                fs,avroPaths,schemaPath,SELECTED_FIELDS,REST_FIELDS);
        encodeCopy("clk_copy", fs, avroPaths, schemaPath,
                new Path("data/person_small/clk.avsc"));

        encodeOriginal(new FieldBloomFilterEncoding(N,SELECTED_FIELDS.length,K,Q),"static_fbf",
                fs,avroPaths,schemaPath,SELECTED_FIELDS,REST_FIELDS);
        encodeCopy("static_fbf_copy", fs, avroPaths, schemaPath,
                new Path("data/person_small/static_fbf.avsc"));

        encodeOriginal(new FieldBloomFilterEncoding(avgQGrams,K,Q),"dynamic_fbf",
                fs,avroPaths,schemaPath,SELECTED_FIELDS,REST_FIELDS);
        encodeCopy("dynamic_fbf_copy", fs, avroPaths, schemaPath,
                new Path("data/person_small/dynamic_fbf.avsc"));

        encodeOriginal(new RowBloomFilterEncoding(Narray,N,K,Q),"uniform_rbf_static_fbf",
                fs,avroPaths,schemaPath,SELECTED_FIELDS,REST_FIELDS);
        encodeCopy("uniform_rbf_static_fbf_copy", fs, avroPaths, schemaPath,
                new Path("data/person_small/uniform_rbf_static_fbf.avsc"));

        encodeOriginal(new RowBloomFilterEncoding(avgQGrams,N,K,Q),"uniform_rbf_dynamic_fbf",
                fs,avroPaths,schemaPath,SELECTED_FIELDS,REST_FIELDS);
        encodeCopy("uniform_rbf_dynamic_fbf_copy", fs, avroPaths, schemaPath,
                new Path("data/person_small/uniform_rbf_dynamic_fbf.avsc"));

        encodeOriginal( new RowBloomFilterEncoding(Narray, weights, K, Q),"weighted_rbf_static_fbf",
                fs,avroPaths,schemaPath,SELECTED_FIELDS,REST_FIELDS);
        encodeCopy("weighted_rbf_static_fbf_copy", fs, avroPaths, schemaPath,
                new Path("data/person_small/weighted_rbf_static_fbf.avsc"));

        encodeOriginal( new RowBloomFilterEncoding(avgQGrams, weights, K, Q),"weighted_rbf_dynamic_fbf",
                fs,avroPaths,schemaPath,SELECTED_FIELDS,REST_FIELDS);
        encodeCopy("weighted_rbf_dynamic_fbf_copy", fs, avroPaths, schemaPath,
                new Path("data/person_small/weighted_rbf_dynamic_fbf.avsc"));
    }

    @Test
    public void test2()
            throws DatasetException, BloomFilterEncodingException, IOException {
        final String[] SELECTED_FIELDS = {"surname","name","address","city"};
        final String[] REST_FIELDS = {"id"};

        int Narray[] = new int [SELECTED_FIELDS.length];
        Arrays.fill(Narray,N);

        final SortedSet<Path> bobAvroPaths = DatasetsUtil.getAllAvroPaths(fs,new Path("data/voters_b/avro"));
        final Path bobSchemaPath = new Path("data/voters_b/schema/voters_b.avsc");

        final DatasetStatistics statistics = calcDatasetStatistics(fs,bobAvroPaths,bobSchemaPath,SELECTED_FIELDS);

        final double[] avgQGrams = new double[SELECTED_FIELDS.length];
        final double[] weights = new double[SELECTED_FIELDS.length];
        for (int i = 0; i < SELECTED_FIELDS.length; i++) {
            avgQGrams[i] = statistics.getFieldStatistics().get(SELECTED_FIELDS[i]).getQgramCount(Q);
            weights[i] = statistics.getFieldStatistics().get(SELECTED_FIELDS[i]).getNormalizedRange();
        }

        encodeOriginal(new CLKEncoding(N,K,Q),"clk",
                fs,bobAvroPaths,bobSchemaPath,SELECTED_FIELDS,REST_FIELDS);
        encodeOriginal(new FieldBloomFilterEncoding(N,SELECTED_FIELDS.length,K,Q),"static_fbf",
                fs,bobAvroPaths,bobSchemaPath,SELECTED_FIELDS,REST_FIELDS);
        encodeOriginal(new FieldBloomFilterEncoding(avgQGrams,K,Q),"dynamic_fbf",
                fs,bobAvroPaths,bobSchemaPath,SELECTED_FIELDS,REST_FIELDS);
        encodeOriginal(new RowBloomFilterEncoding(Narray, N, K, Q),"uniform_rbf_static_fbf",
                fs,bobAvroPaths,bobSchemaPath, SELECTED_FIELDS, REST_FIELDS);
        encodeOriginal(new RowBloomFilterEncoding(avgQGrams,N,K,Q),"uniform_rbf_dynamic_fbf",
                fs,bobAvroPaths,bobSchemaPath,SELECTED_FIELDS,REST_FIELDS);
        encodeOriginal(new RowBloomFilterEncoding(Narray, weights, K, Q),"weighted_rbf_static_fbf",
                fs,bobAvroPaths,bobSchemaPath,SELECTED_FIELDS,REST_FIELDS);
        encodeOriginal(new RowBloomFilterEncoding(avgQGrams, weights, K, Q),"weighted_rbf_dynamic_fbf",
                fs,bobAvroPaths,bobSchemaPath,SELECTED_FIELDS,REST_FIELDS);

        final Set<Path> aliceAvroPaths = DatasetsUtil.getAllAvroPaths(fs,new Path("data/voters_a/avro"));
        final Path aliceSchemaPath = new Path("data/voters_a/schema/voters_a.avsc");

        encodeByExistingSchema("clk",fs,aliceAvroPaths,aliceSchemaPath,
                new Path("data/voters_b/clk.avsc"),
                SELECTED_FIELDS,REST_FIELDS,SELECTED_FIELDS);
        encodeByExistingSchema("static_fbf",fs,aliceAvroPaths,aliceSchemaPath,
                new Path("data/voters_b/static_fbf.avsc"),
                SELECTED_FIELDS,REST_FIELDS,SELECTED_FIELDS);
        encodeByExistingSchema("dynamic_fbf",fs,aliceAvroPaths,aliceSchemaPath,
                new Path("data/voters_b/dynamic_fbf.avsc"),
                SELECTED_FIELDS,REST_FIELDS,SELECTED_FIELDS);
        encodeByExistingSchema("uniform_rbf_static_fbf",fs,aliceAvroPaths,aliceSchemaPath,
                new Path("data/voters_b/uniform_rbf_static_fbf.avsc"),
                SELECTED_FIELDS,REST_FIELDS,SELECTED_FIELDS);
        encodeByExistingSchema("uniform_rbf_dynamic_fbf",fs,aliceAvroPaths,aliceSchemaPath
                ,new Path("data/voters_b/uniform_rbf_dynamic_fbf.avsc"),
                SELECTED_FIELDS,REST_FIELDS,SELECTED_FIELDS);
        encodeByExistingSchema("weighted_rbf_static_fbf",fs,aliceAvroPaths,aliceSchemaPath,
                new Path("data/voters_b/weighted_rbf_static_fbf.avsc"),
                SELECTED_FIELDS,REST_FIELDS,SELECTED_FIELDS);
        encodeByExistingSchema("weighted_rbf_dynamic_fbf",fs,aliceAvroPaths,aliceSchemaPath,
                new Path("data/voters_b/weighted_rbf_dynamic_fbf.avsc"),
                SELECTED_FIELDS,REST_FIELDS,SELECTED_FIELDS);

    }

    private static void encodeOriginal(final BloomFilterEncoding encoding,
                                       final String name,
                                       final FileSystem fs,
                                       final Set<Path> avroPaths,
                                       final Path schemaPath,
                                       final String[] selected,
                                       final String[] included)
            throws DatasetException, IOException, BloomFilterEncodingException {
        final Path basePath = schemaPath.getParent().getParent();
        Schema schema = DatasetsUtil.loadSchemaFromFSPath(fs,schemaPath);
        encoding.makeFromSchema(schema, selected, included);
        assertTrue(encoding.isEncodingOfSchema(schema));
        encodeLocalFile(fs,name,basePath,avroPaths,schema,encoding);
    }

    private static void encodeCopy(final String name,
                                   final FileSystem fs,
                                   final Set<Path> avroPaths,
                                   final Path schemaPath,
                                   final Path encodingSchemaPath)
            throws BloomFilterEncodingException, DatasetException, IOException {
        final Path basePath = schemaPath.getParent().getParent();
        Schema schema = DatasetsUtil.loadSchemaFromFSPath(fs,schemaPath);
        Schema encodingSchema = DatasetsUtil.loadSchemaFromFSPath(fs,encodingSchemaPath);
        BloomFilterEncoding encoding = BloomFilterEncodingUtil.setupNewInstance(encodingSchema);
        assertTrue(encoding.isEncodingOfSchema(schema));
        encodeLocalFile(fs,name,basePath,avroPaths,schema,encoding);
    }

    private static void encodeByExistingSchema(final String name,
                                               final FileSystem fs,
                                               final Set<Path> avroPaths,
                                               final Path schemaPath,
                                               final Path existingEncodingSchemaPath,
                                               final String[] selected,
                                               final String[] included,
                                               final String[] existingFieldNames)
            throws BloomFilterEncodingException, DatasetException, IOException {
        final Path basePath = schemaPath.getParent().getParent();
        Schema schema = DatasetsUtil.loadSchemaFromFSPath(fs,schemaPath);
        Schema existingEncodingSchema = DatasetsUtil.loadSchemaFromFSPath(fs,existingEncodingSchemaPath);
        BloomFilterEncoding encoding = BloomFilterEncodingUtil.setupNewInstance(
                BloomFilterEncodingUtil.basedOnExistingSchema(schema,selected,included,existingEncodingSchema,existingFieldNames));
        assertTrue(encoding.isEncodingOfSchema(schema));
        encodeLocalFile(fs,name,basePath,avroPaths,schema,encoding);
    }

    private static String[] encodeLocalFile(final FileSystem fs,
                                            final String name,
                                            final Path basePath,
                                            final Set<Path> avroFiles, final Schema schema,
                                            final BloomFilterEncoding encoding)
            throws IOException, BloomFilterEncodingException, DatasetException {
        final Schema encodingSchema = encoding.getEncodingSchema();
        encoding.initialize();

        final Path encodingSchemaPath = new Path(basePath,name + ".avsc");
        DatasetsUtil.saveSchemaToFSPath(fs,encoding.getEncodingSchema(),encodingSchemaPath);


        final Path encodingAvroPath = new Path(basePath,name + ".avro");
        final FSDataOutputStream fsdos = fs.create(encodingAvroPath, true);

        final DataFileWriter<GenericRecord> writer =
                new DataFileWriter<GenericRecord>(
                        new GenericDatumWriter<GenericRecord>(encodingSchema));
        writer.create(encodingSchema, fsdos);
        for (Path p : avroFiles) {
            final long len = fs.getFileStatus(p).getLen();
            final DataFileReader<GenericRecord> reader =
                    new DataFileReader<GenericRecord>(new AvroFSInput(fs.open(p),len),
                            new GenericDatumReader<GenericRecord>(schema));
            for (GenericRecord record : reader) {
                final GenericRecord encodingRecord = encoding.encodeRecord(record);
                writer.append(encodingRecord);
            }
            reader.close();
        }
        writer.close();
        fsdos.close();

        return new String[]{
                encodingAvroPath.toString(),
                encodingSchemaPath.toString()
        };
    }

    private static DatasetStatistics calcDatasetStatistics(final FileSystem fs,
                                                           final SortedSet<Path> avroPaths,
                                                           final Path schemaPath,
                                                           final String[] fieldNames) throws DatasetException, IOException {

        final Schema schema = DatasetsUtil.loadSchemaFromFSPath(fs, schemaPath);
        final GenericRecord[] records = DatasetsUtil.loadAvroRecordsFromFSPaths(fs,schema,
                avroPaths.toArray(new Path[avroPaths.size()]));

        // setup stats
        final DatasetStatistics statistics = new DatasetStatistics();

        // record count and field names
        statistics.setRecordCount(records.length);
        statistics.setFieldNames(fieldNames);

        // calculate average q-grams field names
        DatasetStatistics.calculateQgramStatistics(records, schema, statistics, fieldNames);

        // estimate m u
        SimilarityVectorFrequencies frequencies =
                SimilarityUtil.vectorFrequencies(records, fieldNames);
        ExpectationMaximization estimator =new ExpectationMaximization (fieldNames,0.9,0.1,0.01);
        estimator.runAlgorithm(frequencies);

        // using m,u,p estimates complete the statistics of the dataset
        statistics.setEmPairsCount(estimator.getPairCount());
        statistics.setEmAlgorithmIterations(estimator.getIteration());
        statistics.setP(estimator.getP());
        DatasetStatistics.calculateStatsUsingEstimates(
                statistics,fieldNames,
                estimator.getM(),estimator.getU());

        return statistics;
    }
}
