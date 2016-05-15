package gr.upatras.ceid.pprl.test;

import gr.upatras.ceid.pprl.datasets.DatasetException;
import gr.upatras.ceid.pprl.datasets.DatasetsUtil;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncoding;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncodingException;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncodingUtil;
import gr.upatras.ceid.pprl.encoding.CLKEncoding;
import gr.upatras.ceid.pprl.encoding.FieldBloomFilterEncoding;
import gr.upatras.ceid.pprl.encoding.RowBloomFilterEncoding;
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
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;

import static org.junit.Assert.assertTrue;

public class BloomFilterEncodingTest {

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
    public void test1()
            throws DatasetException, BloomFilterEncodingException, IOException {
        double[] avgQcount = new double[]{6.0,9.0};
        double[] weights = new double[]{0.2,0.8};
        final String[] REST_FIELDS = new String[]{"id","location"};
        final String[] SELECTED_FIELDS = new String[]{"name","surname"};
        final String[] datasets = {
                "person_small",
        };

        for(String dName : datasets) {
            final String dataset = "data/" + dName;
            final Path avroPath = new Path(dataset,"avro/" + dName + ".avro");
            final Path schemaPath = new Path(dataset,"schema/" + dName + ".avsc");
            LOG.info("Encoding dataset : {} ({})",dataset,String.format("%s,%s",avroPath,schemaPath));


            encodeOriginal(new CLKEncoding(N,K,Q),dataset + "/clk",
                    fs,Collections.singleton(avroPath),schemaPath,SELECTED_FIELDS,REST_FIELDS);
            encodeCopy(dataset + "/clk_copy", fs, Collections.singleton(avroPath),
                    schemaPath,new Path(dataset + "/clk.avsc"));

            encodeOriginal(new FieldBloomFilterEncoding(avgQcount,K,Q),dataset + "/dynamic_fbf",
                    fs,Collections.singleton(avroPath),schemaPath,SELECTED_FIELDS,REST_FIELDS);
            encodeCopy(dataset + "/dynamic_fbf_copy", fs, Collections.singleton(avroPath),
                    schemaPath,new Path(dataset + "/dynamic_fbf.avsc"));

            encodeOriginal(new FieldBloomFilterEncoding(N,SELECTED_FIELDS.length,K,Q),dataset + "/static_fbf",
                    fs,Collections.singleton(avroPath),schemaPath,SELECTED_FIELDS,REST_FIELDS);
            encodeCopy(dataset + "/static_fbf_copy", fs, Collections.singleton(avroPath),
                    schemaPath,new Path(dataset + "/static_fbf.avsc"));

            encodeOriginal( new RowBloomFilterEncoding(avgQcount, weights, K, Q),dataset + "/weighted_rbf",
                    fs,Collections.singleton(avroPath),schemaPath,SELECTED_FIELDS,REST_FIELDS);
            encodeCopy(dataset + "/weighted_rbf_copy", fs, Collections.singleton(avroPath),
                    schemaPath,new Path(dataset + "/weighted_rbf.avsc"));

            encodeOriginal(new RowBloomFilterEncoding(avgQcount,N,K,Q),dataset + "/uniform_rbf",
                    fs,Collections.singleton(avroPath),schemaPath,SELECTED_FIELDS,REST_FIELDS);
            encodeCopy(dataset + "/uniform_rbf_copy", fs, Collections.singleton(avroPath),
                    schemaPath,new Path(dataset + "/uniform_rbf.avsc"));
        }
    }

    @Test
    public void test2()
            throws DatasetException, BloomFilterEncodingException, IOException {
        final String[] SELECTED_FIELDS = {"surname","name","address","city"};
        final String[] REST_FIELDS = {"id"};

        final Set<Path> aliceAvroPaths = DatasetsUtil.getAllAvroPaths(fs,new Path("data/voters_a/avro"));
        final Path aliceSchemaPath = new Path("data/voters_a/schema/voters_a.avsc");
        final Set<Path> bobAvroPaths = DatasetsUtil.getAllAvroPaths(fs,new Path("data/voters_b/avro"));
        final Path bobSchemaPath = new Path("data/voters_b/schema/voters_b.avsc");

        encodeOriginal(new CLKEncoding(N,K,Q),"data/voters_a/clk",
                fs,aliceAvroPaths,aliceSchemaPath,SELECTED_FIELDS,REST_FIELDS);

        encodeOriginal(new FieldBloomFilterEncoding(N,SELECTED_FIELDS.length,K,Q),"data/voters_a/static_fbf",
                fs,aliceAvroPaths,aliceSchemaPath,SELECTED_FIELDS,REST_FIELDS);
        int Narray[] = new int [SELECTED_FIELDS.length];
        Arrays.fill(Narray,N);
        encodeOriginal(new RowBloomFilterEncoding(Narray, N, K, Q),"data/voters_a/uniform_rbf",
                fs,aliceAvroPaths,aliceSchemaPath, SELECTED_FIELDS, REST_FIELDS);


        encodeByExistingSchema("data/voters_b/clk",fs,bobAvroPaths,bobSchemaPath,new Path("data/voters_a/clk.avsc"),
                SELECTED_FIELDS,REST_FIELDS,SELECTED_FIELDS);
        encodeByExistingSchema("data/voters_b/static_fbf",fs,bobAvroPaths,bobSchemaPath,new Path("data/voters_a/static_fbf.avsc"),
                SELECTED_FIELDS,REST_FIELDS,SELECTED_FIELDS);
        encodeByExistingSchema("data/voters_b/uniform_rbf",fs,bobAvroPaths,bobSchemaPath,new Path("data/voters_a/uniform_rbf.avsc"),
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
        Schema schema = DatasetsUtil.loadSchemaFromFSPath(fs,schemaPath);
        encoding.makeFromSchema(schema, selected, included);
        assertTrue(encoding.isEncodingOfSchema(schema));
        encodeLocalFile(fs,name,avroPaths,schema,encoding);
    }

    private static void encodeCopy(final String name,
                                   final FileSystem fs,
                                   final Set<Path> avroPaths,
                                   final Path schemaPath,
                                   final Path encodingSchemaPath)
            throws BloomFilterEncodingException, DatasetException, IOException {
        Schema schema = DatasetsUtil.loadSchemaFromFSPath(fs,schemaPath);
        Schema encodingSchema = DatasetsUtil.loadSchemaFromFSPath(fs,encodingSchemaPath);
        BloomFilterEncoding encoding = BloomFilterEncodingUtil.setupNewInstance(encodingSchema);
        assertTrue(encoding.isEncodingOfSchema(schema));
        encodeLocalFile(fs,name,avroPaths,schema,encoding);
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

        Schema schema = DatasetsUtil.loadSchemaFromFSPath(fs,schemaPath);
        Schema existingEncodingSchema = DatasetsUtil.loadSchemaFromFSPath(fs,existingEncodingSchemaPath);
        BloomFilterEncoding encoding = BloomFilterEncodingUtil.setupNewInstance(
                BloomFilterEncodingUtil.basedOnExistingSchema(schema,selected,included,existingEncodingSchema,existingFieldNames));
        assertTrue(encoding.isEncodingOfSchema(schema));
        encodeLocalFile(fs,name,avroPaths,schema,encoding);
    }

    private static String[] encodeLocalFile(final FileSystem fs ,
                                            final String name, final Set<Path> avroFiles, final Schema schema,
                                            final BloomFilterEncoding encoding)
            throws IOException, BloomFilterEncodingException {
        final Schema encodingSchema = encoding.getEncodingSchema();
        encoding.initialize();

        final File encodedSchemaFile = new File(name + ".avsc");
        LOG.info("Encoding schema file : ", encodedSchemaFile.toString());
        encodedSchemaFile.createNewFile();
        final PrintWriter schemaWriter = new PrintWriter(encodedSchemaFile);
        schemaWriter.print(encodingSchema .toString(true));
        schemaWriter.close();

        final File encodedFile = new File(name + ".avro");
        LOG.info("Encoding avro file : ", encodedFile.toString());
        encodedFile.createNewFile();
        final DataFileWriter<GenericRecord> writer =
                new DataFileWriter<GenericRecord>(
                        new GenericDatumWriter<GenericRecord>(encodingSchema));
        writer.create(encodingSchema, encodedFile);
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

        return new String[]{
                encodedFile.getAbsolutePath(),
                encodedSchemaFile.getAbsolutePath()
        };
    }
}
