package gr.upatras.ceid.pprl.test;

import gr.upatras.ceid.pprl.encoding.BloomFilterEncoding;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncodingException;
import gr.upatras.ceid.pprl.encoding.CLKEncoding;
import gr.upatras.ceid.pprl.encoding.FieldBloomFilterEncoding;
import gr.upatras.ceid.pprl.encoding.RowBloomFilterEncoding;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.Set;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
@Ignore
// TODO FIXME I STILL USE FILE
public class BloomFilterEncodingTest {

    private static Logger LOG = LoggerFactory.getLogger(BloomFilterEncodingTest.class);

    private Schema schema;
    private Set<File> avroFiles;

    private static double[] avgQcount = new double[]{6.0,9.0};
    private static double[] weights = new double[]{0.2,0.8};
    private static final String[] REST_FIELDS = new String[]{"id","location"};
    private static final String[] SELECTED_FIELDS = new String[]{"name","surname"};
    private static final int N = 500;
    private static final int K = 10;
    private static final int Q = 2;


    @Before
    public void setUp() throws URISyntaxException, IOException {
        avroFiles = Collections.singleton(new File("data/person_small/avro/person_small.avro"));
        File schemaFile = new File("data/person_small/schema/person_small.avsc");
        schema = loadAvroSchemaFromFile(schemaFile);
        assertNotNull(schema);
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
    public void test01() throws URISyntaxException, IOException, InterruptedException, BloomFilterEncodingException {
        BloomFilterEncoding encoding = new FieldBloomFilterEncoding(avgQcount,K,Q);
        encoding.makeFromSchema(schema, SELECTED_FIELDS, REST_FIELDS);
        assertTrue(encoding.isEncodingOfSchema(schema));
        encodeLocalFile("data/dynamic_fbf",avroFiles,schema,encoding);
        saveAvroSchemaToFile(encoding.getEncodingSchema(),new File("data/dynamic_fbf.avsc"));
    }

    @Test
    public void test02() throws URISyntaxException, IOException, InterruptedException, BloomFilterEncodingException {
        final Schema encodingSchema = loadAvroSchemaFromFile( new File("data/dynamic_fbf.avsc"));
        FieldBloomFilterEncoding encoding = new FieldBloomFilterEncoding();
        encoding.setupFromSchema(encodingSchema);
        assertTrue(encoding.isEncodingOfSchema(schema));
        encodeLocalFile("data/dynamic_fbf_copy",avroFiles,schema,encoding);
    }

    @Test
    public void test03() throws URISyntaxException, IOException, InterruptedException, BloomFilterEncodingException {
        FieldBloomFilterEncoding encoding = new FieldBloomFilterEncoding(N,SELECTED_FIELDS.length,K,Q);
        encoding.makeFromSchema(schema, SELECTED_FIELDS, REST_FIELDS);
        assertTrue(encoding.isEncodingOfSchema(schema));
        encodeLocalFile("data/static_fbf", avroFiles, schema, encoding);
        saveAvroSchemaToFile(encoding.getEncodingSchema(),new File("data/static_fbf.avsc"));
    }

    @Test
    public void test04() throws URISyntaxException, IOException, InterruptedException, BloomFilterEncodingException {
        final Schema encodingSchema = loadAvroSchemaFromFile( new File("data/static_fbf.avsc"));
        FieldBloomFilterEncoding encoding = new FieldBloomFilterEncoding();
        encoding.setupFromSchema(encodingSchema);
        assertTrue(encoding.isEncodingOfSchema(schema));
        encodeLocalFile("data/static_fbf_copy", avroFiles, schema, encoding);
    }

    @Test
    public void test05() throws URISyntaxException, IOException, InterruptedException, BloomFilterEncodingException {
        RowBloomFilterEncoding encoding = new RowBloomFilterEncoding(avgQcount, weights, K, Q);
        encoding.makeFromSchema(schema, SELECTED_FIELDS, REST_FIELDS);
        assertTrue(encoding.isEncodingOfSchema(schema));
        encodeLocalFile("data/weighted_rbf", avroFiles, schema, encoding);
        saveAvroSchemaToFile(encoding.getEncodingSchema(),new File("data/weighted_rbf.avsc"));
    }

    @Test
    public void test06() throws URISyntaxException, IOException, InterruptedException, BloomFilterEncodingException {
        final Schema encodingSchema = loadAvroSchemaFromFile( new File("data/weighted_rbf.avsc"));
        RowBloomFilterEncoding encoding = new RowBloomFilterEncoding();
        encoding.setupFromSchema(encodingSchema);
        assertTrue(encoding.isEncodingOfSchema(schema));
        encodeLocalFile("data/weighted_rbf_copy", avroFiles, schema, encoding);
    }

    @Test
    public void test07() throws URISyntaxException, IOException, InterruptedException, BloomFilterEncodingException {
        RowBloomFilterEncoding encoding = new RowBloomFilterEncoding(avgQcount,N,K,Q);
        encoding.makeFromSchema(schema, SELECTED_FIELDS, REST_FIELDS);
        assertTrue(encoding.isEncodingOfSchema(schema));
        encodeLocalFile("data/uniform_rbf", avroFiles, schema, encoding);
        saveAvroSchemaToFile(encoding.getEncodingSchema(),new File("data/uniform_rbf.avsc"));
    }

    @Test
    public void test08() throws URISyntaxException, IOException, InterruptedException, BloomFilterEncodingException {
        final Schema encodingSchema = loadAvroSchemaFromFile( new File("data/uniform_rbf.avsc"));
        RowBloomFilterEncoding encoding = new RowBloomFilterEncoding();
        encoding.setupFromSchema(encodingSchema);
        assertTrue(encoding.isEncodingOfSchema(schema));
        encodeLocalFile("data/uniform_rbf_copy", avroFiles, schema, encoding);
    }

    @Test
    public void test09() throws URISyntaxException, IOException, InterruptedException, BloomFilterEncodingException {
        CLKEncoding encoding = new CLKEncoding(N,K,Q);
        encoding.makeFromSchema(schema, SELECTED_FIELDS, REST_FIELDS);
        assertTrue(encoding.isEncodingOfSchema(schema));
        encodeLocalFile("data/clk", avroFiles, schema, encoding);
        saveAvroSchemaToFile(encoding.getEncodingSchema(),new File("data/clk.avsc"));
    }

    @Test
    public void test10() throws URISyntaxException, IOException, InterruptedException, BloomFilterEncodingException {
        final Schema encodingSchema = loadAvroSchemaFromFile( new File("data/clk.avsc"));
        CLKEncoding encoding = new CLKEncoding();
        encoding.setupFromSchema(encodingSchema);
        assertTrue(encoding.isEncodingOfSchema(schema));
        encodeLocalFile("data/clk_copy", avroFiles, schema, encoding);
    }


    private static Schema loadAvroSchemaFromFile(final File schemaFile) throws IOException {
        FileInputStream fis = new FileInputStream(schemaFile);
        Schema schema = (new Schema.Parser()).parse(fis);
        fis.close();
        return schema;
    }

    private static void saveAvroSchemaToFile(final Schema schema,final File schemaFile) throws IOException {
        FileOutputStream fos = new FileOutputStream(schemaFile,false);
        fos.write(schema.toString(true).getBytes());
        fos.close();
    }
    private static String[] encodeLocalFile(final String name, final Set<File> avroFiles, final Schema schema,
                                            final BloomFilterEncoding encoding)
            throws IOException, BloomFilterEncodingException {
        final Schema encodingSchema = encoding.getEncodingSchema();
        encoding.initialize();

        final File encodedSchemaFile = new File(name + ".avsc");
        encodedSchemaFile.createNewFile();
        final PrintWriter schemaWriter = new PrintWriter(encodedSchemaFile);
        schemaWriter.print(encodingSchema .toString(true));
        schemaWriter.close();

        final File encodedFile = new File(name + ".avro");
        encodedFile.createNewFile();
        final DataFileWriter<GenericRecord> writer =
                new DataFileWriter<GenericRecord>(
                        new GenericDatumWriter<GenericRecord>(encodingSchema));
        writer.create(encodingSchema, encodedFile);
        for (File avroFile : avroFiles) {
            final DataFileReader<GenericRecord> reader =
                    new DataFileReader<GenericRecord>(avroFile,
                            new GenericDatumReader<GenericRecord>(schema));
            for (GenericRecord record : reader) writer.append(encoding.encodeRecord(record));
            reader.close();
        }
        writer.close();

        return new String[]{
                encodedFile.getAbsolutePath(),
                encodedSchemaFile.getAbsolutePath()
        };
    }
}
