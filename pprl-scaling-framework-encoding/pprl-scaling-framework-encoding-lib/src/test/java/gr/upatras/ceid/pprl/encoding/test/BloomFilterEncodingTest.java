package gr.upatras.ceid.pprl.encoding.test;

import gr.upatras.ceid.pprl.encoding.BloomFilterEncoding;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncodingException;
import gr.upatras.ceid.pprl.encoding.FieldBloomFilterEncoding;
import gr.upatras.ceid.pprl.encoding.RowBloomFilterEncoding;
import org.apache.avro.Schema;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.Set;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class BloomFilterEncodingTest {

    private static Logger LOG = LoggerFactory.getLogger(BloomFilterEncodingTest.class);

    private Schema schema;
    private Set<File> avroFiles;

    private static double[] avgQcount = new double[]{16.5,47.5};
    private static double[] weights = new double[]{0.2,0.8};
    private static final String[] REST_FIELDS = new String[]{"key"};
    private static final String[] SELECTED_FIELDS = new String[]{"author","title"};
    private static final int N = 1024;
    private static final int K = 30;
    private static final int Q = 2;


    @Before
    public void setUp() throws URISyntaxException, IOException {
        avroFiles = Collections.singleton(new File("dblp/avro/dblp.avro"));
        File schemaFile = new File("dblp/schema/dblp.avsc");
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
    public void test1() throws URISyntaxException, IOException, InterruptedException, BloomFilterEncodingException {
        FieldBloomFilterEncoding encoding = new FieldBloomFilterEncoding(avgQcount,K,Q);
        encoding.makeFromSchema(schema, SELECTED_FIELDS, REST_FIELDS);
        assertTrue(encoding.isEncodingOfSchema(schema));
        BloomFilterEncoding.encodeLocalFile("dynamic_fbf",avroFiles,schema,encoding);
        saveAvroSchemaToFile(encoding.getEncodingSchema(),new File("dynamic_fbf.avsc"));
    }

    @Test
    public void test2() throws URISyntaxException, IOException, InterruptedException, BloomFilterEncodingException {
        final Schema encodingSchema = loadAvroSchemaFromFile( new File("dynamic_fbf.avsc"));
        FieldBloomFilterEncoding encoding = new FieldBloomFilterEncoding();
        encoding.setupFromSchema(encodingSchema);
        assertTrue(encoding.isEncodingOfSchema(schema));
    }

    @Test
    public void test3() throws URISyntaxException, IOException, InterruptedException, BloomFilterEncodingException {
        FieldBloomFilterEncoding encoding = new FieldBloomFilterEncoding(N,SELECTED_FIELDS.length,K,Q);
        encoding.makeFromSchema(schema, SELECTED_FIELDS, REST_FIELDS);
        assertTrue(encoding.isEncodingOfSchema(schema));
        BloomFilterEncoding.encodeLocalFile("static_fbf", avroFiles, schema, encoding);
        saveAvroSchemaToFile(encoding.getEncodingSchema(),new File("static_fbf.avsc"));
    }

    @Test
    public void test4() throws URISyntaxException, IOException, InterruptedException, BloomFilterEncodingException {
        final Schema encodingSchema = loadAvroSchemaFromFile( new File("static_fbf.avsc"));
        FieldBloomFilterEncoding encoding = new FieldBloomFilterEncoding();
        encoding.setupFromSchema(encodingSchema);
        assertTrue(encoding.isEncodingOfSchema(schema));
    }

    @Test
    public void test5() throws URISyntaxException, IOException, InterruptedException, BloomFilterEncodingException {
        RowBloomFilterEncoding encoding = new RowBloomFilterEncoding(avgQcount, weights, K, Q);
        encoding.makeFromSchema(schema, SELECTED_FIELDS, REST_FIELDS);
        assertTrue(encoding.isEncodingOfSchema(schema));
        BloomFilterEncoding.encodeLocalFile("weighted_rbf", avroFiles, schema, encoding);
        saveAvroSchemaToFile(encoding.getEncodingSchema(),new File("weighted_rbf.avsc"));

    }

    @Test
    public void test6() throws URISyntaxException, IOException, InterruptedException, BloomFilterEncodingException {
        final Schema encodingSchema = loadAvroSchemaFromFile( new File("weighted_rbf.avsc"));
        RowBloomFilterEncoding encoding = new RowBloomFilterEncoding();
        encoding.setupFromSchema(encodingSchema);
        assertTrue(encoding.isEncodingOfSchema(schema));
    }

    @Test
    public void test7() throws URISyntaxException, IOException, InterruptedException, BloomFilterEncodingException {
        RowBloomFilterEncoding encoding = new RowBloomFilterEncoding(avgQcount,N,K,Q);
        encoding.makeFromSchema(schema, SELECTED_FIELDS, REST_FIELDS);
        assertTrue(encoding.isEncodingOfSchema(schema));
        BloomFilterEncoding.encodeLocalFile("uniform_rbf", avroFiles, schema, encoding);
        saveAvroSchemaToFile(encoding.getEncodingSchema(),new File("uniform_rbf.avsc"));
    }

    @Test
    public void test8() throws URISyntaxException, IOException, InterruptedException, BloomFilterEncodingException {
        final Schema encodingSchema = loadAvroSchemaFromFile( new File("uniform_rbf.avsc"));
        RowBloomFilterEncoding encoding = new RowBloomFilterEncoding();
        encoding.setupFromSchema(encodingSchema);
        assertTrue(encoding.isEncodingOfSchema(schema));
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
}
