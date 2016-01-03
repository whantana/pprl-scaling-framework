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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class BloomFilterEncodingTest {

    private static Logger LOG = LoggerFactory.getLogger(BloomFilterEncodingTest.class);

    private Schema schema;

    private static double[] avgQcount = new double[]{16.5,47.5};
    private static final String[] REST_FIELDS = new String[]{"key"};
    private static final String[] SELECTED_FIELDS = new String[]{"author","title"};
    private static final int N = 1024;
    private static final int K = 30;
    private static final int Q = 2;


    @Before
    public void setUp() throws URISyntaxException, IOException {
        File schemaFile = new File("dblp.avsc");
        schema = loadAvroSchemaFromFile(schemaFile);
        assertNotNull(schema);
    }

    @Test
    public void test0() {
        for (int i = 1; i <= 100; i++) {
            double g = Math.pow(0.5, (double) 1 / (i * K));
            int sz = BloomFilterEncoding.dynamicsize(i,K);
            LOG.info(String.format("i=%d - %.6f - %d",i, g,sz));
        }
    }

    @Test
    public void test1() throws URISyntaxException, IOException, InterruptedException, BloomFilterEncodingException {
        FieldBloomFilterEncoding dynFbfe = (FieldBloomFilterEncoding)
                BloomFilterEncoding.newInstanceOfMethod("FBF",BloomFilterEncoding.dynamicsizes(avgQcount,K),K,Q);
        dynFbfe.makeFromSchema(schema,SELECTED_FIELDS,REST_FIELDS);
        assertTrue(dynFbfe.isEncodingOfSchema(schema));
        saveAvroSchemaToFile(dynFbfe.getEncodingSchema(), new File("dyn_fbfe.avsc"));
    }

    @Test
    public void test2() throws URISyntaxException, IOException, InterruptedException, BloomFilterEncodingException {
        final Schema encodingSchema = loadAvroSchemaFromFile( new File("dyn_fbfe.avsc"));
        FieldBloomFilterEncoding dynFbfe = new FieldBloomFilterEncoding();
        dynFbfe.makeFromSchema(encodingSchema);
        assertTrue(dynFbfe.isEncodingOfSchema(schema));
    }

    @Test
    public void test3() throws URISyntaxException, IOException, InterruptedException, BloomFilterEncodingException {
        int[] Ns = new int[SELECTED_FIELDS.length];
        for (int i = 0; i < Ns.length; i++) Ns[i] = N;
        FieldBloomFilterEncoding statFbfe = (FieldBloomFilterEncoding)
                BloomFilterEncoding.newInstanceOfMethod("FBF",Ns,K,Q);
        statFbfe.makeFromSchema(schema,SELECTED_FIELDS,REST_FIELDS);
        assertTrue(statFbfe.isEncodingOfSchema(schema));
        saveAvroSchemaToFile(statFbfe.getEncodingSchema(), new File("stat_fbfe.avsc"));
    }

    @Test
    public void test4() throws URISyntaxException, IOException, InterruptedException, BloomFilterEncodingException {
        final Schema encodingSchema = loadAvroSchemaFromFile( new File("stat_fbfe.avsc"));
        FieldBloomFilterEncoding statFbfe = new FieldBloomFilterEncoding();
        statFbfe.makeFromSchema(encodingSchema);
        assertTrue(statFbfe.isEncodingOfSchema(schema));
    }

    @Test
    public void test5() throws URISyntaxException, IOException, InterruptedException, BloomFilterEncodingException {
        RowBloomFilterEncoding statRbfe = (RowBloomFilterEncoding)
                BloomFilterEncoding.newInstanceOfMethod("RBF", BloomFilterEncoding.dynamicsizes(avgQcount, K), K, Q);
        statRbfe.makeFromSchema(schema,SELECTED_FIELDS,REST_FIELDS);
        assertTrue(statRbfe.isEncodingOfSchema(schema));
        saveAvroSchemaToFile(statRbfe.getEncodingSchema(), new File("dyn_rbfe.avsc"));
    }

    @Test
    public void test6() throws URISyntaxException, IOException, InterruptedException, BloomFilterEncodingException {
        final Schema encodingSchema = loadAvroSchemaFromFile( new File("dyn_rbfe.avsc"));
        FieldBloomFilterEncoding statRbfe = new FieldBloomFilterEncoding();
        statRbfe.makeFromSchema(encodingSchema);
        assertTrue(statRbfe.isEncodingOfSchema(schema));
    }

    @Test
    public void test7() throws URISyntaxException, IOException, InterruptedException, BloomFilterEncodingException {
        int[] Ns = new int[SELECTED_FIELDS.length];
        for (int i = 0; i < Ns.length; i++) Ns[i] = N;
        RowBloomFilterEncoding statRbfe = (RowBloomFilterEncoding)
                BloomFilterEncoding.newInstanceOfMethod("RBF",Ns,K,Q);
        statRbfe.makeFromSchema(schema,SELECTED_FIELDS,REST_FIELDS);
        assertTrue(statRbfe.isEncodingOfSchema(schema));
        saveAvroSchemaToFile(statRbfe.getEncodingSchema(), new File("stat_rbfe.avsc"));
    }

    @Test
    public void test8() throws URISyntaxException, IOException, InterruptedException, BloomFilterEncodingException {
        final Schema encodingSchema = loadAvroSchemaFromFile( new File("stat_fbfe.avsc"));
        FieldBloomFilterEncoding dynFbfe = new FieldBloomFilterEncoding();
        dynFbfe.makeFromSchema(encodingSchema);
        assertTrue(dynFbfe.isEncodingOfSchema(schema));
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
