package gr.upatras.ceid.pprl.encoding.test;

import gr.upatras.ceid.pprl.encoding.BloomFilterEncoding;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncodingException;
import gr.upatras.ceid.pprl.encoding.FieldBloomFilterEncoding;
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
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class BloomFilterEncodingTest {

    private static Logger LOG = LoggerFactory.getLogger(BloomFilterEncodingTest.class);

    private File schemaFile;
    private Schema schema;

    private static final String UID_COLUMN = "key";
    private static final String[] COLUMNS = new String[]{"author","title"};
    private static final int N = 1024;
    private static final int K = 30;
    private static final int Q = 2;
    private static final double[] AVG_Q_GRAMS = new double[]{2.5,5.0};


    @Before
    public void setUp() throws URISyntaxException, IOException {
        schemaFile = new File(getClass().getResource("/dblp.avsc").toURI());
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
        BloomFilterEncoding staticBfe = new BloomFilterEncoding(N,K,Q);
        staticBfe.makeFromSchema(schema,COLUMNS,new String[]{UID_COLUMN});
        assertTrue(staticBfe.isEncodingOfSchema(schema));
        saveAvroSchemaToFile(staticBfe.getEncodingSchema(),new File(schemaFile.getParent() + "/stat_bfe.avsc"));
    }

    @Test
    public void test2() throws URISyntaxException, IOException, InterruptedException, BloomFilterEncodingException {
        final Schema encodingSchema = loadAvroSchemaFromFile(new File(schemaFile.getParent() + "/stat_bfe.avsc"));
        BloomFilterEncoding staticBfe = new BloomFilterEncoding();
        staticBfe.makeFromSchema(encodingSchema);
        assertTrue(staticBfe.isEncodingOfSchema(schema));
    }

    @Test
    public void test3() throws URISyntaxException, IOException, InterruptedException, BloomFilterEncodingException {
        BloomFilterEncoding dynBfe = new BloomFilterEncoding(AVG_Q_GRAMS,K,Q);
        dynBfe.makeFromSchema(schema,COLUMNS,new String[]{UID_COLUMN});
        assertTrue(dynBfe.isEncodingOfSchema(schema));
        saveAvroSchemaToFile(dynBfe.getEncodingSchema(), new File(schemaFile.getParent() + "/dyn_bfe.avsc"));
    }

    @Test
    public void test4() throws URISyntaxException, IOException, InterruptedException, BloomFilterEncodingException {
        final Schema encodingSchema = loadAvroSchemaFromFile( new File(schemaFile.getParent() + "/dyn_bfe.avsc"));
        BloomFilterEncoding dynBfe = new BloomFilterEncoding();
        dynBfe.makeFromSchema(encodingSchema);
        assertTrue(dynBfe.isEncodingOfSchema(schema));
    }

    @Test
    public void test5() throws URISyntaxException, IOException, InterruptedException, BloomFilterEncodingException {
        FieldBloomFilterEncoding dynFbfe =
                FieldBloomFilterEncoding.makeFromSchema(schema,
                        COLUMNS,new String[]{UID_COLUMN},
                        AVG_Q_GRAMS,K,Q);
        assertTrue(dynFbfe.isEncodingOfSchema(schema));
        saveAvroSchemaToFile(dynFbfe.getEncodingSchema(), new File(schemaFile.getParent() + "/dyn_fbfe.avsc"));
    }

    @Test
    public void test6() throws URISyntaxException, IOException, InterruptedException, BloomFilterEncodingException {
        final Schema encodingSchema = loadAvroSchemaFromFile( new File(schemaFile.getParent() + "/dyn_fbfe.avsc"));
        FieldBloomFilterEncoding dynFbfe = new FieldBloomFilterEncoding();
        dynFbfe.makeFromSchema(encodingSchema);
        assertTrue(dynFbfe.isEncodingOfSchema(schema));
    }

    @Test
    public void test7() throws URISyntaxException, IOException, InterruptedException, BloomFilterEncodingException {
        FieldBloomFilterEncoding statFbfe =
                FieldBloomFilterEncoding.makeFromSchema(schema,
                        COLUMNS,new String[]{UID_COLUMN},
                        N,K,Q);
        assertTrue(statFbfe.isEncodingOfSchema(schema));
        saveAvroSchemaToFile(statFbfe.getEncodingSchema(), new File(schemaFile.getParent() + "/stat_fbfe.avsc"));
    }

    @Test
    public void test8() throws URISyntaxException, IOException, InterruptedException, BloomFilterEncodingException {
        final Schema encodingSchema = loadAvroSchemaFromFile( new File(schemaFile.getParent() + "/stat_fbfe.avsc"));
        FieldBloomFilterEncoding statFbfe = new FieldBloomFilterEncoding();
        statFbfe.makeFromSchema(encodingSchema);
        assertTrue(statFbfe.isEncodingOfSchema(schema));
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
