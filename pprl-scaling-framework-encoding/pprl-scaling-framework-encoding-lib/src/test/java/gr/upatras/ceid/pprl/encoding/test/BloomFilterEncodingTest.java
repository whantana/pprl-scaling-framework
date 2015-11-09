package gr.upatras.ceid.pprl.encoding.test;

import gr.upatras.ceid.pprl.encoding.BloomFilterEncodingException;
import gr.upatras.ceid.pprl.encoding.EncodingAvroSchemaUtil;
import gr.upatras.ceid.pprl.encoding.MultiBloomFilterEncoding;
import gr.upatras.ceid.pprl.encoding.RowBloomFilterEncoding;
import gr.upatras.ceid.pprl.encoding.SimpleBloomFilterEncoding;
import org.apache.avro.Schema;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class BloomFilterEncodingTest {

    private File schemaFile;

    private static final String UID_COLUMN = "key";
    private static final String[] COLUMNS = new String[]{"author","title"};
    private static final int N = 1024;
    private static final int K = 30;
    private static final int Q = 2;

    private static String[] SBF_ENCODING_SCHEMA_NAMES = {"/dblp.avsc","/enc_simple_"+ N +"_"+ K +"_" + Q + "_" + "dblp.avsc"};
    private static String[] RBF_ENCODING_SCHEMA_NAMES = {"/dblp.avsc","/enc_row_"+ N +"_"+ K +"_" + Q + "_" + "dblp.avsc"};
    private static String[] FBF_ENCODING_SCHEMA_NAMES = {"/dblp.avsc","/enc_multi_"+ N +"_"+ K +"_" + Q + "_" + "dblp.avsc"};

    @Before
    public void setUp() throws URISyntaxException {
        schemaFile = new File(getClass().getResource(SBF_ENCODING_SCHEMA_NAMES[0]).toURI());
    }

    @Test
    public void test1() throws URISyntaxException, IOException, InterruptedException, BloomFilterEncodingException {
        final Schema schema = EncodingAvroSchemaUtil.loadAvroSchemaFromFile(schemaFile);
        assertNotNull(schema);
        SimpleBloomFilterEncoding sbfe =
                new SimpleBloomFilterEncoding(schema,UID_COLUMN, Arrays.asList(COLUMNS),N, K, Q);
        sbfe.makeEncodingSchema();
        assertTrue(sbfe.validateEncodingSchema());
        EncodingAvroSchemaUtil.saveAvroSchemaToFile(
                sbfe.getEncodingSchema(),new File(schemaFile.getParent() + "/" + SBF_ENCODING_SCHEMA_NAMES[1]));
    }


    @Test
    public void test2() throws URISyntaxException, IOException, InterruptedException, BloomFilterEncodingException {
        final Schema schema = EncodingAvroSchemaUtil.loadAvroSchemaFromFile(schemaFile);
        assertNotNull(schema);
        final Schema encodingSchema =
                EncodingAvroSchemaUtil.loadAvroSchemaFromFile(new File(getClass().getResource(SBF_ENCODING_SCHEMA_NAMES[1]).toURI()));
        assertNotNull(encodingSchema);
        assertNotNull("Schema is null", encodingSchema);
        SimpleBloomFilterEncoding sbfe = new SimpleBloomFilterEncoding(
                schema, encodingSchema,UID_COLUMN,
                Arrays.asList(COLUMNS),N, K, Q);
        assertTrue(sbfe.validateEncodingSchema());
    }

    @Test
    public void test3() throws URISyntaxException, IOException, InterruptedException, BloomFilterEncodingException {
        final Schema schema = EncodingAvroSchemaUtil.loadAvroSchemaFromFile(schemaFile);
        assertNotNull(schema);
        MultiBloomFilterEncoding fbfe =
                new MultiBloomFilterEncoding(schema,UID_COLUMN, Arrays.asList(COLUMNS),N, K, Q);
        fbfe.makeEncodingSchema();
        assertTrue(fbfe.validateEncodingSchema());
        EncodingAvroSchemaUtil.saveAvroSchemaToFile(
                fbfe.getEncodingSchema(),new File(schemaFile.getParent() + "/" + FBF_ENCODING_SCHEMA_NAMES[1]));
    }


    @Test
    public void test4() throws URISyntaxException, IOException, InterruptedException, BloomFilterEncodingException {
        final Schema schema = EncodingAvroSchemaUtil.loadAvroSchemaFromFile(schemaFile);
        assertNotNull(schema);
        final Schema encodingSchema =
                EncodingAvroSchemaUtil.loadAvroSchemaFromFile(new File(getClass().getResource(FBF_ENCODING_SCHEMA_NAMES[1]).toURI()));
        assertNotNull(encodingSchema);
        assertNotNull("Schema is null", encodingSchema);
        MultiBloomFilterEncoding fbfe = new MultiBloomFilterEncoding(
                schema, encodingSchema,UID_COLUMN,
                Arrays.asList(COLUMNS),N, K, Q);
        assertTrue(fbfe.validateEncodingSchema());
    }

    @Test
    public void test5() throws URISyntaxException, IOException, InterruptedException, BloomFilterEncodingException {
        final Schema schema = EncodingAvroSchemaUtil.loadAvroSchemaFromFile(schemaFile);
        assertNotNull(schema);
        RowBloomFilterEncoding rbfe =
                new RowBloomFilterEncoding(schema,UID_COLUMN, Arrays.asList(COLUMNS),N, K, Q);
        rbfe.makeEncodingSchema();
        assertTrue(rbfe.validateEncodingSchema());
        EncodingAvroSchemaUtil.saveAvroSchemaToFile(
                rbfe.getEncodingSchema(),new File(schemaFile.getParent() + "/" + RBF_ENCODING_SCHEMA_NAMES[1]));
    }


    @Test
    public void test6() throws URISyntaxException, IOException, InterruptedException, BloomFilterEncodingException {
        final Schema schema = EncodingAvroSchemaUtil.loadAvroSchemaFromFile(schemaFile);
        assertNotNull(schema);
        final Schema encodingSchema =
                EncodingAvroSchemaUtil.loadAvroSchemaFromFile(new File(getClass().getResource(RBF_ENCODING_SCHEMA_NAMES[1]).toURI()));
        assertNotNull(encodingSchema);
        assertNotNull("Schema is null", encodingSchema);
        RowBloomFilterEncoding rbfe = new RowBloomFilterEncoding(
                schema, encodingSchema,UID_COLUMN,
                Arrays.asList(COLUMNS),N, K, Q);
        assertTrue(rbfe.validateEncodingSchema());
    }

    @Test
    public void test7() throws URISyntaxException, IOException, BloomFilterEncodingException {
        RowBloomFilterEncoding rbfe = new RowBloomFilterEncoding(
                EncodingAvroSchemaUtil.loadAvroSchemaFromFile(schemaFile),
                EncodingAvroSchemaUtil.loadAvroSchemaFromFile(new File(getClass().getResource(RBF_ENCODING_SCHEMA_NAMES[1]).toURI())),
                UID_COLUMN,
                Arrays.asList(COLUMNS),N, K, Q);

        SimpleBloomFilterEncoding sbfe = new SimpleBloomFilterEncoding(
                EncodingAvroSchemaUtil.loadAvroSchemaFromFile(schemaFile),
                EncodingAvroSchemaUtil.loadAvroSchemaFromFile(new File(getClass().getResource(SBF_ENCODING_SCHEMA_NAMES[1]).toURI())),
                UID_COLUMN,
                Arrays.asList(COLUMNS),N, K, Q);

        MultiBloomFilterEncoding fbfe = new MultiBloomFilterEncoding(
                EncodingAvroSchemaUtil.loadAvroSchemaFromFile(schemaFile),
                EncodingAvroSchemaUtil.loadAvroSchemaFromFile(new File(getClass().getResource(FBF_ENCODING_SCHEMA_NAMES[1]).toURI())),
                UID_COLUMN,
                Arrays.asList(COLUMNS),N, K, Q);

        assertEquals(rbfe.getEncodingColumnNames().size(), 1);
        assertEquals(sbfe.getEncodingColumnNames().size(), 1);
        assertEquals(fbfe.getEncodingColumnNames().size(), 2);

        assertEquals(rbfe.getEncodingColumns().size(), 1);
        assertEquals(sbfe.getEncodingColumns().size(), 1);
        assertEquals(fbfe.getEncodingColumns().size(), 2);

        for (String s : fbfe.getSelectedColumnNames()) {
            assertTrue(fbfe.getEncodingColumnNames().get(fbfe.getSelectedColumnNames().indexOf(s)).contains(s));
            assertTrue(fbfe.getEncodingColumns().contains(fbfe.getEncodingColumnForName(s)));
        }
    }
}
