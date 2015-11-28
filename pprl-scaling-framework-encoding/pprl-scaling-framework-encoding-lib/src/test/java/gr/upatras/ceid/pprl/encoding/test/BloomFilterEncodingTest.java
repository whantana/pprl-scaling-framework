package gr.upatras.ceid.pprl.encoding.test;

import gr.upatras.ceid.pprl.encoding.BloomFilterEncodingException;
import gr.upatras.ceid.pprl.encoding.MultiBloomFilterEncoding;
import gr.upatras.ceid.pprl.encoding.RowBloomFilterEncoding;
import gr.upatras.ceid.pprl.encoding.SimpleBloomFilterEncoding;
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
import java.util.Arrays;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class BloomFilterEncodingTest {

    private static Logger LOG = LoggerFactory.getLogger(BloomFilterEncodingTest.class);

    private File schemaFile;

    private static final String UID_COLUMN = "key";
    private static final String[] COLUMNS = new String[]{"author","title"};
    private static final int N = 1024;
    private static final int K = 30;
    private static final int Q = 2;

    private static String[] SBF_ENCODING_SCHEMA_NAMES = {"/dblp.avsc","/enc_simple_"+ N +"_"+ K +"_" + Q + "_" + "dblp.avsc"};
    private static String[] RBF_ENCODING_SCHEMA_NAMES = {"/dblp.avsc","/enc_row_"+ N +"_"+ K +"_" + Q + "_" + "dblp.avsc"};
    private static String[] MBF_ENCODING_SCHEMA_NAMES = {"/dblp.avsc","/enc_multi_"+ N +"_"+ K +"_" + Q + "_" + "dblp.avsc"};

    @Before
    public void setUp() throws URISyntaxException {
        schemaFile = new File(getClass().getResource(SBF_ENCODING_SCHEMA_NAMES[0]).toURI());
    }

    @Test
    public void test1() throws URISyntaxException, IOException, InterruptedException, BloomFilterEncodingException {
        final Schema schema = loadAvroSchemaFromFile(schemaFile);
        assertNotNull(schema);
        SimpleBloomFilterEncoding sbfe =
                new SimpleBloomFilterEncoding(schema, Arrays.asList(COLUMNS),N, K, Q);
        sbfe.createEncodingFields();
        sbfe.generateEncodingSchema();
        assertTrue(sbfe.validateEncodingSchema());
        saveAvroSchemaToFile(
                sbfe.getEncodingSchema(), new File(schemaFile.getParent() + "/" + SBF_ENCODING_SCHEMA_NAMES[1]));
    }


    @Test
    public void test2() throws URISyntaxException, IOException, InterruptedException, BloomFilterEncodingException {
        final Schema schema = loadAvroSchemaFromFile(schemaFile);
        assertNotNull(schema);
        final Schema encodingSchema =
                loadAvroSchemaFromFile(new File(getClass().getResource(SBF_ENCODING_SCHEMA_NAMES[1]).toURI()));
        assertNotNull(encodingSchema);
        assertNotNull("Schema is null", encodingSchema);
        SimpleBloomFilterEncoding sbfe = new SimpleBloomFilterEncoding(
                schema, encodingSchema, Arrays.asList(COLUMNS),N, K, Q);
        sbfe.createEncodingFields();
        assertTrue(sbfe.validateEncodingSchema());
    }

    @Test
    public void test3() throws URISyntaxException, IOException, InterruptedException, BloomFilterEncodingException {
        final Schema schema = loadAvroSchemaFromFile(schemaFile);
        assertNotNull(schema);
        MultiBloomFilterEncoding mbfe = new MultiBloomFilterEncoding(schema, Arrays.asList(COLUMNS),N, K, Q);
        mbfe.createEncodingFields();
        mbfe.generateEncodingSchema();
        assertTrue(mbfe.validateEncodingSchema());
        saveAvroSchemaToFile(
                mbfe.getEncodingSchema(), new File(schemaFile.getParent() + "/" + MBF_ENCODING_SCHEMA_NAMES[1]));
    }


    @Test
    public void test4() throws URISyntaxException, IOException, InterruptedException, BloomFilterEncodingException {
        final Schema schema = loadAvroSchemaFromFile(schemaFile);
        assertNotNull(schema);
        final Schema encodingSchema =
                loadAvroSchemaFromFile(new File(getClass().getResource(MBF_ENCODING_SCHEMA_NAMES[1]).toURI()));
        assertNotNull(encodingSchema);
        assertNotNull("Schema is null", encodingSchema);
        MultiBloomFilterEncoding mbfe = new MultiBloomFilterEncoding(
                schema, encodingSchema, Arrays.asList(COLUMNS),N, K, Q);
        mbfe.createEncodingFields();
        assertTrue(mbfe.validateEncodingSchema());
    }

    @Test
    public void test5() throws URISyntaxException, IOException, InterruptedException, BloomFilterEncodingException {
        final Schema schema = loadAvroSchemaFromFile(schemaFile);
        assertNotNull(schema);
        RowBloomFilterEncoding rbfe =
                new RowBloomFilterEncoding(schema,Arrays.asList(COLUMNS),N, K, Q);
        rbfe.createEncodingFields();
        rbfe.generateEncodingSchema();
        assertTrue(rbfe.validateEncodingSchema());
        saveAvroSchemaToFile(
                rbfe.getEncodingSchema(), new File(schemaFile.getParent() + "/" + RBF_ENCODING_SCHEMA_NAMES[1]));
    }


    @Test
    public void test6() throws URISyntaxException, IOException, InterruptedException, BloomFilterEncodingException {
        final Schema schema = loadAvroSchemaFromFile(schemaFile);
        assertNotNull(schema);
        final Schema encodingSchema =
                loadAvroSchemaFromFile(new File(getClass().getResource(RBF_ENCODING_SCHEMA_NAMES[1]).toURI()));
        assertNotNull(encodingSchema);
        assertNotNull("Schema is null", encodingSchema);
        RowBloomFilterEncoding rbfe = new RowBloomFilterEncoding(
                schema, encodingSchema,Arrays.asList(COLUMNS),N, K, Q);
        rbfe.createEncodingFields();
        assertTrue(rbfe.validateEncodingSchema());
    }

    @Test
    public void test7() throws IOException, URISyntaxException, BloomFilterEncodingException {
        final Schema schema = loadAvroSchemaFromFile(schemaFile);
        assertNotNull(schema);
        Schema encodingSchema;
        encodingSchema =
                loadAvroSchemaFromFile(new File(getClass().getResource(RBF_ENCODING_SCHEMA_NAMES[1]).toURI()));
        assertNotNull(encodingSchema);
        assertNotNull("Schema is null", encodingSchema);
        RowBloomFilterEncoding rbfe = new RowBloomFilterEncoding(
                schema, encodingSchema,Arrays.asList(COLUMNS),N, K, Q);
        encodingSchema =
                loadAvroSchemaFromFile(new File(getClass().getResource(MBF_ENCODING_SCHEMA_NAMES[1]).toURI()));
        assertNotNull(encodingSchema);
        assertNotNull("Schema is null", encodingSchema);
        MultiBloomFilterEncoding mbfe = new MultiBloomFilterEncoding(
                schema, encodingSchema, Arrays.asList(COLUMNS),N, K, Q);
        encodingSchema =
                loadAvroSchemaFromFile(new File(getClass().getResource(SBF_ENCODING_SCHEMA_NAMES[1]).toURI()));
        assertNotNull(encodingSchema);
        assertNotNull("Schema is null", encodingSchema);
        SimpleBloomFilterEncoding sbfe = new SimpleBloomFilterEncoding(
                schema, encodingSchema, Arrays.asList(COLUMNS),N, K, Q);

        rbfe.setupFromEncodingSchema();
        LOG.info(rbfe.getSmallName());
        LOG.info(rbfe.getName());
        LOG.info(rbfe.getEncodingColumnName());
        LOG.info(rbfe.getEncodingColumn().toString());
        LOG.info(rbfe.getRestEncodingColumns().toString());
        assertTrue(rbfe.validateEncodingSchema());

        mbfe.setupFromEncodingSchema();
        LOG.info(mbfe.getSmallName());
        LOG.info(mbfe.getName());
        LOG.info(mbfe.getEncodingColumnForName(COLUMNS[0]).toString());
        LOG.info(mbfe.getEncodingColumnForName(COLUMNS[1]).toString());
        LOG.info(mbfe.getRestEncodingColumns().toString());
        assertTrue(mbfe.validateEncodingSchema());

        sbfe.setupFromEncodingSchema();
        LOG.info(sbfe.getSmallName());
        LOG.info(sbfe.getName());
        LOG.info(sbfe.getEncodingColumnName());
        LOG.info(sbfe.getEncodingColumn().toString());
        LOG.info(sbfe.getRestEncodingColumns().toString());
        assertTrue(sbfe.validateEncodingSchema());
    }

    @Test
    public void test8() throws IOException, URISyntaxException, BloomFilterEncodingException {
        RowBloomFilterEncoding rbfe = new RowBloomFilterEncoding(
                        loadAvroSchemaFromFile(new File(getClass().getResource(RBF_ENCODING_SCHEMA_NAMES[1]).toURI())),N, K, Q);

        SimpleBloomFilterEncoding sbfe = new SimpleBloomFilterEncoding(
                loadAvroSchemaFromFile(new File(getClass().getResource(SBF_ENCODING_SCHEMA_NAMES[1]).toURI())),N, K, Q);

        MultiBloomFilterEncoding mbfe = new MultiBloomFilterEncoding(
                loadAvroSchemaFromFile(new File(getClass().getResource(MBF_ENCODING_SCHEMA_NAMES[1]).toURI())),N, K, Q);

        rbfe.setupFromEncodingSchema();
        LOG.info(rbfe.getSmallName());
        LOG.info(rbfe.getName());
        LOG.info(rbfe.getEncodingColumnName());
        LOG.info(rbfe.getEncodingColumn().toString());
        LOG.info(rbfe.getRestEncodingColumns().toString());

        mbfe.setupFromEncodingSchema();
        LOG.info(mbfe.getSmallName());
        LOG.info(mbfe.getName());
        LOG.info(mbfe.getEncodingColumnForName(COLUMNS[0]).toString());
        LOG.info(mbfe.getEncodingColumnForName(COLUMNS[1]).toString());
        LOG.info(mbfe.getRestEncodingColumns().toString());

        sbfe.setupFromEncodingSchema();
        LOG.info(sbfe.getSmallName());
        LOG.info(sbfe.getName());
        LOG.info(sbfe.getEncodingColumnName());
        LOG.info(sbfe.getEncodingColumn().toString());
        LOG.info(sbfe.getRestEncodingColumns().toString());
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
