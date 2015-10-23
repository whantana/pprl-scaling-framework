package gr.upatras.ceid.pprl.encoding.test;

import gr.upatras.ceid.pprl.encoding.EncodingAvroSchemaUtil;
import gr.upatras.ceid.pprl.encoding.FieldBFEncoding;
import gr.upatras.ceid.pprl.encoding.RowBFEncoding;
import gr.upatras.ceid.pprl.encoding.SimpleBFEncoding;
import org.apache.avro.Schema;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class BFEncodingTest {

    private File schemaFile;

    private static final String[] COLUMNS = new String[]{"author","title"};
    private static final int N = 1024;
    private static final int K = 30;
    private static final int Q = 2;

    private static String[] SBF_ENCODING_SCHEMA_NAMES = {"/dblp.avsc","/enc_sbf_"+N+"_"+ K +"_"+"dblp.avsc"};
    private static String[] RBF_ENCODING_SCHEMA_NAMES = {"/dblp.avsc","/enc_rbf_"+N+"_"+ K +"_"+"dblp.avsc"};
    private static String[] FBF_ENCODING_SCHEMAS_NAMES = {"/dblp.avsc","/enc_fbf_"+N+"_"+ K +"_"+"dblp.avsc"};

    @Before
    public void setUp() throws URISyntaxException {
        schemaFile = new File(getClass().getResource(SBF_ENCODING_SCHEMA_NAMES[0]).toURI());
    }

    @Test
    public void test1() throws URISyntaxException, IOException, InterruptedException {
        final Schema schema = EncodingAvroSchemaUtil.loadAvroSchemaFromFile(schemaFile);
        assertNotNull(schema);
        SimpleBFEncoding sbfe = new SimpleBFEncoding(schema,COLUMNS,N, K, Q);
        sbfe.setupEncodingSchema();
        assertTrue(sbfe.validateEncodingSchema());
        EncodingAvroSchemaUtil.saveAvroSchemaToFile(
                sbfe.getEncodingSchema(),new File(schemaFile.getParent() + "/" + SBF_ENCODING_SCHEMA_NAMES[1]));
    }


    @Test
    public void test2() throws URISyntaxException, IOException, InterruptedException {
        final Schema schema = EncodingAvroSchemaUtil.loadAvroSchemaFromFile(schemaFile);
        assertNotNull(schema);
        final Schema encodingSchema =
                EncodingAvroSchemaUtil.loadAvroSchemaFromFile(new File(getClass().getResource(SBF_ENCODING_SCHEMA_NAMES[1]).toURI()));
        assertNotNull(encodingSchema);
        assertNotNull("Schema is null", encodingSchema);
        SimpleBFEncoding sbfe = new SimpleBFEncoding(
                schema, encodingSchema,
                COLUMNS,N, K, Q);
        assertTrue(sbfe.validateEncodingSchema());
    }

    @Test
    public void test3() throws URISyntaxException, IOException, InterruptedException {
        final Schema schema = EncodingAvroSchemaUtil.loadAvroSchemaFromFile(schemaFile);
        assertNotNull(schema);
        FieldBFEncoding fbfe = new FieldBFEncoding(schema,COLUMNS,N, K, Q);
        fbfe.setupEncodingSchema();
        assertTrue(fbfe.validateEncodingSchema());
        EncodingAvroSchemaUtil.saveAvroSchemaToFile(
                fbfe.getEncodingSchema(),new File(schemaFile.getParent() + "/" + FBF_ENCODING_SCHEMAS_NAMES[1]));
    }


    @Test
    public void test4() throws URISyntaxException, IOException, InterruptedException {
        final Schema schema = EncodingAvroSchemaUtil.loadAvroSchemaFromFile(schemaFile);
        assertNotNull(schema);
        final Schema encodingSchema =
                EncodingAvroSchemaUtil.loadAvroSchemaFromFile(new File(getClass().getResource(FBF_ENCODING_SCHEMAS_NAMES[1]).toURI()));
        assertNotNull(encodingSchema);
        assertNotNull("Schema is null", encodingSchema);
        FieldBFEncoding fbfe = new FieldBFEncoding(
                schema, encodingSchema,
                COLUMNS,N, K, Q);
        assertTrue(fbfe.validateEncodingSchema());
    }

    @Test
    public void test5() throws URISyntaxException, IOException, InterruptedException {
        final Schema schema = EncodingAvroSchemaUtil.loadAvroSchemaFromFile(schemaFile);
        assertNotNull(schema);
        RowBFEncoding rbfe = new RowBFEncoding(schema,COLUMNS,N, K, Q);
        rbfe.setupEncodingSchema();
        assertTrue(rbfe.validateEncodingSchema());
        EncodingAvroSchemaUtil.saveAvroSchemaToFile(
                rbfe.getEncodingSchema(),new File(schemaFile.getParent() + "/" + RBF_ENCODING_SCHEMA_NAMES[1]));
    }


    @Test
    public void test6() throws URISyntaxException, IOException, InterruptedException {
        final Schema schema = EncodingAvroSchemaUtil.loadAvroSchemaFromFile(schemaFile);
        assertNotNull(schema);
        final Schema encodingSchema =
                EncodingAvroSchemaUtil.loadAvroSchemaFromFile(new File(getClass().getResource(RBF_ENCODING_SCHEMA_NAMES[1]).toURI()));
        assertNotNull(encodingSchema);
        assertNotNull("Schema is null", encodingSchema);
        RowBFEncoding rbfe = new RowBFEncoding(
                schema, encodingSchema,
                COLUMNS,N, K, Q);
        assertTrue(rbfe.validateEncodingSchema());
    }
}
