package gr.upatras.ceid.pprl.encoding.test;

import gr.upatras.ceid.pprl.encoding.EncodingAvroSchemaUtil;
import org.apache.avro.Schema;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class EncodingAvroSchemaUtilTest {

    private static final List<String> COLUMNS = Arrays.asList("author","title");
    private static final String METHOD = "FBF";

    @Test
    public void testEncodingDatasetSchema() throws URISyntaxException, IOException, InterruptedException {
        // find test file
        final URL schemaUrl = getClass().getResource("/dblp.avsc");
        assertNotNull("Test file missing", schemaUrl);
        final Schema schema = new Schema.Parser().parse(new File(schemaUrl.toURI()));
        assertNotNull("Schema is null", schema);
        final Schema encodingSchema = EncodingAvroSchemaUtil.createEncodingSchema(schema, COLUMNS, METHOD);
        assertTrue(EncodingAvroSchemaUtil.validateEncocingSchema(schema, encodingSchema, COLUMNS, METHOD));
    }


    @Test
    public void testValidationEncodingDatasetSchema() throws URISyntaxException, IOException, InterruptedException {
        // find test file
        final Schema schema = new Schema.Parser().parse(new File(getClass().getResource("/dblp.avsc").toURI()));
        assertNotNull("Schema is null", schema);
        final Schema encodingSchema = new Schema.Parser().parse(new File(getClass().getResource("/enc_dblp.avsc").toURI()));
        assertNotNull("Schema is null", encodingSchema);

        assertTrue(EncodingAvroSchemaUtil.validateEncocingSchema(schema, encodingSchema, COLUMNS, METHOD));
    }
}
