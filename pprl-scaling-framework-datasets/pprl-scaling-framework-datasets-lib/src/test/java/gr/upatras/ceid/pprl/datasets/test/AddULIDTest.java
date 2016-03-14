package gr.upatras.ceid.pprl.datasets.test;

import gr.upatras.ceid.pprl.datasets.DatasetsUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class AddULIDTest {

    private static final Logger LOG = LoggerFactory.getLogger(AddULIDTest.class);


    private Schema schema;
    private GenericRecord[] records = new GenericRecord[10];

    @Before
    public void setup() throws IOException {
        schema = loadAvroSchemaFromFile(new File("person_small/schema/person_small.avsc"));
        for (int i = 0; i < 10; i++) {
            records[i] = new GenericData.Record(schema);
            records[i].put("id",String.valueOf(i));
            records[i].put("name",String.format("Name #%d",i));
            records[i].put("surname",String.format("Surname #%d",i));
            records[i].put("location",String.format("Location #%d",i));
        }
        LOG.info(10 + " records ready.");
    }

    @Test
    public void test0() {
        final String ulidFieldName = "my_ulid";
        LOG.info(Arrays.deepToString(records));
        final Schema updatedSchema = DatasetsUtil.addULID(schema,ulidFieldName);
        final GenericRecord[] updatedRecords = DatasetsUtil.addULID(records,updatedSchema,ulidFieldName);
        assert records.length == updatedRecords.length;
        LOG.info(Arrays.deepToString(updatedRecords));
        for (int i = 0; i < updatedRecords.length; i++) {
            assertEquals(records[i].get("id"), updatedRecords[i].get("id"));
            assertEquals(Long.parseLong((String)records[i].get("id")),
                    updatedRecords[i].get(ulidFieldName));
            assertEquals(Long.parseLong((String) updatedRecords[i].get("id")),
                    updatedRecords[i].get(ulidFieldName));
        }
    }


    private static Schema loadAvroSchemaFromFile(final File schemaFile) throws IOException {
        FileInputStream fis = new FileInputStream(schemaFile);
        Schema schema = (new Schema.Parser()).parse(fis);
        fis.close();
        return schema;
    }
}
