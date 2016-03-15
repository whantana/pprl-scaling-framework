package gr.upatras.ceid.pprl.datasets.test;

import gr.upatras.ceid.pprl.datasets.DatasetsUtil;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class UpdateWithULIDTest {

    private static final Logger LOG = LoggerFactory.getLogger(UpdateWithULIDTest.class);


    private Schema schema;
    private GenericRecord[] records;

    @Before
    public void setup() throws IOException {
        schema = loadAvroSchemaFromFile(new File("person_small/schema/person_small.avsc"));
        records = loadAvroRecordsFromFiles(schema,new File[]{new File("person_small/avro/person_small.avro")});
        LOG.info(records.length + " records ready.");
    }

    @Test
    public void test0() {
        final String ulidFieldName = "my_ulid";
        final Schema updatedSchema = DatasetsUtil.updateSchemaWithULID(schema, ulidFieldName);
        final GenericRecord[] updatedRecords = DatasetsUtil.updateRecordsWithULID(records, updatedSchema, ulidFieldName);
        assert records.length == updatedRecords.length;
        for (int i = 0; i < updatedRecords.length; i++) {
            assertEquals(records[i].get("id"), updatedRecords[i].get("id"));
            assertEquals(records[i].get("id"),updatedRecords[i].get(ulidFieldName));
            assertEquals(updatedRecords[i].get("id"),updatedRecords[i].get(ulidFieldName));
        }
    }


    private static Schema loadAvroSchemaFromFile(final File schemaFile) throws IOException {
        FileInputStream fis = new FileInputStream(schemaFile);
        Schema schema = (new Schema.Parser()).parse(fis);
        fis.close();
        return schema;
    }

    private static GenericRecord[] loadAvroRecordsFromFiles(final Schema schema,final File[] avroFiles) throws IOException {
        final List<GenericRecord> recordList =  new ArrayList<GenericRecord>();
        int i = 0;
        for (File avroFile : avroFiles) {
            final DataFileReader<GenericRecord> reader =
                    new DataFileReader<GenericRecord>(avroFile,
                            new GenericDatumReader<GenericRecord>(schema));
            for (GenericRecord record : reader) recordList.add(i++,record);
            reader.close();
        }
        return recordList.toArray(new GenericRecord[recordList.size()]);
    }
}
