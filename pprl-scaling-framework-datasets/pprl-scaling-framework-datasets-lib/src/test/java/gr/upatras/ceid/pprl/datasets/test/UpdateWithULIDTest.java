package gr.upatras.ceid.pprl.datasets.test;

import gr.upatras.ceid.pprl.datasets.DatasetException;
import gr.upatras.ceid.pprl.datasets.DatasetsUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class UpdateWithULIDTest {

    private static final Logger LOG = LoggerFactory.getLogger(UpdateWithULIDTest.class);


    private Schema schema;
    private GenericRecord[] records;

    @Before
    public void setup() throws IOException, DatasetException {
        final FileSystem fs = FileSystem.getLocal(new Configuration());
        schema = DatasetsUtil.loadSchemaFromFSPath(fs,new Path("data/da_int/schema/da_int.avsc"));
        DatasetsUtil.DatasetRecordReader reader = new DatasetsUtil.DatasetRecordReader(fs,schema,
                new Path("data/da_int/avro"));
        final List<GenericRecord> recordList = new ArrayList<GenericRecord>();
        while(reader.hasNext()) recordList.add(reader.next());
        records = new GenericRecord[recordList.size()];
        records = recordList.toArray(records);
        LOG.info(records.length + " records ready.");
    }

    @Test
    public void test0() throws IOException, DatasetException {

        final String ulidFieldName = "my_ulid";
        final Schema updatedSchema = DatasetsUtil.updateSchemaWithULID(schema, ulidFieldName);
        final GenericRecord[] updatedRecords = DatasetsUtil.updateRecordsWithULID(records, updatedSchema, ulidFieldName);

        assertEquals(records.length,updatedRecords.length);
    }
}
