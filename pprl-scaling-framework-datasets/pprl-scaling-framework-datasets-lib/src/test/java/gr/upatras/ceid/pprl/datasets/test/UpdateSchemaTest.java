package gr.upatras.ceid.pprl.datasets.test;

import gr.upatras.ceid.pprl.datasets.DatasetException;
import gr.upatras.ceid.pprl.datasets.DatasetsUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class UpdateSchemaTest {

    private static final Logger LOG = LoggerFactory.getLogger(UpdateSchemaTest.class);

    @Test
    public void test0() throws IOException, DatasetException {

        final FileSystem fs = FileSystem.getLocal(new Configuration());
        Schema schema = DatasetsUtil.loadSchemaFromFSPath(fs,new Path("data/da_int/schema/da_int.avsc"));
        DatasetsUtil.DatasetRecordReader reader = new DatasetsUtil.DatasetRecordReader(fs,schema,
                new Path("data/da_int/avro"));
        final List<GenericRecord> recordList = new ArrayList<GenericRecord>();
        while(reader.hasNext()) recordList.add(reader.next());
        GenericRecord[] records = new GenericRecord[recordList.size()];
        records = recordList.toArray(records);
        LOG.info(records.length + " records ready.");

        final String ulidFieldName = "my_ulid";
        final Schema updatedSchema = DatasetsUtil.updateSchemaWithULID(schema, ulidFieldName);
        final GenericRecord[] updatedRecords = DatasetsUtil.updateRecordsWithULID(records, updatedSchema, ulidFieldName);

        assertEquals(records.length,updatedRecords.length);

        final Path[] paths = DatasetsUtil.createDatasetDirectories(fs,"da_int_1",new Path("data"));
        DatasetsUtil.saveSchemaToFSPath(fs,updatedSchema,new Path(paths[2],"da_int_1.avsc"));
        DatasetsUtil.DatasetRecordWriter writer =  new DatasetsUtil.DatasetRecordWriter(fs,"da_int_1", updatedSchema,paths[1]);
        writer.writeRecords(updatedRecords);
        writer.close();
    }

    @Test
    public void test1() throws IOException, DatasetException {
        final FileSystem fs = FileSystem.getLocal(new Configuration());
        Schema schema = DatasetsUtil.loadSchemaFromFSPath(fs,new Path("data/da_int/schema/da_int.avsc"));
        LOG.info("Schema after load " + schema.toString());
    }

    @Test
    public void test2() throws IOException, DatasetException {
        final FileSystem fs = FileSystem.getLocal(new Configuration());
        final Schema schema = DatasetsUtil.loadSchemaFromFSPath(fs,
                new Path("data/person_small/schema/person_small.avsc"));
        DatasetsUtil.DatasetRecordReader reader = new DatasetsUtil.DatasetRecordReader(fs,schema,
                new Path("data/person_small/avro"));
        final List<GenericRecord> recordList = new ArrayList<GenericRecord>();
        while(reader.hasNext()) recordList.add(reader.next());
        GenericRecord[] records = new GenericRecord[recordList.size()];
        records = recordList.toArray(records);

        final String[] fieldNames = new String[]{"name", "surname"};
        final Schema.Field.Order[] orders = new Schema.Field.Order[]{
                Schema.Field.Order.ASCENDING,Schema.Field.Order.ASCENDING};
        final Schema updatedSchema =
                DatasetsUtil.updateSchemaWithOrderByFields(schema,fieldNames,orders);
        final Path[] paths = DatasetsUtil.createDatasetDirectories(fs,"person_small_sorted",new Path("data"));
        DatasetsUtil.saveSchemaToFSPath(fs,updatedSchema,new Path(paths[2],"person_small_sorted.avsc"));
        final GenericRecord[] updatedRecords =
                DatasetsUtil.updateRecordsWithOrderByFields(records, updatedSchema);
        DatasetsUtil.DatasetRecordWriter writer =
                new DatasetsUtil.DatasetRecordWriter
                        (fs,"person_small_sorted", updatedSchema, paths[1]);
        writer.writeRecords(updatedRecords);
        writer.close();
    }

    @Test
    public void test3() throws IOException, DatasetException {
        final FileSystem fs = FileSystem.getLocal(new Configuration());
        final Schema schema = DatasetsUtil.loadSchemaFromFSPath(fs,
                new Path("data/person_small_sorted/schema/person_small_sorted.avsc"));
        DatasetsUtil.DatasetRecordReader reader = new DatasetsUtil.DatasetRecordReader(fs,schema,
                new Path("data/person_small_sorted/avro"));
        LOG.info("Updated schema : " + schema);
        while(reader.hasNext()) {
            LOG.info("Sorted record : " + reader.next());
        }
    }
}
