package gr.upatras.ceid.pprl.test;

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

        final GenericRecord[] sample = DatasetsUtil.sampleDataset(records,5);

        LOG.info(sample.length + " records ready.");
        final String ulidFieldName = "my_id";
        final Schema updatedSchema = DatasetsUtil.updateSchemaWithUID(schema, ulidFieldName);
        final GenericRecord[] updatedRecords = DatasetsUtil.updateRecordsWithUID(sample, schema, ulidFieldName);

        assertEquals(sample.length,updatedRecords.length);

        LOG.info(DatasetsUtil.prettyRecords(updatedRecords,updatedSchema));
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
        final Schema sortedSchema =
                DatasetsUtil.updateSchemaWithOrderByFields(schema,fieldNames,orders);
        final GenericRecord[] sortedRecords =
                DatasetsUtil.updateRecordsWithOrderByFields(records, schema,fieldNames);

        final Schema updatedSchema =
                DatasetsUtil.updateSchemaWithUID(sortedSchema, "uiid");
        final GenericRecord [] updatedRecords =
                DatasetsUtil.updateRecordsWithUID(sortedRecords, sortedSchema, "uiid");


        final Path[] paths = DatasetsUtil.createDatasetDirectories(fs,"person_small_sorted",new Path("data"));
        final Path basePath = paths[0];
        final Path avroPath = paths[1];
        final Path schemaPath = paths[2];
        DatasetsUtil.saveSchemaToFSPath(fs,updatedSchema,new Path(schemaPath,"person_small_sorted.avsc"));

        DatasetsUtil.DatasetRecordWriter writer =
                new DatasetsUtil.DatasetRecordWriter
                        (fs,"person_small_sorted", updatedSchema, avroPath);
        writer.writeRecords(updatedRecords);
        writer.close();
        LOG.info("Sorted Dataset at {}", basePath);
    }

    @Test
    public void test3() throws IOException, DatasetException {
        final FileSystem fs = FileSystem.getLocal(new Configuration());
        final Schema schema = DatasetsUtil.loadSchemaFromFSPath(fs,
                new Path("data/person_small_sorted/schema/person_small_sorted.avsc"));
        DatasetsUtil.DatasetRecordReader reader = new DatasetsUtil.DatasetRecordReader(fs,schema,
                new Path("data/person_small_sorted/avro"));
        LOG.info("Updated schema : " + schema);
        final List<GenericRecord> recordList = new ArrayList<GenericRecord>();
        while(reader.hasNext()) recordList.add(reader.next());
        GenericRecord[] records = new GenericRecord[recordList.size()];
        records = recordList.toArray(records);
        LOG.info(DatasetsUtil.prettyRecords(records,schema));
    }
}
