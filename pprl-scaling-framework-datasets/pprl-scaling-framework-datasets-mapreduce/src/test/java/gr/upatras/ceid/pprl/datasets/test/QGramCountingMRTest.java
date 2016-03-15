package gr.upatras.ceid.pprl.datasets.test;


import gr.upatras.ceid.pprl.datasets.mapreduce.DblpXmlToAvroTool;
import gr.upatras.ceid.pprl.datasets.mapreduce.QGramCountingMapper;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.hadoop.io.AvroSerialization;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class QGramCountingMRTest {
    private static final Logger LOG = LoggerFactory.getLogger(QGramCountingMRTest.class);
    private static final String[] fieldNames = {"name","surname","location"};
    private MapDriver<AvroKey<GenericRecord>, NullWritable , NullWritable  ,NullWritable > mapDriver;
    private GenericRecord[] records;


    @Before
    public void setup() throws IOException {
        Schema schema = loadAvroSchemaFromFile(
                new File("person_small/schema/person_small.avsc"));
        records = loadAvroRecordsFromFiles(schema, new File[]{
                new File("person_small/avro/person_small.avro")});

        mapDriver = MapDriver.newMapDriver(new QGramCountingMapper());
        mapDriver.getContext().getConfiguration().set(QGramCountingMapper.SCHEMA_KEY, schema.toString());
        mapDriver.getContext().getConfiguration().setStrings(QGramCountingMapper.FIELD_NAMES_KEY, fieldNames);
        AvroSerialization.setKeyWriterSchema(mapDriver.getConfiguration(), schema);
        AvroSerialization.setKeyReaderSchema(mapDriver.getConfiguration(), schema);
        mapDriver.setOutputSerializationConfiguration(mapDriver.getConfiguration());
        AvroSerialization.addToConfiguration(mapDriver.getOutputSerializationConfiguration());
    }

    @Test
    public void test0() throws IOException {
        List<Pair<AvroKey<GenericRecord>,NullWritable>> input =
                new ArrayList<Pair<AvroKey<GenericRecord>,NullWritable>>();
        for (GenericRecord record : records)
            input.add(new Pair<AvroKey<GenericRecord>, NullWritable>(
                    new AvroKey<GenericRecord>(record),NullWritable.get()));
        mapDriver.withAll(input);
        mapDriver.run();
        long recordCount = mapDriver.getCounters().findCounter("","record.count").getValue();
        LOG.info("Record count : {}",recordCount);
        for (String counterGroupName : mapDriver.getCounters().getGroupNames()) {
            if(counterGroupName.equals("")) continue;
            for (String counterName : QGramCountingMapper.STATISTICS) {
                long val = mapDriver.getCounters().findCounter(counterGroupName, counterName).getValue();
                final String key = counterGroupName + ".avg." + counterName;
                double avg = (double) val / (double) recordCount;
                LOG.info("Key = {} , value = {}", key, avg);
            }
        }

        assertEquals(recordCount, 120);

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
