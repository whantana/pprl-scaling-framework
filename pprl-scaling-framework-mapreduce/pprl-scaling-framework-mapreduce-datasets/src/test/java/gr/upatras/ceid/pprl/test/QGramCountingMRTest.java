package gr.upatras.ceid.pprl.test;

import gr.upatras.ceid.pprl.mapreduce.QGramCountingMapper;
import gr.upatras.ceid.pprl.mapreduce.QGramCountingTool;
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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class QGramCountingMRTest {
    private static final String[] fieldNames = {"name","surname","location"};
    private MapDriver<AvroKey<GenericRecord>, NullWritable , NullWritable  ,NullWritable > mapDriver;
    private GenericRecord[] records;


    @Before
    public void setup() throws IOException {
        Schema schema = loadAvroSchemaFromFile(
                new File("data/person_small/schema/person_small.avsc"));
        records = loadAvroRecordsFromFiles(schema, new File[]{
                new File("data/person_small/avro/person_small.avro")});

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

        final Properties p = QGramCountingTool.counters2Properties(mapDriver.getCounters(),fieldNames);
        p.list(System.out);
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
