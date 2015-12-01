package gr.upatras.ceid.pprl.datasets.test;

import gr.upatras.ceid.pprl.datasets.avro.dblp.DblpPublication;
import gr.upatras.ceid.pprl.datasets.input.MultiTagXmlInputFormat;
import gr.upatras.ceid.pprl.datasets.mapreduce.DblpXmlToAvroMapper;
import org.apache.avro.hadoop.io.AvroSerialization;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class DblpXmlToAvroMRTest {

    private LongWritable firstKey;
    private Text firstValue;
    private static final String EXPECTED_KEY = "journals/acta/Saxena96";
    private static final String EXPECTED_AUTHOR = "Sanjeev Saxena";
    private static final String EXPECTED_TITLE = "Parallel Integer Sorting and Simulation Amongst CRCW Models.";
    private static final String EXPECTED_YEAR = "1996";

    private MapDriver<LongWritable, Text, AvroKey<DblpPublication>,NullWritable > mapDriver;

    @Before
    public void setUp() throws URISyntaxException, IOException, InterruptedException {
        // find test file
        final URL resourceUrl = getClass().getResource("/dblp.xml");
        assertNotNull("Test file missing", resourceUrl);
        File testFile = new File(resourceUrl.toURI());
        assertNotNull("Test file missing",testFile);

        // set configuration
        final Configuration conf = new Configuration(false);
        conf.setStrings(MultiTagXmlInputFormat.TAGS_KEY,
                "article","phdthesis","mastersthesis");

        // open reader and read first value
        final FileSplit split = new FileSplit(
                new Path(testFile.getAbsoluteFile().toURI()), 0, testFile.length(), null);
        MultiTagXmlInputFormat inputFormat = ReflectionUtils.newInstance(MultiTagXmlInputFormat.class, conf);
        TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
        RecordReader<LongWritable, Text> reader = inputFormat.createRecordReader(split, context);
        reader.initialize(split, context);
        boolean foundKv = reader.nextKeyValue();
        assertTrue(foundKv);
        firstKey = reader.getCurrentKey();
        assertNotNull(firstKey);
        firstValue = reader.getCurrentValue();
        assertNotNull(firstValue);

        // Mapper/Reducer
        mapDriver = MapDriver.newMapDriver(new DblpXmlToAvroMapper());
        AvroSerialization.addToConfiguration(mapDriver.getConfiguration());
        mapDriver.getConfiguration().set("avro.serialization.key.writer.schema",
                DblpPublication.getClassSchema().toString(true));
    }

    @Test
    public void test1() throws IOException {
        mapDriver.withInput(firstKey, firstValue);
        mapDriver.withOutput(
                new AvroKey<DblpPublication>(new DblpPublication(EXPECTED_KEY,EXPECTED_AUTHOR,EXPECTED_TITLE,EXPECTED_YEAR)),
                NullWritable.get());
        mapDriver.runTest();
    }
}
