package gr.upatras.ceid.pprl.datasets.test;


import gr.upatras.ceid.pprl.datasets.input.MultiTagXmlInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class MultiTagXmlInputFormatTest {
    private Configuration conf;
    private FileSplit split;
    private FileSplit split1;
    private FileSplit split2;

    private final int SAMPLE_ARTICLE_TAG_PAIR_COUNT = 9;
    private final int SAMPLE_PHD_TAG_PAIR_COUNT = 8;
    private final int SAMPLE_MSC_ARTICLE_TAG_PAIR_COUNT = 1;

    @Before
    public void setUp() throws URISyntaxException {
        URL resourceUrl = getClass().getResource("/dblp_sample.xml");
        assertNotNull("Test file missing", resourceUrl);
        File testFile = new File(resourceUrl.toURI());
        conf = new Configuration(false);
        split = new FileSplit(new Path(testFile.getAbsoluteFile().toURI()), 0, testFile.length(), null);
        long splitPos = testFile.length() / 2;
        split1 =  new FileSplit(new Path(testFile.getAbsoluteFile().toURI()), 0, splitPos, null);
        split2 =  new FileSplit(new Path(testFile.getAbsoluteFile().toURI()), splitPos , testFile.length() - splitPos, null);
    }

    @Test
    public void testMultiTagXmlInputFormat1() throws IOException, InterruptedException {
        conf.setStrings(MultiTagXmlInputFormat.TAGS_KEY,
                "article","phdthesis","mastersthesis");
        MultiTagXmlInputFormat inputFormat = ReflectionUtils.newInstance(MultiTagXmlInputFormat.class, conf);
        TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
        RecordReader<LongWritable, Text> reader = inputFormat.createRecordReader(split, context);
        reader.initialize(split, context);
        boolean foundKv = reader.nextKeyValue();
        int i = 0;
        while(foundKv) {
            foundKv = reader.nextKeyValue();
            i++;
        }
        int SAMPLE_TOTAL_TAG_PAIR_COUNT = SAMPLE_ARTICLE_TAG_PAIR_COUNT +
                SAMPLE_PHD_TAG_PAIR_COUNT +
                SAMPLE_MSC_ARTICLE_TAG_PAIR_COUNT;
        assertEquals(SAMPLE_TOTAL_TAG_PAIR_COUNT,i);
    }

    @Test
    public void testMultiTagXmlInputFormat2() throws IOException, InterruptedException {
        conf.setStrings(MultiTagXmlInputFormat.TAGS_KEY, "article","phdthesis");
        MultiTagXmlInputFormat inputFormat = ReflectionUtils.newInstance(MultiTagXmlInputFormat.class, conf);
        TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
        RecordReader<LongWritable, Text> reader = inputFormat.createRecordReader(split, context);
        reader.initialize(split, context);
        boolean foundKv = reader.nextKeyValue();
        int i = 0;
        while(foundKv) {
            foundKv = reader.nextKeyValue();
            i++;
        }
        assertEquals(i, SAMPLE_PHD_TAG_PAIR_COUNT + SAMPLE_ARTICLE_TAG_PAIR_COUNT);
    }

    @Test
    public void testMultiTagXmlInputFormat3() throws IOException, InterruptedException {
        conf.setStrings(MultiTagXmlInputFormat.TAGS_KEY, "mastersthesis");
        MultiTagXmlInputFormat inputFormat = ReflectionUtils.newInstance(MultiTagXmlInputFormat.class, conf);
        TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
        RecordReader<LongWritable, Text> reader = inputFormat.createRecordReader(split, context);
        reader.initialize(split, context);
        boolean foundKv = reader.nextKeyValue();
        int i = 0;
        while(foundKv) {
            foundKv = reader.nextKeyValue();
            i++;
        }
        assertEquals(i, SAMPLE_MSC_ARTICLE_TAG_PAIR_COUNT);

    }

    @Test
    public void testMultiTagXmlInputFormat4() throws IOException, InterruptedException {
        conf.setStrings(MultiTagXmlInputFormat.TAGS_KEY, "asdf","foo","mastersthesis");
        MultiTagXmlInputFormat inputFormat = ReflectionUtils.newInstance(MultiTagXmlInputFormat.class, conf);
        TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
        RecordReader<LongWritable, Text> reader = inputFormat.createRecordReader(split, context);
        reader.initialize(split, context);
        boolean foundKv = reader.nextKeyValue();
        int i = 0;
        while(foundKv) {
            foundKv = reader.nextKeyValue();
            i++;
        }
        assertEquals(i, SAMPLE_MSC_ARTICLE_TAG_PAIR_COUNT);

    }

    @Test
    public void testMultiTagXmlInputFormat5() throws IOException, InterruptedException {
        conf.setStrings(MultiTagXmlInputFormat.TAGS_KEY, "asdf");
        MultiTagXmlInputFormat inputFormat = ReflectionUtils.newInstance(MultiTagXmlInputFormat.class, conf);
        TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
        RecordReader<LongWritable, Text> reader = inputFormat.createRecordReader(split, context);
        reader.initialize(split, context);
        boolean foundKv = reader.nextKeyValue();
        int i = 0;
        while(foundKv) {
            foundKv = reader.nextKeyValue();
            i++;
        }
        assertEquals(i, 0);

    }

    @Test
    public void testMultiTagXmlInputFormat6() throws IOException, InterruptedException {
        conf.setStrings(MultiTagXmlInputFormat.TAGS_KEY, "article","phdthesis");
        MultiTagXmlInputFormat inputFormat = ReflectionUtils.newInstance(MultiTagXmlInputFormat.class, conf);
        TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
        RecordReader<LongWritable, Text> reader1 = inputFormat.createRecordReader(split1, context);
        reader1.initialize(split1, context);
        int i = 0;
        boolean foundKv = reader1.nextKeyValue();
        while(foundKv) {
            foundKv = reader1.nextKeyValue();
            i++;
        }
        RecordReader<LongWritable, Text> reader2 = inputFormat.createRecordReader(split2, context);
        reader2.initialize(split2, context);
        foundKv = reader2.nextKeyValue();
        while(foundKv) {
            foundKv = reader2.nextKeyValue();
            i++;
        }
        assertEquals(i, SAMPLE_PHD_TAG_PAIR_COUNT + SAMPLE_ARTICLE_TAG_PAIR_COUNT);
    }
}
