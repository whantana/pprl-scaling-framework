package gr.upatras.ceid.pprl.test;


import gr.upatras.ceid.pprl.mapreduce.input.MultiTagXmlInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MultiTagXmlInputFormatTest {

    private static final Logger LOG = LoggerFactory.getLogger(QGramCountingMRTest.class);

    private Configuration conf;
    private FileSplit split;
    private FileSplit split1;
    private FileSplit split2;

    private final int SAMPLE_ARTICLE_TAG_PAIR_COUNT = 9;
    private final int SAMPLE_PHD_TAG_PAIR_COUNT = 8;
    private final int SAMPLE_MSC_ARTICLE_TAG_PAIR_COUNT = 1;

    @Before
    public void setUp() throws URISyntaxException, IOException {
        conf = new Configuration();
        final LocalFileSystem fs = FileSystem.getLocal(conf);
        final Path basePath = fs.getWorkingDirectory();
        final Path xmlPath = new Path(basePath,"data/dblp/xml/dblp.xml");
        assertTrue(fs.exists(xmlPath));
        final long len = fs.getFileStatus(xmlPath).getLen();
        split = new FileSplit(xmlPath, 0, len, null);
        long splitPos = len / 2;
        split1 =  new FileSplit(xmlPath, 0, splitPos, null);
        split2 =  new FileSplit(xmlPath, splitPos , len - splitPos, null);
    }

    @Test
    public void test1() throws IOException, InterruptedException {
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
    public void test2() throws IOException, InterruptedException {
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
    public void test3() throws IOException, InterruptedException {
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
    public void test4() throws IOException, InterruptedException {
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
    public void test5() throws IOException, InterruptedException {
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
    public void test6() throws IOException, InterruptedException {
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
