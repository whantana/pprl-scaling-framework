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
import org.apache.mahout.text.wikipedia.XmlInputFormat;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class MultiTagXmlInputFormatTest {

    private Configuration conf;
    private FileSplit split;

    private final int SAMPLE_ARTICLE_TAG_PAIR_COUNT = 9;
    private final int SAMPLE_PHD_TAG_PAIR_COUNT = 8;
    private final int SAMPLE_MSC_ARTICLE_TAG_PAIR_COUNT = 1;
    private final int SAMPLE_TOTAL_TAG_PAIR_COUNT =  SAMPLE_ARTICLE_TAG_PAIR_COUNT +
            SAMPLE_PHD_TAG_PAIR_COUNT +
            SAMPLE_MSC_ARTICLE_TAG_PAIR_COUNT;

    @Before
    public void setUp() {
        URL resourceUrl = getClass().getResource("/sample.xml");
        assertNotNull("Test file missing", resourceUrl);
        File testFile = null;
        try {
            testFile = new File(resourceUrl.toURI());
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        conf = new Configuration(false);

        assert testFile != null;
        split = new FileSplit(new Path(testFile.getAbsoluteFile().toURI()), 0, testFile.length(), null);
    }

    @Test
    public void testXmlInputFormat1() {
        conf.set(XmlInputFormat.START_TAG_KEY,"<phdthesis");
        conf.set(XmlInputFormat.END_TAG_KEY,"</phdthesis>");
        XmlInputFormat inputFormat = ReflectionUtils.newInstance(XmlInputFormat.class, conf);
        TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
        RecordReader<LongWritable, Text> reader = inputFormat.createRecordReader(split, context);
        try {
            reader.initialize(split, context);
            boolean foundKv = reader.nextKeyValue();
            int i = 0;
            while(foundKv) {
                foundKv = reader.nextKeyValue();
                i++;
            }
            assertEquals(i,SAMPLE_PHD_TAG_PAIR_COUNT);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testXmlInputFormat2() {
        conf.set(XmlInputFormat.START_TAG_KEY,"<foobar");
        conf.set(XmlInputFormat.END_TAG_KEY,"</foobar>");
        XmlInputFormat inputFormat = ReflectionUtils.newInstance(XmlInputFormat.class, conf);
        TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
        RecordReader<LongWritable, Text> reader = inputFormat.createRecordReader(split, context);
        try {
            reader.initialize(split, context);
            boolean foundKv = reader.nextKeyValue();
            int i = 0;
            while(foundKv) {
                foundKv = reader.nextKeyValue();
                i++;
            }
            assertEquals(i,0);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testMultiTagXmlInputFormat1() {
        conf.setStrings(MultiTagXmlInputFormat.START_TAGS_KEY,
                new String[]{"<article","<phdthesis","<mastersthesis"});
        conf.setStrings(MultiTagXmlInputFormat.END_TAGS_KEY,
                new String[]{"</article>","</phdthesis>","</mastersthesis>"});
        MultiTagXmlInputFormat inputFormat = ReflectionUtils.newInstance(MultiTagXmlInputFormat.class, conf);
        TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
        RecordReader<LongWritable, Text> reader = inputFormat.createRecordReader(split, context);
        try {
            reader.initialize(split, context);
            boolean foundKv = reader.nextKeyValue();
            int i = 0;
            while(foundKv) {
                foundKv = reader.nextKeyValue();
                i++;
            }
            assertEquals(i, SAMPLE_TOTAL_TAG_PAIR_COUNT);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testMultiTagXmlInputFormat2() {
        conf.setStrings(MultiTagXmlInputFormat.START_TAGS_KEY,
                new String[]{"<article","<phdthesis"});
        conf.setStrings(MultiTagXmlInputFormat.END_TAGS_KEY,
                new String[]{"</article>","</phdthesis>"});
        MultiTagXmlInputFormat inputFormat = ReflectionUtils.newInstance(MultiTagXmlInputFormat.class, conf);
        TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
        RecordReader<LongWritable, Text> reader = inputFormat.createRecordReader(split, context);
        try {
            reader.initialize(split, context);
            boolean foundKv = reader.nextKeyValue();
            int i = 0;
            while(foundKv) {
                foundKv = reader.nextKeyValue();
                i++;
            }
            assertEquals(i, SAMPLE_PHD_TAG_PAIR_COUNT + SAMPLE_ARTICLE_TAG_PAIR_COUNT);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testMultiTagXmlInputFormat3() {
        conf.setStrings(MultiTagXmlInputFormat.START_TAGS_KEY,
                new String[]{"<mastersthesis",});
        conf.setStrings(MultiTagXmlInputFormat.END_TAGS_KEY,
                new String[]{"</mastersthesis>"});
        MultiTagXmlInputFormat inputFormat = ReflectionUtils.newInstance(MultiTagXmlInputFormat.class, conf);
        TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
        RecordReader<LongWritable, Text> reader = inputFormat.createRecordReader(split, context);
        try {
            reader.initialize(split, context);
            boolean foundKv = reader.nextKeyValue();
            int i = 0;
            while(foundKv) {
                foundKv = reader.nextKeyValue();
                i++;
            }
            assertEquals(i, SAMPLE_MSC_ARTICLE_TAG_PAIR_COUNT);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testMultiTagXmlInputFormat4() {
        conf.setStrings(MultiTagXmlInputFormat.START_TAGS_KEY,
                new String[]{"<asdf","<foo","<mastersthesis"});
        conf.setStrings(MultiTagXmlInputFormat.END_TAGS_KEY,
                new String[]{"</asdf>","</foo>","</mastersthesis>"});
        MultiTagXmlInputFormat inputFormat = ReflectionUtils.newInstance(MultiTagXmlInputFormat.class, conf);
        TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
        RecordReader<LongWritable, Text> reader = inputFormat.createRecordReader(split, context);
        try {
            reader.initialize(split, context);
            boolean foundKv = reader.nextKeyValue();
            int i = 0;
            while(foundKv) {
                foundKv = reader.nextKeyValue();
                i++;
            }
            assertEquals(i, SAMPLE_MSC_ARTICLE_TAG_PAIR_COUNT);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testMultiTagXmlInputFormat5() {
        conf.setStrings(MultiTagXmlInputFormat.START_TAGS_KEY,
                new String[]{"<asdf"});
        conf.setStrings(MultiTagXmlInputFormat.END_TAGS_KEY,
                new String[]{"</asdf>"});
        MultiTagXmlInputFormat inputFormat = ReflectionUtils.newInstance(MultiTagXmlInputFormat.class, conf);
        TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
        RecordReader<LongWritable, Text> reader = inputFormat.createRecordReader(split, context);
        try {
            reader.initialize(split, context);
            boolean foundKv = reader.nextKeyValue();
            int i = 0;
            while(foundKv) {
                foundKv = reader.nextKeyValue();
                i++;
            }
            assertEquals(i, 0);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testCheckConfigArgs1() {
        conf.setStrings(MultiTagXmlInputFormat.START_TAGS_KEY,
                "<phdthesis","<mastersthesis","<asdf");
        conf.setStrings(MultiTagXmlInputFormat.END_TAGS_KEY,
                        "</phdthesis>","</mastersthesis>","</asdf>");
        try {
            MultiTagXmlInputFormat.MultiTagXmlRecordReader.checkXmlTagsConfiguration(conf);
            assertTrue(true);
        } catch (IOException e) {
            assertTrue(e.getMessage(),false);
        }
    }

    @Test(expected = IOException.class)
    public void testCheckConfigArgs2() throws IOException{
        conf.setStrings(MultiTagXmlInputFormat.START_TAGS_KEY,
                "<phdthesis","<mastersthesis","<asdf");
        conf.setStrings(MultiTagXmlInputFormat.END_TAGS_KEY,
                        "</phdthesis>","</asdf>");
        MultiTagXmlInputFormat.MultiTagXmlRecordReader.checkXmlTagsConfiguration(conf);
    }

    @Test(expected = IOException.class)
    public void testCheckConfigArgs3() throws IOException{
        conf.setStrings(MultiTagXmlInputFormat.START_TAGS_KEY,
                "<phdthesis","<mastersthesis","<asdf");
        conf.setStrings(MultiTagXmlInputFormat.END_TAGS_KEY,
                        "</phdthesis>","</masTersthesis>","</Asdf>");
        MultiTagXmlInputFormat.MultiTagXmlRecordReader.checkXmlTagsConfiguration(conf);
    }
}
