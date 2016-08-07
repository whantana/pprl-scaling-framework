package gr.upatras.ceid.pprl.test;


import gr.upatras.ceid.pprl.mapreduce.TextArrayWritable;
import gr.upatras.ceid.pprl.mapreduce.input.DblpXmlInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;

import static org.junit.Assert.assertTrue;

public class DblpXmlInputFormatTest {

    private Configuration conf;
    private long len ;
    private Path xmlPath;
    private DblpXmlInputFormat inputFormat;
    private TaskAttemptContext context;


    @Before
    public void setUp() throws URISyntaxException, IOException {
        conf = new Configuration();
        final LocalFileSystem fs = FileSystem.getLocal(conf);
        final Path basePath = fs.getWorkingDirectory();
        xmlPath = new Path(basePath,"data/dblp/xml/dblp.small.xml");
        conf.setStrings(DblpXmlInputFormat.TAGS_KEY,
                "article","inproceedings","proceedings",
                "book","incollection","www",
                "phdthesis","mastersthesis");
        conf.setStrings(DblpXmlInputFormat.SECONDARY_TAGS_KEY, "author","title","year");
        assertTrue(fs.exists(xmlPath));
        len = fs.getFileStatus(xmlPath).getLen();
        System.out.println("Path " + xmlPath + " length : " + len + " bytes");
        inputFormat = ReflectionUtils.newInstance(DblpXmlInputFormat.class, conf);
        context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
    }


    @Test
    public void test1() throws IOException, InterruptedException {
        final FileSplit whole = new FileSplit(xmlPath,0,len,null);

        RecordReader<Text, TextArrayWritable> reader =
                inputFormat.createRecordReader(whole, context);
        reader.initialize(whole, context);

        while(reader.nextKeyValue()) {
            System.out.println("whole reader key : " + reader.getCurrentKey());
            System.out.println("whole reader value : " + reader.getCurrentValue());
        }
    }

    @Test
    public void test2() throws IOException, InterruptedException {
        final long splitPos = len/2;
        final FileSplit split1 = new FileSplit(xmlPath,0,len-splitPos,null);
        final FileSplit split2 = new FileSplit(xmlPath,len-splitPos,len,null);

        RecordReader<Text, TextArrayWritable> reader1 =
                inputFormat.createRecordReader(split1, context);
        reader1.initialize(split1, context);

        while(reader1.nextKeyValue()) {
        }


        RecordReader<Text, TextArrayWritable> reader2 =
                inputFormat.createRecordReader(split2, context);
        reader1.initialize(split2, context);

        while(reader2.nextKeyValue()) {
        }
    }
}
