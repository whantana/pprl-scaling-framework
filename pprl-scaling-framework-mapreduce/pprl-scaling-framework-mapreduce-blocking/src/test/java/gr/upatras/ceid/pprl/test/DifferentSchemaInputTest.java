package gr.upatras.ceid.pprl.test;

import avro.shaded.com.google.common.collect.Lists;
import gr.upatras.ceid.pprl.datasets.DatasetException;
import gr.upatras.ceid.pprl.datasets.DatasetsUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.junit.Test;

import java.io.IOException;


public class DifferentSchemaInputTest {

    @Test
    public void test0() throws IOException, DatasetException, InterruptedException {
        Configuration conf = new Configuration(false);
        conf.set("fs.default.name", "file:///");

        final Schema aliceSchema = DatasetsUtil.loadSchemaFromFSPath(FileSystem.get(conf),
                new Path("data/clk_voters_a/schema/clk_voters_a.avsc"));
        final Schema bobSchema = DatasetsUtil.loadSchemaFromFSPath(FileSystem.get(conf),
                new Path("data/clk_voters_b/schema/clk_voters_b.avsc"));

        final Path inputA = new Path("data/clk_voters_a/avro/clk_voters_a.avro");
        final long lenA = FileSystem.get(conf).getFileStatus(inputA).getLen();
        final FileSplit fA = new FileSplit(inputA,0,lenA,null);
        final Path inputB = new Path("data/clk_voters_b/avro/clk_voters_b.avro");
        final long lenB = FileSystem.get(conf).getFileStatus(inputB).getLen();
        final FileSplit fB = new FileSplit(inputB,0,lenA,null);

        final Schema union = Schema.createUnion(aliceSchema,bobSchema);

        final Job job = Job.getInstance(conf);
        AvroJob.setInputKeySchema(job,union);
        AvroKeyInputFormat<GenericRecord> avroKeyInputFormat = new AvroKeyInputFormat();
        TaskAttemptContext context = new TaskAttemptContextImpl(job.getConfiguration(), new TaskAttemptID());

        RecordReader<AvroKey<GenericRecord>, NullWritable> readerA =
                avroKeyInputFormat.createRecordReader(fA, context);
        readerA.initialize(fA,context);

        RecordReader<AvroKey<GenericRecord>, NullWritable> readerB =
                        avroKeyInputFormat.createRecordReader(fB, context);
        readerB.initialize(fB,context);


        for (int i = 0; i < 5; i++) {
            readerA.nextKeyValue();
            readerB.nextKeyValue();
            System.out.println("readerA.getCurrentKey() : " + readerA.getCurrentKey().datum().get("id"));
            System.out.println("readerB.getCurrentKey() : " + readerB.getCurrentKey().datum().get("id"));
        }

    }
}
