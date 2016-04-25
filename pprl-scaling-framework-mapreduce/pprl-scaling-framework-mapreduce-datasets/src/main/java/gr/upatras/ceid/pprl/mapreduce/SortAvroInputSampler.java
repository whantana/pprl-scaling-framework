package gr.upatras.ceid.pprl.mapreduce;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.hadoop.io.AvroKeyComparator;
import org.apache.avro.hadoop.io.AvroSequenceFile;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

/**
 * Input sampler class for sorting avro datasets.
 */
public class SortAvroInputSampler extends InputSampler<AvroKey<GenericRecord>,NullWritable> {


    public SortAvroInputSampler(Configuration conf) {
        super(conf);
    }

    public static void writePartitionFile(Job job, RandomSampler sampler) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = job.getConfiguration();
        int numPartitions = job.getNumReduceTasks();

        AvroKeyInputFormat akinf = new AvroKeyInputFormat<GenericRecord>();
        final AvroKey<GenericRecord>[] samples = sampler.getSample(akinf, job);

        final String sortedSchemaStr = job.getConfiguration().get(SortMapper.SORTED_SCHEMA_KEY, null);
        final Schema sortedSchema = (new Schema.Parser().parse(sortedSchemaStr));

        AvroKeyComparator<GenericRecord> comparator = new AvroKeyComparator<GenericRecord>();
        comparator.setConf(conf);

        Set<AvroKey<GenericRecord>> sortedAndUpdatedSamples  = new
                TreeSet<AvroKey<GenericRecord>>(comparator);
        for (AvroKey<GenericRecord> sample : samples) {
            final GenericRecord record = new GenericData.Record(sortedSchema);
            for (Schema.Field field : sortedSchema.getFields())
                record.put(field.name(), sample.datum().get(field.name()));
            sortedAndUpdatedSamples.add(new AvroKey<GenericRecord>(record));
        }
        final AvroKey<GenericRecord>[] sortedSamples =
                sortedAndUpdatedSamples.toArray(new AvroKey[sortedAndUpdatedSamples.size()]);


        final Path dst = new Path(SortAvroTotalOrderPartitioner.getPartitionFile(conf));  // Avro total parititoner
        final FileSystem fs = dst.getFileSystem(conf);
        if(fs.exists(dst)) {
            fs.delete(dst, false);
        }
        SequenceFile.Writer writer = AvroSequenceFile.createWriter(
                (new AvroSequenceFile.Writer.Options())
                        .withFileSystem(fs)
                        .withOutputPath(dst)
                        .withConfiguration(conf)
                        .withKeySchema(sortedSchema)
                        .withValueClass(NullWritable.class));

        NullWritable nullValue = NullWritable.get();
        float stepSize = (float)sortedSamples.length / (float)numPartitions;
        int last = -1;

        for(int i = 1; i < numPartitions; ++i) {
            int k;
            for(k = Math.round(stepSize * (float)i);
                last >= k && comparator.compare(sortedSamples[last], sortedSamples[k]) == 0;
                ++k) {;}
            writer.append(sortedSamples[k], nullValue);
            last = k;
        }

        writer.close();
    }

    public static class RandomSampler implements InputSampler.Sampler<AvroKey<GenericRecord>, NullWritable> {

        protected double freq;
        protected final int numSamples;
        protected final int maxSplitsSampled;


        public RandomSampler(double freq, int numSamples) {
            this(freq, numSamples, 2147483647);
        }

        public RandomSampler(double freq, int numSamples, int maxSplitsSampled) {
            this.freq = freq;
            this.numSamples = numSamples;
            this.maxSplitsSampled = maxSplitsSampled;
        }

        /**
         * Get a random sample from the input.
         *
         * @param inf <code>InputFormat</code> instance.
         * @param job <code>Job</code> instance.
         * @return a random sample from the input.
         * @throws IOException
         * @throws InterruptedException
         */
        public AvroKey<GenericRecord>[] getSample(InputFormat<AvroKey<GenericRecord>,NullWritable> inf, Job job)
                throws IOException, InterruptedException {

            List<InputSplit> splits = inf.getSplits(job);

            List<AvroKey<GenericRecord>> slist = new ArrayList<AvroKey<GenericRecord>>(this.numSamples);
            int splitsToSample = Math.min(this.maxSplitsSampled, splits.size());
            Random r = new Random();
            long seed = r.nextLong();
            r.setSeed(seed);

            for (int i = 0; i < splits.size(); ++i) {
                InputSplit samplingContext = splits.get(i);
                int reader = r.nextInt(splits.size());
                splits.set(i, splits.get(reader));
                splits.set(reader, samplingContext);
            }

            for (int i = 0; i < splitsToSample || i < splits.size() && slist.size() < this.numSamples; ++i) {
                TaskAttemptContextImpl ctx = new TaskAttemptContextImpl(job.getConfiguration(), new TaskAttemptID());
                RecordReader<AvroKey<GenericRecord>, NullWritable> reader = inf.createRecordReader(splits.get(i), ctx);
                reader.initialize(splits.get(i), ctx);

                while (reader.nextKeyValue()) {
                    if (r.nextDouble() <= this.freq) {
                        if (slist.size() < this.numSamples) {
                            slist.add(new AvroKey<GenericRecord>(copyRecord(reader.getCurrentKey().datum())));
                        } else {
                            int ind = r.nextInt(this.numSamples);
                            if (ind != this.numSamples) {
                                slist.add(new AvroKey<GenericRecord>(copyRecord(reader.getCurrentKey().datum())));
                            }
                            this.freq *= (double) (this.numSamples - 1) / (double) this.numSamples;
                        }
                    }
                }
                reader.close();
            }

            return slist.toArray(new AvroKey[slist.size()]);
        }


        private GenericRecord copyRecord(final GenericRecord record) {
            final Schema schema = record.getSchema();
            final GenericRecord copyRecord = new GenericData.Record(schema);
            for(Schema.Field field : schema.getFields()) {
                copyRecord.put(field.name(),record.get(field.name()));
            }
            return copyRecord;
        }
    }
}
