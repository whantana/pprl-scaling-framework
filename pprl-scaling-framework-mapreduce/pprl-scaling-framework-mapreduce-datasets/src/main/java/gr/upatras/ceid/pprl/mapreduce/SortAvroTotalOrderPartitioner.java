package gr.upatras.ceid.pprl.mapreduce;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.hadoop.io.AvroKeyComparator;
import org.apache.avro.hadoop.io.AvroSequenceFile;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Sort Avro Total Order Partitioner class.
 */
public class SortAvroTotalOrderPartitioner extends Partitioner<AvroKey<GenericRecord>, NullWritable> implements Configurable {

    public static final String DEFAULT_PATH = "_partition.lst";
    public static final String PARTITIONER_PATH = "sortavro.totalorderpartitioner.path";

    private Configuration conf;
    private Node<AvroKey<GenericRecord>> partitions;

    public Configuration getConf() {
        return conf;
    }

    public void setConf(Configuration configuration) {
        try {
            this.conf = configuration;
            final Path partFile = new Path(getPartitionFile(configuration));
            final FileSystem fs = FileSystem.get(configuration);
            AvroKeyComparator<GenericRecord> comparator = new AvroKeyComparator<GenericRecord>();
            comparator.setConf(configuration);
            AvroKey<GenericRecord>[] splitPoints = readPartitions(fs,partFile,configuration,comparator);
            Job job = new Job(conf);
            int numReduceTasks = job.getNumReduceTasks();
            if (splitPoints.length != numReduceTasks - 1)
                throw new IOException("Wrong number of partitions in keyset. Partitions count : " +
                        splitPoints.length +".");
            partitions = new BinarySearchNode(splitPoints, comparator);
        } catch (IOException e) {
            throw new IllegalArgumentException("Can't read partitions file", e);
        }
    }

    @Override
    public int getPartition(AvroKey<GenericRecord> avroKey, NullWritable nullWritable, int i) {
        return partitions.findPartition(avroKey);
    }

    public static void setPartitionFile(Configuration conf, Path p) {
        conf.set(PARTITIONER_PATH, p.toString());
    }

    public static String getPartitionFile(Configuration conf) {
        return conf.get(PARTITIONER_PATH, DEFAULT_PATH);
    }

    @SuppressWarnings("unchecked")
    private AvroKey<GenericRecord>[] readPartitions(FileSystem fs, Path p,
                                                    Configuration conf, AvroKeyComparator<GenericRecord> comparator)
            throws IOException {

        List<AvroKey<GenericRecord>> parts = new ArrayList<AvroKey<GenericRecord>>();
        String schemaStr = conf.get(SortMapper.SORTED_SCHEMA_KEY,null);
        if(schemaStr == null) throw new IllegalStateException("Must set sorted schema.");
        final Schema schema = (new Schema.Parser()).parse(schemaStr);
        AvroSequenceFile.Reader reader = new AvroSequenceFile.Reader(
                new AvroSequenceFile.Reader.Options()
                        .withFileSystem(fs)
                        .withConfiguration(conf)
                        .withInputPath(p)
                        .withKeySchema(schema)
        );
        AvroKey<GenericRecord> key = null;
        do {
            key = new AvroKey<GenericRecord>();
            key = (AvroKey<GenericRecord>) reader.next(key);
            if(key != null) parts.add(key);
        }while(key != null);

        Collections.sort(parts, comparator);
        return parts.toArray(new AvroKey[parts.size()]);
    }

    /**
     * Interface to the partitioner to locate a key in the partition keyset.
     */
    public interface Node<T> {
        /**
         * Locate partition in keyset K, st [Ki..Ki+1) defines a partition,
         * with implicit K0 = -inf, Kn = +inf, and |K| = #partitions - 1.
         */
        int findPartition(T key);
    }

    public static class BinarySearchNode implements Node<AvroKey<GenericRecord>> {
        private final AvroKey<GenericRecord>[] splitPoints;
        private final AvroKeyComparator<GenericRecord> comparator;
        public BinarySearchNode(AvroKey<GenericRecord>[] splitPoints, AvroKeyComparator<GenericRecord> comparator) {
            this.splitPoints = splitPoints;
            this.comparator = comparator;
        }
        public int findPartition(AvroKey<GenericRecord> key) {
            final int pos = Arrays.binarySearch(splitPoints, key, comparator) + 1;
            return (pos < 0) ? -pos : pos;
        }
    }
}
