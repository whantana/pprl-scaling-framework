package gr.upatras.ceid.pprl.mapreduce.input;


import com.google.common.io.Closeables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;

/**
 * Multi Tag XML Input Format class.
 */
public class MultiTagXmlInputFormat extends TextInputFormat {
    public static final String TAGS_KEY = "xmlinput.tags";

    /**
     * Creates the record reader.
     *
     * @param split input split.
     * @param context context.
     * @return
     */
    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
        try {
            return new MultiTagXmlRecordReader((FileSplit) split, context.getConfiguration());
        } catch (IOException ioe) {
            return null;
        }
    }

    /**
     * Multi tag XML record reader class.
     */
    public static class MultiTagXmlRecordReader extends RecordReader<LongWritable, Text> {

        private final byte[][] startTags;
        private final byte[][] endTags;
        private final long start;
        private final long end;
        private boolean inOriginalSplit;
        private final FSDataInputStream fsin;
        private final DataOutputBuffer buffer = new DataOutputBuffer();
        private LongWritable currentKey;
        private Text currentValue;
        private byte[][] currentTag;
        private int[] matchCounters;
        private int tagCount;

        /**
         * Constructor.
         *
         * @param split input split.
         * @param conf configuration.
         * @throws IOException
         */
        public MultiTagXmlRecordReader(FileSplit split, Configuration conf) throws IOException{

            // tags and tag count
            final String[] tagNames = conf.getStrings(TAGS_KEY);
            tagCount = tagNames.length;

            // start tags
            startTags = new byte[tagCount][];
            for (int i = 0; i < tagCount; i++)
                startTags[i] = ("<" + conf.getStrings(TAGS_KEY)[i]).getBytes();


            // end tags
            endTags = new byte[tagCount][];
            for (int i = 0; i < tagCount; i++)
                endTags[i] = ("</" + conf.getStrings(TAGS_KEY)[i] + ">").getBytes();

            // current tag (0:start tag,1:end tag)
            // i.e currentTag[0] = "<tag>", currentTag[1] = "</tag>"
            currentTag = new byte[2][];
            currentTag[0] = null;
            currentTag[1] = null;

            // match counters
            matchCounters = new int[tagCount];
            resetMatchCounters();

            // start and end of split
            start = split.getStart();
            end = start + split.getLength();

            // path and filesystem
            Path file = split.getPath();
            FileSystem fs = file.getFileSystem(conf);

            // open input stream and seek start
            inOriginalSplit = true;
            fsin = fs.open(split.getPath());
            fsin.seek(start);

            // If this is not the first split, we always throw away first record
            // because we always (except the last split) read one extra line in
            // next() method.
            if (start != 0) {
                // read until first record is skipped
                readUntilAnyEndTagIsFound();
            }
        }

        /**
         * Next (key,value).
         *
         * @param key a key.
         * @param value a value.
         * @return
         * @throws IOException
         */
        private boolean next(LongWritable key, Text value) throws IOException {
            if(!inOriginalSplit) return false;
            try {
                readUntilAnyStartTagIsFound();
                if(currentTag[0] == null) return false;

                // write start tag
                buffer.write(currentTag[0]);

                boolean foundEndTag = readUntilCurrentEndTagIsFound();
                if(foundEndTag) {
                    key.set(fsin.getPos());
                    value.set(new String(buffer.getData(),0,buffer.getLength()));
                    return true;
                }
            } finally {
                buffer.reset();
            }
            return false;
        }

        /**
         * Read file until a start tag is found.
         *
         * @throws IOException
         */
        private void readUntilAnyStartTagIsFound() throws IOException {
            while (true) {
                int b = fsin.read();

                // if end of split reached mark that we are outside original split
                if(fsin.getPos() >= end) inOriginalSplit = false;
                if(b == -1) return;

                for (int i = 0; i < tagCount; i++) {
                    // continue matching for a tag
                    if(b == startTags[i][matchCounters[i]]) matchCounters[i]++;
                    else resetMatchCounter(i);

                    //  if counter matchs the tag length, assign tag as current and reset counters
                    if(matchCounters[i] >= startTags[i].length) {
                        currentTag[0] = startTags[i];
                        currentTag[1] = endTags[i];
                        resetMatchCounters();
                        return;
                    }
                }
            }
        }

        /**
         * Read until any end tag is found.
         *
         * @throws IOException
         */
        private void readUntilAnyEndTagIsFound() throws IOException {
            while(true) {
                int b = fsin.read();
                if(fsin.getPos() >= end || b == -1) {
                    inOriginalSplit = false ;
                    return;
                }

                for (int i = 0; i < tagCount; i++) {
                    // continue matching for a tag
                    if(b == endTags[i][matchCounters[i]]) matchCounters[i]++;
                    else resetMatchCounter(i);

                    //  if counter matchs the tag length, assign tag as current and reset counters
                    if(matchCounters[i] >= endTags[i].length) {
                        resetMatchCounters();
                        return;
                    }
                }
            }
        }

        /**
         * Read until current end tag is found.
         *
         * @return boolean true if end tag is found, false if split reaches end or file reaches end.
         * @throws IOException
         */
        private boolean readUntilCurrentEndTagIsFound() throws IOException {
            int i = 0;
            while(true) {

                int b = fsin.read();

                // if end of split reached mark that we are outside original split
                if(fsin.getPos() >= end) inOriginalSplit = false;
                if(b == -1) return false;

                // save to buffer
                buffer.write(b);

                if(b == currentTag[1][i]) {
                    i++;
                    //  if counter matchs the tag length, set current tag to null and return true
                    if(i >= currentTag[1].length) {
                        currentTag[0] = null;
                        currentTag[1] = null;
                        resetMatchCounters();
                        return true;
                    }
                } else i = 0;
            }
        }

        /**
         * Reset match counters ((start,end)-tag matching).
         */
        private void resetMatchCounters() {
            for (int i = 0; i < matchCounters.length; i++) {
                resetMatchCounter(i);
            }
        }

        /**
         * Reset the i-th match counter.
         *
         * @param i index of counter.
         */
        private void resetMatchCounter(int i) {
            matchCounters[i] = 0;
        }


        @Override
        public void close() throws IOException {
            Closeables.close(fsin, true);
        }

        @Override
        public float getProgress() throws IOException {
            return (fsin.getPos() - start) / (float) (end - start);
        }


        @Override
        public LongWritable getCurrentKey() throws IOException, InterruptedException {
            return currentKey;
        }

        @Override
        public Text getCurrentValue() throws IOException, InterruptedException {
            return currentValue;
        }

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {}

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            currentKey = new LongWritable();
            currentValue = new Text();
            return next(currentKey, currentValue);
        }
    }
}
