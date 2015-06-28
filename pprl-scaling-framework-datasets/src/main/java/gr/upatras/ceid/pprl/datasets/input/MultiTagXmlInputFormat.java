package gr.upatras.ceid.pprl.datasets.input;


import com.google.common.base.Charsets;
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

public class MultiTagXmlInputFormat extends TextInputFormat {

    public static final String START_TAGS_KEY = "xmlinput.start.tags";
    public static final String END_TAGS_KEY = "xmlinput.end.tags";

    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
        try {
            return new MultiTagXmlRecordReader((FileSplit) split, context.getConfiguration());
        } catch (IOException ioe) {
            //log.warn("Error while creating XmlRecordReader", ioe);
            return null;
        }
    }

    public static class MultiTagXmlRecordReader extends RecordReader<LongWritable, Text> {

        private final byte[][] startTags;
        private final byte[][] endTags;
        private final long start;
        private final long end;
        private final FSDataInputStream fsin;
        private final DataOutputBuffer buffer = new DataOutputBuffer();
        private LongWritable currentKey;
        private Text currentValue;
        private byte[][] currentTag;
        private int[] matchCounters;
        private int tagCount;


        public MultiTagXmlRecordReader(FileSplit split, Configuration conf) throws IOException{
            // check tags for validity
            checkXmlTagsConfiguration(conf);

            // tag count
            tagCount = conf.getStrings(START_TAGS_KEY).length;

            // start tags
            startTags = new byte[tagCount][];
            for (int i = 0; i < tagCount; i++)
                startTags[i] = conf.getStrings(START_TAGS_KEY)[i].getBytes(Charsets.UTF_8);

            // end tags
            endTags = new byte[tagCount][];
            for (int i = 0; i < tagCount; i++)
                endTags[i] = conf.getStrings(END_TAGS_KEY)[i].getBytes(Charsets.UTF_8);

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
            fsin = fs.open(split.getPath());
            fsin.seek(start);
        }

        public static void checkXmlTagsConfiguration(Configuration conf) throws IOException {
            final String[] startTags = conf.getStrings(START_TAGS_KEY);
            final String[] endTags = conf.getStrings(END_TAGS_KEY);
            if(startTags.length != endTags.length )
                throw new IOException("Number of tags provided must agree (" + startTags.length + "!=" + endTags.length + ").");
            for (int i = 0; i < startTags.length ; i++) {
                String st = startTags[i].replaceAll("\\p{P}|<|>", "");
                String et = endTags[i].replaceAll("\\p{P}|<|>", "");
                if(!st.equals(et))
                    throw new IOException("Starting and ending tag do not agree (" + st + " != " + et + ").");
            }
        }

        private boolean next(LongWritable key, Text value) throws IOException {
            boolean foundStartTag = readUntilStartTagIsFound();

            // if end of file reached or no start tag found return false
            if(!foundStartTag) return false;
            try {
                // write start tag
                buffer.write(currentTag[0]);

                boolean foundEndTag = readUntilEndTagIsFound();
                if(foundEndTag) {
                    key.set(fsin.getPos());
                    value.set(buffer.getData(),0,buffer.getLength());
                    return true;
                }
            } finally {
                buffer.reset();
            }
            return false;
        }


        private boolean readUntilStartTagIsFound() throws IOException {
            while (true) {
                int b = fsin.read();

                // if end of split reached return false
                if(b == -1 || fsin.getPos() >= end) return false;
                for (int i = 0; i < tagCount; i++) {

                    // continue matching for a tag
                    if(b == startTags[i][matchCounters[i]]) matchCounters[i]++;
                    else resetMatchCounter(i);

                    //  if counter matchs the tag length, assign tag as current and reset counters
                    if(matchCounters[i] >= startTags[i].length) {
                        currentTag[0] = startTags[i];
                        currentTag[1] = endTags[i];
                        resetMatchCounters();
                        return true;
                    }
                }
            }
        }

        private boolean readUntilEndTagIsFound() throws IOException {
            int i = 0;
            while(true) {
                int b = fsin.read();

                // if end of split reached return false
                if(b == -1 || fsin.getPos() >= end) return false;

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
                }
                else i = 0;
            }
        }

        private void resetMatchCounters() {
            for (int i = 0; i < matchCounters.length; i++) {
                resetMatchCounter(i);
            }
        }

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
