package gr.upatras.ceid.pprl.mapreduce.input;

import com.google.common.io.Closeables;
import gr.upatras.ceid.pprl.mapreduce.TextArrayWritable;
import org.apache.commons.io.Charsets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;


/**
 * An input format calss for the DBLP xml, filtering fields and entities.
 */
public class DblpXmlInputFormat extends FileInputFormat<Text,TextArrayWritable> {
    public static final String TAGS_KEY = "dblp.xml.primary.tags";
    public static final String SECONDARY_TAGS_KEY = "dblp.xml.secondary.tags";
    public static final String MISSING_VALUE= "-missing-";

    @Override
    public RecordReader<Text, TextArrayWritable> createRecordReader(InputSplit split, TaskAttemptContext context) {
        try {
            return new MultiTagXmlRecordReader((FileSplit) split, context.getConfiguration());
        } catch (IOException ioe) {
            return null;
        }
    }

    /**
     * Multi tag XML record reader class.
     */
    public static class MultiTagXmlRecordReader extends RecordReader<Text, TextArrayWritable> {
        private final byte TAG_CLOSING = (byte) '>';
        private final byte TAG_OPENING = '<';
        private final byte END_TAG_OPENING_FOLLOWUP = '/';


        private final List<String> primaryTags;
        private final List<String> secondaryTags;

        private long currentPos;

        private final long start;
        private final long end;
        private boolean inForeignSplit;
        private boolean endOfFileReached;
        private final FSDataInputStream fsin;

        private Text currentKey;
        private TextArrayWritable currentValue;

        public MultiTagXmlRecordReader(FileSplit split, Configuration conf) throws IOException {
            // start and end of split
            start = split.getStart();
            end = start + split.getLength();

            primaryTags = Arrays.asList(conf.getStrings(TAGS_KEY));
            secondaryTags = Arrays.asList(conf.getStrings(SECONDARY_TAGS_KEY));

            // path and filesystem
            Path file = split.getPath();
            FileSystem fs = file.getFileSystem(conf);

            // open input stream and seek start
            inForeignSplit = false;
            endOfFileReached = false;
            fsin = fs.open(split.getPath());
            fsin.seek(start);

            // If this is not the first split, we always throw away first record
            // because we always (except the last split) read one extra line in
            // next() method.
            if (start != 0) {
                // read until first record is skipped
                skipUntilFirstPrimaryEndTag();
            }
        }

        @Override
        public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
                throws IOException, InterruptedException {}

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            currentKey = new Text();
            currentValue = new TextArrayWritable();
            return next(currentKey, currentValue);
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
        public Text getCurrentKey() throws IOException, InterruptedException {
            return currentKey;
        }

        @Override
        public TextArrayWritable getCurrentValue() throws IOException, InterruptedException {
            return currentValue;
        }


        /**
         * Read file until a start tag is found.
         *
         * @throws IOException
         */
        private void skipUntiAPrimaryStartTagIsFound() throws IOException {
            while (true) {
                int b0 = readByte();

                // if end of file has reached
                if(endOfFileReached) { return;}

                if (b0 == TAG_OPENING) {
                    long pos = fsin.getPos();
                    String tagValue = readCharsUntilStopChar((byte)' ');
                    if (primaryTags.contains(tagValue)) {
                        currentPos = pos;
                        break;
                    }
                }
            }
        }


        private boolean next(Text key, TextArrayWritable value) throws IOException {
            // avoid this call if reader already in foreign split
            if (inForeignSplit) return false;

            skipUntiAPrimaryStartTagIsFound();
            if (endOfFileReached) return false;

            // we reached a primary start tag. first read key
            final String kav = readKeyAttribute();
            if (endOfFileReached || kav == null) return false;

            // read values . Place "-missing-" for missing.
            final String[] tavs = readSecondaryTagValues();
            if (endOfFileReached || tavs == null) return false;

            // fill key
            key.set(kav);

            // fill values
            value.set(tavs);
            return true;
        }

        /**
         * Read until any end tag is found.
         *
         * @throws IOException
         */
        private void skipUntilFirstPrimaryEndTag() throws IOException {

            while(true) {
                int b0 = readByte();
                if(endOfFileReached || inForeignSplit) return;

                // if found the <
                if (b0 == TAG_OPENING) {
                    int b1 = readByte();
                    if (b1 < 0) return;
                    if (b1 == END_TAG_OPENING_FOLLOWUP) { // found the </
                        String tagValue = readCharsUntilTagClosing();
                        if (primaryTags.contains(tagValue)) return;
                    }
                }
            }
        }

        private int readByte() throws IOException {
            int b = fsin.read();
            if(fsin.getPos() >= end) inForeignSplit = true;
            if(b == -1) endOfFileReached = true;
            return b;
        }

        private String readCharsUntilStopChar(final byte stopChar) throws IOException {
            String v = "";
            for (int b2 = readByte();
                 b2 >= 0 && (b2 != stopChar && b2 != TAG_CLOSING);
                 b2 = readByte()) {
                v += (char) b2;
            }
            return v;
        }

        private String readCharsUntilTagClosing() throws IOException {
            String v = "";
            for (int b2 = readByte();
                 b2 >= 0 && b2 != TAG_CLOSING;
                 b2 = readByte()) {
                v += (char) b2;
            }
            return v;
        }


        private String readKeyAttribute() throws IOException {
            byte[] keyBytes = "key=\"".getBytes();
            int m = 0;
            while(true) {
                int b0 = readByte();
                if(endOfFileReached) return null;
                if(b0 == keyBytes[m]){
                    m++;
                    if(m == keyBytes.length) break;
                } else {
                    m = 0;
                    if(b0 == TAG_CLOSING) return null;
                }
            }
            String value = readCharsUntilStopChar((byte)'\"');
            if(endOfFileReached) return null;
            return value;
        }

        private String[] readSecondaryTagValues() throws IOException {
            final String[] values = new String[secondaryTags.size()];
            Arrays.fill(values,MISSING_VALUE);
            while (true) {
                int b0 = readByte();

                // if end of file has reached
                if(endOfFileReached) { return null;}

                if (b0 == TAG_OPENING) {
                    String tagValue = readCharsUntilTagClosing();
                    if (secondaryTags.contains(tagValue)) {
                        int idx = secondaryTags.indexOf(tagValue);
                        String value = readCharsUntilStopChar((byte)'<');
                        value = DblpCharMapping.unescapeXMLChars(value);
                        values[idx] = (values[idx].equals(MISSING_VALUE)) ? value : values[idx];
                    } else if(tagValue.startsWith("/") &&
                            primaryTags.contains(tagValue.substring(1))) break;
                }
            }
            return values;
        }
    }
}
