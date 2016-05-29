package gr.upatras.ceid.pprl.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A text pair writable class.
 */
public class TextPairWritable implements WritableComparable<TextPairWritable> {

    public Text first;
    public Text second;

    public Text getFirst() {
        return first;
    }

    public Text getSecond() {
        return second;
    }

    public TextPairWritable() {
        first = new Text();
        second = new Text();
    }

    public TextPairWritable(final String first, final String second) {

        this(new Text(first), new Text(second));
    }

    public TextPairWritable(final Text first, final Text second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        first.readFields(in);
        second.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        first.write(out);
        second.write(out);

    }

    @Override
    public int compareTo(TextPairWritable that) {
        int cmp = first.compareTo(that.first);
        if (cmp == 0) {
            cmp = second.compareTo(that.second);
        }
        return cmp;
    }

    @Override
    public boolean equals(Object obj) {

        if (obj instanceof TextPairWritable) {
            TextPairWritable that = (TextPairWritable) obj;
            return (first.equals(that.first) && second.equals(that.second));
        }

        return false;
    }

    @Override
    public int hashCode() {
        return first.hashCode() + 167 * second.hashCode();
    }

    @Override
    public String toString() {
        return "[" + first.toString() + "," + second.toString() + "]";
    }

}
