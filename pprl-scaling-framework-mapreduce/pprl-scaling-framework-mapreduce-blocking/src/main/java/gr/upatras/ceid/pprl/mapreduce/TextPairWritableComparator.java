package gr.upatras.ceid.pprl.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

import java.io.IOException;

/**
 * TextPairWritable comparator class.
 */
public class TextPairWritableComparator extends WritableComparator {

    public Text.Comparator TEXT_COMPARATOR = new Text.Comparator();

    public TextPairWritableComparator() {
        super(TextPairWritable.class);
    }


    public int compare(byte[] b1, int s1, int l1,
                       byte[] b2, int s2, int l2) {
        try {
            int firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
            int firstL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);
            int cmp = TEXT_COMPARATOR.compare(b1, s1, firstL1, b2, s2, firstL2);
            if (cmp != 0) {
                return cmp;
            }
            return TEXT_COMPARATOR.compare(b1, s1 + firstL1,
                    l1 - firstL1, b2, s2 + firstL2, l2 - firstL2);
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    static {
        // Register comparator
        WritableComparator.define(TextPairWritable.class, new TextPairWritableComparator());
    }
}
