package gr.upatras.ceid.pprl.datasets.mapreduce;


import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class StatsWritable implements Writable {

    private int fieldLength;
    private int fieldQgramCount;

    public void write(DataOutput out) throws IOException {
        out.writeInt(fieldLength);
        out.writeInt(fieldQgramCount);
    }

    public void readFields(DataInput in) throws IOException {
        fieldLength = in.readInt();
        fieldQgramCount = in.readInt();
    }

    public int getFieldLength() {
        return fieldLength;
    }

    public void setFieldLength(int fieldLength) {
        this.fieldLength = fieldLength;
    }

    public int getFieldQgramCount() {
        return fieldQgramCount;
    }

    public void setFieldQgramCount(int fieldQgramCount) {
        this.fieldQgramCount = fieldQgramCount;
    }
}
