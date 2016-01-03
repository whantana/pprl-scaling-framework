package gr.upatras.ceid.pprl.datasets;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DatasetStatsWritable implements Writable {

    private double fieldLength;
    private double fieldQgramCount;

    public DatasetStatsWritable(){}

    public DatasetStatsWritable(double fieldLength,double fieldQgramCount){
        this.fieldLength = fieldLength;
        this.fieldQgramCount = fieldQgramCount;
    }

    public void write(DataOutput out) throws IOException {
        out.writeDouble(fieldLength);
        out.writeDouble(fieldQgramCount);
    }

    public void readFields(DataInput in) throws IOException {
        fieldLength = in.readDouble();
        fieldQgramCount = in.readDouble();
    }

    public double getFieldLength() {
        return fieldLength;
    }

    public void setFieldLength(double fieldLength) {
        this.fieldLength = fieldLength;
    }

    public double getFieldQgramCount() {
        return fieldQgramCount;
    }

    public void setFieldQgramCount(double fieldQgramCount) {
        this.fieldQgramCount = fieldQgramCount;
    }

    public double[] getStats() { return new double[]{fieldLength,fieldQgramCount}; }

    public void setStats(double fl, double fqc) {
        fieldLength = fl;
        fieldQgramCount = fqc;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DatasetStatsWritable that = (DatasetStatsWritable) o;

        if (Double.compare(that.fieldLength, fieldLength) != 0) return false;
        return Double.compare(that.fieldQgramCount, fieldQgramCount) == 0;

    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        temp = Double.doubleToLongBits(fieldLength);
        result = (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(fieldQgramCount);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "DatasetStatsWritable{" +
                "fieldLength=" + fieldLength +
                ", fieldQgramCount=" + fieldQgramCount +
                '}';
    }
}
