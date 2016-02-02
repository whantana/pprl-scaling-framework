package gr.upatras.ceid.pprl.datasets;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;

public class DatasetStatistics implements Serializable,Writable {
    // TODO more stats logic here
    public static final int[] Q_GRAMS = new int[3];
    static {
        Q_GRAMS[0] = 2;
        Q_GRAMS[1] = 3;
        Q_GRAMS[2] = 4;
    }

    private double fieldLength;
    private double[] fieldQgramCount;  // 0 for bigrams , 1 : trigrams, 2 : quadgrams

    public DatasetStatistics(){
        fieldLength = 0;
        fieldQgramCount = new double[3];
        fieldQgramCount[0] = 0;
        fieldQgramCount[1] = 0;
        fieldQgramCount[2] = 0;
    }

    public DatasetStatistics(double fieldLength, double[] fieldQgramCount){
        assert fieldQgramCount.length == Q_GRAMS.length;
        this.fieldLength = fieldLength;
        this.fieldQgramCount = fieldQgramCount;
    }

    public void write(DataOutput out) throws IOException {
        out.writeDouble(fieldLength);
        for (double aFieldQgramCount : fieldQgramCount) {
            out.writeDouble(aFieldQgramCount);
        }
    }

    public void readFields(DataInput in) throws IOException {
        fieldLength = in.readDouble();
        fieldQgramCount = new double[Q_GRAMS.length];
        for (int i = 0; i < Q_GRAMS.length; i++) {
            fieldQgramCount[i] = in.readDouble();
        }
    }

    public double getFieldLength() {
        return fieldLength;
    }

    public void setFieldLength(double fieldLength) {
        this.fieldLength = fieldLength;
    }

    public double[] getFieldQgramCount() {
        return fieldQgramCount;
    }

    public void setFieldQgramCount(double[] fieldQgramCount) {
        assert fieldQgramCount.length == Q_GRAMS.length;
        this.fieldQgramCount = fieldQgramCount;
    }

    public void setFieldQGramCount(final int Q, final double fieldQgramCount) {
        this.fieldQgramCount[Arrays.binarySearch(Q_GRAMS,Q)] = fieldQgramCount;
    }

    public double[] getStats() { return new double[]{fieldLength,fieldQgramCount[0],fieldQgramCount[1],fieldQgramCount[2]}; }

    public void setStats(double fl, double[] fqc) {
        assert fqc.length == 3;
        fieldLength = fl;
        fieldQgramCount = fqc;
    }

    public void updateFieldLength(final double update) {
        fieldLength += update;
    }

    public void updateFieldQGramCount(final double[] update) {
        assert fieldQgramCount.length == update.length;
        for (int i = 0; i < fieldQgramCount.length; i++) {
            fieldQgramCount[i] += update[i];
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DatasetStatistics that = (DatasetStatistics) o;

        if (Double.compare(that.fieldLength, fieldLength) != 0) return false;
        return Arrays.equals(fieldQgramCount, that.fieldQgramCount);

    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        temp = Double.doubleToLongBits(fieldLength);
        result = (int) (temp ^ (temp >>> 32));
        result = 31 * result + (fieldQgramCount != null ? Arrays.hashCode(fieldQgramCount) : 0);
        return result;
    }

    @Override
    public String toString() {
        return "DatasetStatsWritable{" +
                "fieldLength=" + fieldLength +
                ", fieldQgramCount=" + Arrays.toString(fieldQgramCount) +
                '}';
    }


    public static String prettyStats(final double[] stats) {
        if(stats.length != 4) throw new IllegalArgumentException("Stats length must be 4");
        return String.format(
                "[Average Length = %.2f," +
                " Average 2-gram Count = %.2f," +
                " Average 3-gram Count = %.2f," +
                " Average 4-gram Count = %.2f]"
                ,stats[0],stats[1],stats[2],stats[3]);
    }

    public static String prettyStats(final DatasetStatistics stats) {
        return String.format(
                "[Average Length = %.2f," +
                " Average 2-gram Count = %.2f," +
                " Average 3-gram Count = %.2f," +
                " Average 4-gram Count = %.2f]"
                ,stats.getFieldLength(),stats.getFieldQgramCount()[0],
                stats.getFieldQgramCount()[1],
                stats.getFieldQgramCount()[2]);
    }
}
