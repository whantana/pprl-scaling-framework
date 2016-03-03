package gr.upatras.ceid.pprl.datasets;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;

public class DatasetFieldStatistics implements Serializable,Writable {

    private double fieldLength;
    private double[] fieldQgramCount;
    private double estimatedM;
    private double estimatedU;
    private double agreeWeight;
    private double disagreeWeight;
    private double range;
    private double normalizedRange;

    public static final int[] Q_GRAMS = new int[3];
    static {
        Q_GRAMS[0] = 2;
        Q_GRAMS[1] = 3;
        Q_GRAMS[2] = 4;
    }
    public static final String[] description = new String[10];
    static {
        description[0] = "Avg length";
        description[1] = "Avg 2-grams count";
        description[2] = "Avg 3-grams count";
        description[3] = "Avg 4-grams count";
        // TODO Add unique q gram count
        description[4] = "F-S m-probability";
        description[5] = "F-S u-probability";
        description[6] = "Agreement Weight";
        description[7] = "Disagreement Weight";
        description[8] = "Weight range";
        description[9] = "Normaliszed weight range";
    }

    public static final String[] props = new String[10];
    static {
        props[0] = "avg.length";
        props[1] = "avg.2grams.count";
        props[2] = "avg.3grams.count";
        props[3] = "avg.4grams.count";
        props[4] = "m.probability";
        props[5] = "u.probability";
        props[6] = "agreement.weight";
        props[7] = "disagreement.weight";
        props[8] = "weight.range";
        props[9] = "normalized.weight.range";
    }

    public DatasetFieldStatistics(){
        fieldLength = 0;
        fieldQgramCount = new double[Q_GRAMS.length];
        fieldQgramCount[0] = 0;
        fieldQgramCount[1] = 0;
        fieldQgramCount[2] = 0;
        estimatedM = 0;
        estimatedU = 0;
        agreeWeight = 0;
        disagreeWeight = 0;
        range = 0;
        normalizedRange = 0;
    }

    public DatasetFieldStatistics(double fieldLength, double[] fieldQgramCount){
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

    public double getFieldQGramCount(final int Q) {
        return this.fieldQgramCount[Arrays.binarySearch(Q_GRAMS,Q)];
    }

    public double getM() {
        return estimatedM;
    }

    public void setM(double estimatedM) {
        this.estimatedM = estimatedM;
    }

    public double getU() {
        return estimatedU;
    }

    public void setU(double estimatedU) {
        this.estimatedU = estimatedU;
    }

    public double getAgreeWeight() {
        return agreeWeight;
    }

    public void setAgreeWeight(double agreeWeight) {
        this.agreeWeight = agreeWeight;
    }

    public double getDisagreeWeight() {
        return disagreeWeight;
    }

    public void setDisagreeWeight(double disagreeWeight) {
        this.disagreeWeight = disagreeWeight;
    }

    public double getRange() {
        return range;
    }

    public void setRange(double range) {
        this.range = range;
    }

    public double getNormalizedRange() {
        return normalizedRange;
    }

    public void setNormalizedRange(double normalizedRange) {
        this.normalizedRange = normalizedRange;
    }

    public double[] getStats() {
        return new double[]{
                fieldLength,
                fieldQgramCount[0], fieldQgramCount[1], fieldQgramCount[2],
                estimatedM, estimatedU,
                agreeWeight, disagreeWeight,
                range,normalizedRange
        };
    }

    public void incrementAvgFieldLengthBy(final double update) {
        fieldLength += update;
    }

    public void incrementAvgQgramCountBy(final double[] update) {
        assert fieldQgramCount.length == update.length;
        for (int i = 0; i < fieldQgramCount.length; i++) {
            fieldQgramCount[i] += update[i];
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DatasetFieldStatistics that = (DatasetFieldStatistics) o;

        if (Double.compare(that.fieldLength, fieldLength) != 0) return false;
        if (Double.compare(that.estimatedM, estimatedM) != 0) return false;
        if (Double.compare(that.estimatedU, estimatedU) != 0) return false;
        if (Double.compare(that.agreeWeight, agreeWeight) != 0) return false;
        if (Double.compare(that.disagreeWeight, disagreeWeight) != 0) return false;
        if (Double.compare(that.range, range) != 0) return false;
        if (Double.compare(that.normalizedRange, normalizedRange) != 0) return false;
        return Arrays.equals(fieldQgramCount, that.fieldQgramCount);

    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        temp = Double.doubleToLongBits(fieldLength);
        result = (int) (temp ^ (temp >>> 32));
        result = 31 * result + Arrays.hashCode(fieldQgramCount);
        temp = Double.doubleToLongBits(estimatedM);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(estimatedU);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(agreeWeight);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(disagreeWeight);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(range);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(normalizedRange);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "DatasetStatsWritable{" +
                "fieldLength=" + fieldLength +
                ", fieldQgramCount=" + Arrays.toString(fieldQgramCount) +
                ", estimatedM=" + estimatedM +
                ", estimatedU=" + estimatedU +
                '}';
    }

    public static String prettyStats(final DatasetFieldStatistics fieldStatistics) {
        final double stats[] = fieldStatistics.getStats();
        assert stats.length == description.length;
        final StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < stats.length; i++)
            sb.append(String.format(" %s = %.2f,",description[i],stats[i]));
        sb.append("]");
        return sb.toString();
    }
}
