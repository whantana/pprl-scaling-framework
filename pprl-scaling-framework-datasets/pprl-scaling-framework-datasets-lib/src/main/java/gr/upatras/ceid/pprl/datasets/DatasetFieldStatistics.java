package gr.upatras.ceid.pprl.datasets;

import java.io.Serializable;
import java.util.Arrays;

public class DatasetFieldStatistics implements Serializable {

    private double length;
    private double[] qgramCount;
    private double[] uniqueQgramCount;
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
    public static final String[] description = new String[]{
            "Avg length",
            "Avg 2-grams count",
            "Avg 3-grams count",
            "Avg 4-grams count",
            "Avg Unique 2-grams count",   // TODO worth keeping these ?
            "Avg Unique 3-grams count",
            "Avg Unique 4-grams count",
            "F-S m-probability",
            "F-S u-probability",
            "Agreement Weight",
            "Disagreement Weight",
            "Weight range",
            "Normaliszed weight range"
    };

    public static final String[] props = new String[]{
        "avg.length",
        "avg.2grams.count",
        "avg.3grams.count",
        "avg.4grams.count",
        "avg.unique.2grams.count",
        "avg.unique.3grams.count",
        "avg.unique.4grams.count",
        "em.estimated.m",
        "em.estimated.u",
        "agreement.weight",
        "disagreement.weight",
        "weight.range",
        "normalized.weight.range"
    };

    public DatasetFieldStatistics(){
        length = 0;
        qgramCount = new double[Q_GRAMS.length];
        qgramCount[0] = 0;
        qgramCount[1] = 0;
        qgramCount[2] = 0;
        uniqueQgramCount = new double[Q_GRAMS.length];
        uniqueQgramCount[0] = 0;
        uniqueQgramCount[1] = 0;
        uniqueQgramCount[2] = 0;
        estimatedM = 0;
        estimatedU = 0;
        agreeWeight = 0;
        disagreeWeight = 0;
        range = 0;
        normalizedRange = 0;
    }

    public double getLength() {
        return length;
    }

    public void setLength(double length) {
        this.length = length;
    }

    public double[] getQgramCount() {
        return qgramCount;
    }

    public double getQgramCount(final int Q) {
        assert (Q-2) >= 0 && (Q-2) < 3;
        return this.qgramCount[Q-2];
    }

    public void setQgramCount(double[] qgramCount) {
        assert qgramCount.length == Q_GRAMS.length;
        this.qgramCount = qgramCount;
    }

    public void setQgramCount(final int Q, final double qgramCount) {
        assert (Q-2) >= 0 && (Q-2) < 3;
        this.qgramCount[Q-2] = qgramCount;
    }

    public double[] getUniqueQgramCount() {
        return uniqueQgramCount;
    }

    public double getUniqueQgramCount(final int Q) {
        assert (Q-2) >= 0 && (Q-2) < 3;
        return this.uniqueQgramCount[Q-2];
    }


    public void setUniqueQgramCount(double[] qgramCount) {
        assert qgramCount.length == Q_GRAMS.length;
        this.uniqueQgramCount = qgramCount;
    }

    public void setUniqueQgramCount(final int Q, final double qgramCount) {
        assert (Q-2) >= 0 && (Q-2) < 3;
        this.uniqueQgramCount[Q-2] = qgramCount;
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
                getLength(),
                getQgramCount(2),getQgramCount(3),getQgramCount(4),
                getUniqueQgramCount(2),getUniqueQgramCount(3),getUniqueQgramCount(4),
                getM(),getU(),
                getAgreeWeight(),getDisagreeWeight(),
                getRange(),getNormalizedRange()
        };
    }

    public void setStats(final double[] stats) {
        assert stats.length == description.length && stats.length == props.length;
        setLength(stats[0]);
        setQgramCount(2,stats[1]);
        setQgramCount(3,stats[2]);
        setQgramCount(4,stats[3]);
        setUniqueQgramCount(2, stats[4]);
        setUniqueQgramCount(3, stats[5]);
        setUniqueQgramCount(4, stats[6]);
        setM(stats[7]);
        setU(stats[8]);
        setAgreeWeight(stats[9]);
        setDisagreeWeight(stats[10]);
        setRange(stats[11]);
        setNormalizedRange(stats[12]);
    }

    public void incrementLength(final double update) {
        length += update;
    }

    public void incrementQgramCount(final double[] update) {
        assert qgramCount.length == update.length;
        for (int i = 0; i < qgramCount.length; i++) {
            qgramCount[i] += update[i];
        }
    }

    public void incrementUniqueQgramCount(final double[] update) {
        assert uniqueQgramCount.length == update.length;
        for (int i = 0; i < uniqueQgramCount.length; i++) {
            uniqueQgramCount[i] += update[i];
        }
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DatasetFieldStatistics that = (DatasetFieldStatistics) o;

        if (Double.compare(that.length, length) != 0) return false;
        if (Double.compare(that.estimatedM, estimatedM) != 0) return false;
        if (Double.compare(that.estimatedU, estimatedU) != 0) return false;
        if (Double.compare(that.agreeWeight, agreeWeight) != 0) return false;
        if (Double.compare(that.disagreeWeight, disagreeWeight) != 0) return false;
        if (Double.compare(that.range, range) != 0) return false;
        if (Double.compare(that.normalizedRange, normalizedRange) != 0) return false;
        if (!Arrays.equals(qgramCount, that.qgramCount)) return false;
        return Arrays.equals(uniqueQgramCount, that.uniqueQgramCount);

    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        temp = Double.doubleToLongBits(length);
        result = (int) (temp ^ (temp >>> 32));
        result = 31 * result + Arrays.hashCode(qgramCount);
        result = 31 * result + Arrays.hashCode(uniqueQgramCount);
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
        return "DatasetFieldStatistics{" +
                "length=" + length +
                ", qgramCount=" + Arrays.toString(qgramCount) +
                ", uniqueQgramCount=" + Arrays.toString(uniqueQgramCount) +
                ", estimatedM=" + estimatedM +
                ", estimatedU=" + estimatedU +
                ", agreeWeight=" + agreeWeight +
                ", disagreeWeight=" + disagreeWeight +
                ", range=" + range +
                ", normalizedRange=" + normalizedRange +
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
