package gr.upatras.ceid.pprl.datasets;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Dataset/Field statistics class.
 */
public class DatasetFieldStatistics implements Serializable {

    private double length;              // avg length of field values.
    private double[] qgramCount;        // avg qram count of field values.
    private double[] uniqueQgramCount;  // avg unique qram count of field values.
    private double estimatedM;          // estimated(EM) M probability.
    private double estimatedU;          // estimated(EM) U probability.
    private double agreeWeight;         // agree weight
    private double disagreeWeight;      // disagree weight
    private double range;               // range = agree - disagree
    private double normalizedRange;     // normalized range

    public static final int[] Q_GRAMS = new int[3];
    static {
        Q_GRAMS[0] = 2;             // available Q-grams (Q : {2,3,4})
        Q_GRAMS[1] = 3;
        Q_GRAMS[2] = 4;
    }
    public static final String[] description = new String[]{   // description
            "Avg length",
            "Avg 2-grams count",
            "Avg 3-grams count",
            "Avg 4-grams count",
            "Avg Unique 2-grams count",
            "Avg Unique 3-grams count",
            "Avg Unique 4-grams count",
            "F-S m-probability",
            "F-S u-probability",
            "Agreement Weight",
            "Disagreement Weight",
            "Weight range",
            "Normaliszed weight range"
    };

    public static final String[] props = new String[]{  // property names references
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

    /**
     * Constructor
     */
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

    /**
     * Returns avg length of field.
     *
     * @return avg length of field.
     */
    public double getLength() {
        return length;
    }

    /**
     * Set avg length of field.
     *
     * @param length avg length of field.
     */
    public void setLength(double length) {
        this.length = length;
    }

    /**
     * Returns avg q gram count of field.
     *
     * @return array with avg q-grams [q=2,3,4].
     */
    public double[] getQgramCount() {
        return qgramCount;
    }

    /**
     * Returns avg q gram count of field.
     *
     * @param Q as in Q-Grams.
     * @return the avg qgram count for the selected q.
     */
    public double getQgramCount(final int Q) {
        assert (Q-2) >= 0 && (Q-2) < 3;
        return this.qgramCount[Q-2];
    }

    /**
     * Sets the avg q gram count for all available q.
     *
     * @param qgramCount q-gram count array.
     */
    public void setQgramCount(double[] qgramCount) {
        assert qgramCount.length == Q_GRAMS.length;
        this.qgramCount = qgramCount;
    }

    /**
     * Sets the avg q gram count for a selected q.
     *
     * @param Q as in Q-Grams.
     * @param qgramCount avg q gram count.
     */
    public void setQgramCount(final int Q, final double qgramCount) {
        assert (Q-2) >= 0 && (Q-2) < 3;
        this.qgramCount[Q-2] = qgramCount;
    }

    /**
     * Returns avg unique q-gram count of field.
     *
     * @return array with avg q-grams [q=2,3,4].
     */
    public double[] getUniqueQgramCount() {
        return uniqueQgramCount;
    }

    /**
     * Returns avg unique q-gram count of field.
     *
     * @param Q as in Q-Grams.
     * @return avg unique q-gram count of field for selected q.
     */
    public double getUniqueQgramCount(final int Q) {
        assert (Q-2) >= 0 && (Q-2) < 3;
        return this.uniqueQgramCount[Q-2];
    }

    /**
     * Set avg unique q-gram count of field.
     *
     * @param qgramCount array of avg unique q-gram count.
     */
    public void setUniqueQgramCount(double[] qgramCount) {
        assert qgramCount.length == Q_GRAMS.length;
        this.uniqueQgramCount = qgramCount;
    }

    /**
     * Set unique avg q-gram count for selected Q.
     *
     * @param Q as in Q-Grams.
     * @param qgramCount avg unique q gram count.
     */
    public void setUniqueQgramCount(final int Q, final double qgramCount) {
        assert (Q-2) >= 0 && (Q-2) < 3;
        this.uniqueQgramCount[Q-2] = qgramCount;
    }

    /**
     * Returns m probability.
     *
     * @return m probability.
     */
    public double getM() {
        return estimatedM;
    }

    /**
     * Set estimated m probability as m.
     *
     * @param estimatedM estimated m probability.
     */
    public void setM(double estimatedM) {
        this.estimatedM = estimatedM;
    }

    /**
     * Returns u probability.
     *
     * @return u probability.
     */
    public double getU() {
        return estimatedU;
    }

    /**
     * Set estimated u probability as m.
     *
     * @param estimatedU estimated u probability.
     */
    public void setU(double estimatedU) {
        this.estimatedU = estimatedU;
    }

    /**
     * Return agreement weight.
     *
     * @return agreement weight.
     */
    public double getAgreeWeight() {
        return agreeWeight;
    }

    /**
     * Set agreement weight.
     *
     * @param agreeWeight agreement weight.
     */
    public void setAgreeWeight(double agreeWeight) {
        this.agreeWeight = agreeWeight;
    }

    /**
     * Return disagreement weight.
     *
     * @return disagreement weight.
     */
    public double getDisagreeWeight() {
        return disagreeWeight;
    }

    /**
     * Set disagreement weight.
     *
     * @param disagreeWeight disagreement weight.
     */
    public void setDisagreeWeight(double disagreeWeight) {
        this.disagreeWeight = disagreeWeight;
    }

    /**
     * Returns range.
     *
     * @return range.
     */
    public double getRange() {
        return range;
    }

    /**
     * Sets range.
     *
     * @param range range.
     */
    public void setRange(double range) {
        this.range = range;
    }

    /**
     * Returns normailzed range.
     *
     * @return normailzed range.
     */
    public double getNormalizedRange() {
        return normalizedRange;
    }

    /**
     * Sets normalized range.
     * @param normalizedRange normalized range.
     */
    public void setNormalizedRange(double normalizedRange) {
        this.normalizedRange = normalizedRange;
    }

    /**
     * Returns field statistics in a single double array.
     *
     * @return field statistics in a single double array.
     */
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

    /**
     * Sets field statistics from an double array.
     *
     * @param stats double array containing statistics in a specific order.
     */
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

    /**
     * Increment length by update.
     *
     * @param update update.
     */
    public void incrementLength(final double update) {
        length += update;
    }

    /**
     * Increment all q-gram counts by update
     *
     * @param update update.
     */
    public void incrementQgramCount(final double[] update) {
        assert qgramCount.length == update.length;
        for (int i = 0; i < qgramCount.length; i++) {
            qgramCount[i] += update[i];
        }
    }

    /**
     * Increment all q-gram counts by update
     *
     * @param update update.
     */
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

    /**
     * Returns a pretty string representation of this object.
     *
     * @param fieldStatistics field statistics object.
     * @return a pretty string representation.
     */
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
