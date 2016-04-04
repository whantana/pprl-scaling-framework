package gr.upatras.ceid.pprl.datasets;

import gr.upatras.ceid.pprl.qgram.QGramUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Dataset statistics class.
 */
public class DatasetStatistics implements Serializable {
    private long recordCount;                                                        // record count
    private Map<String,DatasetFieldStatistics> fieldStatistics = new HashMap<String,DatasetFieldStatistics>();   // field statistics map
    private double emEstimatedP;                                                     // proportion of estimated(EM) True Matching pairs.
    private long emPairsCount;                                                       // number of record-pairs used by the estimator
    private int emAlgorithmIterations;                                               // iterations of algorithm

    /**
     * Return record count.
     *
     * @return record count.
     */
    public long getRecordCount() {
        return recordCount;
    }

    /**
     * Sets record count.
     *
     * @param recordCount record count.
     */
    public void setRecordCount(long recordCount) {
        this.recordCount = recordCount;
    }

    /**
     * Returns field count.
     *
     * @return field count.
     */
    public int getFieldCount() {
        return fieldStatistics.keySet().size();
    }

    /**
     * Returns pairs count used in EM.
     *
     * @return
     */
    public long getEmPairsCount() {
        return emPairsCount;
    }

    /**
     * Set pairs count used in EM.
     *
     * @param emPairsCount pair count used in EM.
     */
    public void setEmPairsCount(long emPairsCount) {
        this.emPairsCount = emPairsCount;
    }

    /**
     * Returns the field statistics map.
     *
     * @return the field statistics map.
     */
    public Map<String, DatasetFieldStatistics> getFieldStatistics() {
        return fieldStatistics;
    }

    /**
     * Sets the field statistics map.
     *
     * @param fieldStatistics the field statistics map.
     */
    public void setFieldStatistics(Map<String, DatasetFieldStatistics> fieldStatistics) {
        this.fieldStatistics = fieldStatistics;
    }

    /**
     * Returns P, the proportion of estimated(EM) True Matching pairs.
     *
     * @return P.
     */
    public double getP() {
        return emEstimatedP;
    }

    /**
     * Sets P, the proportion of estimated(EM) True Matching pairs.
     *
     * @param emEstimatedP P.
     */
    public void setP(double emEstimatedP) {
        this.emEstimatedP = emEstimatedP;
    }

    /**
     * Returns the iterations of the EM algorithm executed.
     *
     * @return EM iterations.
     */
    public int getEmAlgorithmIterations() {
        return emAlgorithmIterations;
    }

    /**
     * Sets the itrations of the EM  algorithm exectuted.
     *
     * @param emAlgorithmIterations  EM iterations.
     */
    public void setEmAlgorithmIterations(int emAlgorithmIterations) {
        this.emAlgorithmIterations = emAlgorithmIterations;
    }

    /**
     * Initialize the field statistics map with these fieldnames.
     *
     * @param fieldNames field names.
     */
    public void setFieldNames(final String[] fieldNames) {
        for(String fieldName : fieldNames)
            if(!fieldStatistics.containsKey(fieldName)) fieldStatistics.put(fieldName,new DatasetFieldStatistics());
    }

    /**
     * Returns the field names from the field statistics map.
     *
     * @return the field names.
     */
    public String[] getFieldNames() {
        return fieldStatistics.keySet().toArray(new String[getFieldCount()]);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DatasetStatistics that = (DatasetStatistics) o;

        if (recordCount != that.recordCount) return false;
        if (Double.compare(that.emEstimatedP, emEstimatedP) != 0) return false;
        if (emPairsCount != that.emPairsCount) return false;
        if (emAlgorithmIterations != that.emAlgorithmIterations) return false;
        return fieldStatistics.equals(that.fieldStatistics);

    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        result = (int) (recordCount ^ (recordCount >>> 32));
        result = 31 * result + fieldStatistics.hashCode();
        temp = Double.doubleToLongBits(emEstimatedP);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        result = 31 * result + (int) (emPairsCount ^ (emPairsCount >>> 32));
        result = 31 * result + emAlgorithmIterations;
        return result;
    }

    @Override
    public String toString() {
        return "DatasetStatistics{" +
                "recordCount=" + recordCount +
                ", emPairsCount=" + emPairsCount +
                ", fieldStatistics=" + fieldStatistics +
                '}';
    }

    /**
     * Converts this statistics object to a <code>Properties</code> object.
     *
     * @return a <code>Properties</code> object
     */
    public Properties toProperties(){
        final Properties properties = new Properties();

        properties.setProperty("record.count",String.valueOf(getRecordCount()));

        properties.setProperty("em.pair.count",String.valueOf(getEmPairsCount()));
        properties.setProperty("em.algorithm.iterations",String.valueOf(getEmAlgorithmIterations()));
        properties.setProperty("em.estimated.p",String.valueOf(getP()));

        StringBuilder sb = new StringBuilder();
        int f = 0;
        for (Map.Entry<String,DatasetFieldStatistics> entry : getFieldStatistics().entrySet()) {
            final String fieldName = entry.getKey();
            final DatasetFieldStatistics fieldStats = entry.getValue();
            final String firstKey  = "f." + fieldName;
            int i =0;
            for (String prop : DatasetFieldStatistics.props)
                properties.setProperty(firstKey + "." + prop,String.valueOf(fieldStats.getStats()[i++]));
            sb.append(fieldName);
            if(f < getFieldCount() - 1) sb.append(",");
            f++;
        }
        properties.setProperty("field.names",sb.toString());
        return properties;
    }

    /**
     * Setup this statistics object based on a <code>Properties</code> object.
     *
     * @param properties a <code>Properties</code> object.
     */
    public void fromProperties(final Properties properties) {
        if(properties.getProperty("record.count") != null)
            setRecordCount(Long.parseLong(properties.getProperty("record.count")));
        if(properties.getProperty("em.pair.count") != null)
            setEmPairsCount(Long.parseLong(properties.getProperty("em.pair.count")));
        if(properties.getProperty("em.algorithm.iterations") != null)
            setEmAlgorithmIterations(Integer.parseInt(properties.getProperty("em.algorithm.iterations")));
        if(properties.getProperty("em.estimated.p") != null)
            setP(Double.parseDouble(properties.getProperty("em.estimated.p")));

        if(properties.getProperty("field.names") != null) {
            String[] fieldNames = properties.getProperty("field.names").split(",");
            setFieldNames(fieldNames);
        }

        if(getFieldStatistics().isEmpty()) return;

        for (Map.Entry<String,DatasetFieldStatistics> entry : getFieldStatistics().entrySet()) {
            final String fieldName = entry.getKey();
            final DatasetFieldStatistics fieldStats = entry.getValue();
            int i = 0;
            double[] stats = new double[DatasetFieldStatistics.props.length];
            for (String prop : DatasetFieldStatistics.props) {
                final String key = "f." + fieldName + "." + prop;
                if(properties.getProperty(key) != null)
                    stats[i] = Double.valueOf(properties.getProperty(key));
                i++;
            }
            fieldStats.setStats(stats);
        }
    }

    /**
     * Calculate avg length and avg Q-Gram counts from an array of Avro Records.
     * Updates the respected statistics in the <code>statistics</code> object.
     *
     * @param records avro records array.
     * @param schema schema of records.
     * @param statistics statistics object.
     * @param fieldNames field names.
     */
    public static void calculateQgramStatistics(final GenericRecord[] records,
                                                final Schema schema,
                                                final DatasetStatistics statistics,
                                                final String[] fieldNames) {
        for (GenericRecord record : records) {
            for (String fieldName : fieldNames) {
                DatasetFieldStatistics fieldStats = statistics.getFieldStatistics().get(fieldName);
                Object obj = record.get(fieldName);
                Schema.Type type = schema.getField(fieldName).schema().getType();
                double update = (double) String.valueOf(obj).length() / (double) statistics.getRecordCount();
                double[] updateQgram = new double[DatasetFieldStatistics.Q_GRAMS.length];
                double[] updateUniqueQgram = new double[DatasetFieldStatistics.Q_GRAMS.length];
                for (int i = 0; i < updateQgram.length; i++) {
                    updateQgram[i] = ((double) QGramUtil.calcQgramsCount(
                            obj, type, DatasetFieldStatistics.Q_GRAMS[i]) / (double) statistics.getRecordCount());
                    updateUniqueQgram[i] = ((double) QGramUtil.calcUniqueQgramsCount(
                            obj, type, DatasetFieldStatistics.Q_GRAMS[i]) / (double) statistics.getRecordCount());
                }

                fieldStats.incrementLength((update));
                fieldStats.incrementQgramCount(updateQgram);
                fieldStats.incrementUniqueQgramCount(updateUniqueQgram);
            }
        }

    }

    /**
     * Calculate statistics using the EM estimates as input for the selected fields.
     *
     * @param statistics statistics object.
     * @param fieldNames field names.
     * @param m m-probability array for each field name.
     * @param u u-probability array for each field name.
     */
    public static void calculateStatsUsingEstimates(final DatasetStatistics statistics,
                                                    final String[] fieldNames,
                                                    final double[] m, final double[] u) {
        double wrangeSum = 0;

        for (int i = 0; i < fieldNames.length ; i++) {
            statistics.getFieldStatistics().get(fieldNames[i]).setM(m[i]);
            statistics.getFieldStatistics().get(fieldNames[i]).setU(u[i]);
            final double wa =
                    Math.log(m[i] == 0 ? Double.MIN_NORMAL : m[i]) -
                            Math.log(u[i] == 0 ? Double.MIN_NORMAL : u[i]);
            final double wd =
                    Math.log((1 - m[i] ) == 0 ? Double.MIN_NORMAL : (1 - m[i])) -
                            Math.log((1 - u[i] ) == 0 ? Double.MIN_NORMAL : (1 - u[i]));
            statistics.getFieldStatistics().get(fieldNames[i]).setAgreeWeight(wa);
            statistics.getFieldStatistics().get(fieldNames[i]).setDisagreeWeight(wd);
            final double wrange = Math.abs(wa - wd);
            statistics.getFieldStatistics().get(fieldNames[i]).setRange(wrange);
            wrangeSum += wrange;
        }

        for (String fieldName : fieldNames) {
            double wrangeNormalized = statistics.getFieldStatistics().get(fieldName).getRange() / wrangeSum;
            statistics.getFieldStatistics().get(fieldName)
                    .setNormalizedRange(wrangeNormalized);
        }
    }
}
