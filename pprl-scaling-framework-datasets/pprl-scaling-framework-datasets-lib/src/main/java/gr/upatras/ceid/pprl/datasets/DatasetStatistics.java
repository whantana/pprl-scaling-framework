package gr.upatras.ceid.pprl.datasets;

import gr.upatras.ceid.pprl.base.QGramUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class DatasetStatistics implements Serializable {
    private long recordCount;
    private long fieldCount;
    private long pairCount;
    private Map<String,DatasetFieldStatistics> fieldStatistics =
            new HashMap<String,DatasetFieldStatistics>();
    private double estimatedDuplicatePercentage;
    private int emAlgorithmIterations;

    public long getRecordCount() {
        return recordCount;
    }

    public void setRecordCount(long recordCount) {
        this.recordCount = recordCount;
    }

    public long getFieldCount() {
        return fieldCount;
    }

    public void setFieldCount(long fieldCount) {
        this.fieldCount = fieldCount;
    }

    public long getPairCount() {
        return pairCount;
    }

    public void setPairCount(long pairCount) {
        this.pairCount = pairCount;
    }

    public Map<String, DatasetFieldStatistics> getFieldStatistics() {
        return fieldStatistics;
    }

    public void setFieldStatistics(Map<String, DatasetFieldStatistics> fieldStatistics) {
        this.fieldStatistics = fieldStatistics;
    }

    public double getEstimatedDuplicatePercentage() {
        return estimatedDuplicatePercentage;
    }

    public void setEstimatedDuplicatePercentage(double estimatedDuplicatePercentage) {
        this.estimatedDuplicatePercentage = estimatedDuplicatePercentage;
    }

    public int getEmAlgorithmIterations() {
        return emAlgorithmIterations;
    }

    public void setEmAlgorithmIterations(int emAlgorithmIterations) {
        this.emAlgorithmIterations = emAlgorithmIterations;
    }

    public void setFieldNames(final String[] fieldNames) {
        for(String fieldName : fieldNames) fieldStatistics.put(fieldName,new DatasetFieldStatistics());
        setFieldCount(fieldNames.length);
    }

    @Override
    public String toString() {
        return "DatasetStatistics{" +
                "recordCount=" + recordCount +
                ", fieldCount=" + fieldCount +
                ", pairCount=" + pairCount +
                ", fieldStatistics=" + fieldStatistics +
                '}';
    }

    public Properties toProperties(){
        final Properties properties = new Properties();
        properties.setProperty("record.count",String.valueOf(getRecordCount()));
        properties.setProperty("field.count",String.valueOf(getFieldCount()));
        properties.setProperty("pair.count",String.valueOf(getPairCount()));
        properties.setProperty("em.algorithm.iterations",String.valueOf(getEmAlgorithmIterations()));
        properties.setProperty("em.estimated.p",String.valueOf(getEstimatedDuplicatePercentage()));
        for (Map.Entry<String,DatasetFieldStatistics> entry : fieldStatistics.entrySet()) {
            final String fieldName = entry.getKey();
            final DatasetFieldStatistics fieldStats = entry.getValue();
            final String firstKey  = "field." + fieldName;
            int i =0;
            for (String prop : DatasetFieldStatistics.props)
                properties.setProperty(firstKey + "." + prop,String.format("%.5f",fieldStats.getStats()[i++]));
        }
        return properties;
    }

    public void fromProperties(final Properties properties) {
        // TODO if needed
    }


    public static void calculateAvgQgramsLength(final GenericRecord[] records,
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
                for (int i = 0; i < updateQgram.length; i++) {
                    updateQgram[i] = ((double) QGramUtil.calcQgramsCount(
                            obj, type, DatasetFieldStatistics.Q_GRAMS[i]) / (double) statistics.getRecordCount());
                }

                fieldStats.incrementAvgFieldLengthBy((update));
                fieldStats.incrementAvgQgramCountBy(updateQgram);
            }
        }

    }

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
        for (int i = 0; i < fieldNames.length; i++) {
            double wrangeNormalized = statistics.getFieldStatistics().get(fieldNames[i]).getRange() / wrangeSum;
            statistics.getFieldStatistics().get(fieldNames[i])
                    .setNormalizedRange(wrangeNormalized);
        }
    }
}
