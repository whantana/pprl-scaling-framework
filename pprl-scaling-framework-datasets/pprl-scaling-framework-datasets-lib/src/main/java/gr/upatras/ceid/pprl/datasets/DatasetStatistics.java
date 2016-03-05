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
    private Map<String,DatasetFieldStatistics> fieldStatistics = new HashMap<String,DatasetFieldStatistics>();
    private double emEstimatedP;
    private long emPairs;
    private int emAlgorithmIterations;

    public long getRecordCount() {
        return recordCount;
    }

    public void setRecordCount(long recordCount) {
        this.recordCount = recordCount;
    }

    public int getFieldCount() {
        return fieldStatistics.keySet().size();
    }

    public long getEmPairs() {
        return emPairs;
    }

    public void setEmPairs(long emPairs) {
        this.emPairs = emPairs;
    }

    public Map<String, DatasetFieldStatistics> getFieldStatistics() {
        return fieldStatistics;
    }

    public void setFieldStatistics(Map<String, DatasetFieldStatistics> fieldStatistics) {
        this.fieldStatistics = fieldStatistics;
    }

    public double getP() {
        return emEstimatedP;
    }

    public void setP(double emEstimatedP) {
        this.emEstimatedP = emEstimatedP;
    }

    public int getEmAlgorithmIterations() {
        return emAlgorithmIterations;
    }

    public void setEmAlgorithmIterations(int emAlgorithmIterations) {
        this.emAlgorithmIterations = emAlgorithmIterations;
    }

    public void setFieldNames(final String[] fieldNames) {
        for(String fieldName : fieldNames)
            if(!fieldStatistics.containsKey(fieldName)) fieldStatistics.put(fieldName,new DatasetFieldStatistics());
    }

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
        if (emPairs != that.emPairs) return false;
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
        result = 31 * result + (int) (emPairs ^ (emPairs >>> 32));
        result = 31 * result + emAlgorithmIterations;
        return result;
    }

    @Override
    public String toString() {
        return "DatasetStatistics{" +
                "recordCount=" + recordCount +
                ", emPairs=" + emPairs +
                ", fieldStatistics=" + fieldStatistics +
                '}';
    }

    public Properties toProperties(){
        final Properties properties = new Properties();

        properties.setProperty("record.count",String.valueOf(getRecordCount()));

        properties.setProperty("em.pair.count",String.valueOf(getEmPairs()));
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

    public void fromProperties(final Properties properties) {

        setRecordCount(Long.parseLong(properties.getProperty("record.count")));
        setEmPairs(Long.parseLong(properties.getProperty("em.pair.count")));
        setEmAlgorithmIterations(Integer.parseInt(properties.getProperty("em.algorithm.iterations")));
        setP(Double.parseDouble(properties.getProperty("em.estimated.p")));

        String[] fieldNames = properties.getProperty("field.names").split(",");
        setFieldNames(fieldNames);

        for (Map.Entry<String,DatasetFieldStatistics> entry : getFieldStatistics().entrySet()) {
            final String fieldName = entry.getKey();
            final DatasetFieldStatistics fieldStats = entry.getValue();
            final String firstKey = "f." + fieldName;
            int i = 0;
            double[] stats = new double[DatasetFieldStatistics.props.length];
            for (String prop : DatasetFieldStatistics.props)
                stats[i++] = Double.valueOf(properties.getProperty(firstKey + "." + prop));
            fieldStats.setStats(stats);
        }
    }


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
