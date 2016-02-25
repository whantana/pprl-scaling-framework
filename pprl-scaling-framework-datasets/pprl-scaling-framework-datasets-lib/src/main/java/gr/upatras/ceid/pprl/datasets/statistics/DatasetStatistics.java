package gr.upatras.ceid.pprl.datasets.statistics;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

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



    public void calculateFieldStatistics(final GenericRecord[] records,
                                         final Schema schema,
                                         final String[] fieldNames)
            throws IOException {

        int recordCount = records.length;
        int fieldCount = fieldNames.length;
        for (GenericRecord record : records) {
            for (String fieldName : fieldNames) {
                DatasetFieldStatistics fieldStats = fieldStatistics.get(fieldName);
                Object obj = record.get(fieldName);
                Schema.Type type = schema.getField(fieldName).schema().getType();
                double update = (double) String.valueOf(obj).length() / (double) recordCount;
                double[] updateQgram = new double[DatasetFieldStatistics.Q_GRAMS.length];
                for (int i = 0; i < updateQgram.length; i++) {
                    updateQgram[i] = ((double) QGramUtil.calcQgramsCount(
                            obj, type, DatasetFieldStatistics.Q_GRAMS[i]) / (double) recordCount);
                }

                fieldStats.incrementAvgFieldLengthBy((update));
                fieldStats.incrementAvgQgramCountBy(updateQgram);
            }
        }

        final Gamma gamma = Gamma.createGamma(records,fieldNames,false);

        final ExpectationMaximization estimator =
                new ExpectationMaximization(fieldCount,0.9,0.1,0.1);
        estimator.runAlgorithm(gamma);
        double wrangeSum = 0;
        for (int i = 0; i < fieldNames.length ; i++) {
            final double m = estimator.getM()[i];
            final double u = estimator.getU()[i];
            fieldStatistics.get(fieldNames[i]).setM(m);
            fieldStatistics.get(fieldNames[i]).setU(u);
            final double wa =
                    Math.log(m == 0 ? Double.MIN_NORMAL : m) -
                    Math.log(u == 0 ? Double.MIN_NORMAL : u);
            final double wd =
                    Math.log((1 - m ) == 0 ? Double.MIN_NORMAL : (1 - m)) -
                    Math.log((1 - u ) == 0 ? Double.MIN_NORMAL : (1 - u));
            fieldStatistics.get(fieldNames[i]).setAgreeWeight(wa);
            fieldStatistics.get(fieldNames[i]).setDisagreeWeight(wd);
            final double wrange = Math.abs(wa - wd);
            fieldStatistics.get(fieldNames[i]).setRange(wrange);
            wrangeSum += wrange;
        }
        for (int i = 0; i < fieldNames.length; i++) {
            double wrangeNormalized =
                    fieldStatistics.get(fieldNames[i])
                            .getRange() / wrangeSum;
            fieldStatistics.get(fieldNames[i])
                    .setNormalizedRange(wrangeNormalized);
        }

        emAlgorithmIterations = estimator.getIteration();
        estimatedDuplicatePercentage = estimator.getP();
    }
}
