package gr.upatras.ceid.pprl.matching;

import java.util.Arrays;

/**
 * Expectation Maximization class.
 */
public class ExpectationMaximization {
    public static final int MAX_ITERATIONS = 1000;  // Maximum iterations
    private long pairCount;                         // Count of pairs
    private int fieldCount;                         // Count of fields
    private int iteration;                          // current iteration
    private double[] m;                             // probability m for each field
    private double[] u;                             // probability u for each field
    private double p;                               // proportion of estimated True-Matching Pairs to Total Pairs.
    private double[][] g;                           // vectors g

    /**
     * Constructor.
     *
     * @param fieldCount field count.
     * @param m0 initial m for all fields.
     * @param u0 initial u for all fields.
     * @param p0 initial p.
     */
    public ExpectationMaximization(int fieldCount, double m0, double u0, double p0) {
        this.fieldCount = fieldCount;
        this.m = new double[fieldCount];
        Arrays.fill(this.m,m0);
        this.u = new double[fieldCount];
        Arrays.fill(this.u,u0);
        this.p = p0;
    }

    /**
     * Constructor.
     *
     * @param fieldCount field count.
     * @param m0 initial m.
     * @param u0 initial u.
     * @param p0 initial p.
     */
    public ExpectationMaximization(int fieldCount, double[] m0, double[] u0, double p0) {
        assert fieldCount == m0.length;
        assert m0.length == u0.length;
        this.fieldCount = fieldCount;
        this.m = m0;
        this.u = u0;
        this.p  = p0;
    }

    /**
     * Constructor.
     *
     * @param fieldNames field names.
     * @param m0 initial m for all fields.
     * @param u0 initial u for all fields.
     * @param p0 initial p.
     */
    public ExpectationMaximization(String[] fieldNames, double m0, double u0, double p0) {
        this(fieldNames.length,m0,u0,p0);
    }

    /**
     * Constructor.
     *
     * @param fieldNames field names.
     * @param m0 initial m.
     * @param u0 initial u.
     * @param p0 initial p.
     */
    public ExpectationMaximization(String[] fieldNames, double[] m0, double[] u0, double p0) {
        this(fieldNames.length,m0,u0,p0);
    }

    /**
     * Run algorithm.
     *
     * @param frequencies a <code>SimilarityVectorFrequencies</code> instance.
     */
    public void runAlgorithm(final SimilarityVectorFrequencies frequencies) {
        pairCount = frequencies.getPairCount();
        assert frequencies.getFieldCount() == fieldCount;
        for(iteration = 1; iteration <= MAX_ITERATIONS; iteration++ ) {

            // Expectation Step - calculate g from frequencies. Size of g is 2^fieldCount
            g = new double[1 << fieldCount][2];
            double mSum = 0;
            double uSum = 0;
            for (int i = 0; i < g.length; i++) {
                double a = p;
                double b = 1 - p;
                boolean[] row = SimilarityVectorFrequencies.index2Vector(i, fieldCount);
                for(int j = 0; j < fieldCount ; j++) {
                    a *= row[j] ? m[j] : (1 - m[j]);
                    b *= row[j] ? u[j] : (1 - u[j]);
                }
                g[i][0] = a/(a+b);
                g[i][1] = b/(a+b);
                mSum += g[i][0]* frequencies.getVectorFrequency(i);
                uSum += g[i][1]* frequencies.getVectorFrequency(i);
            }

            // Maximization Step - Using g to estimate m,u and p.
            // For each j 2^{fieldCount - 1} iterations occur.
            for (int j = 0; j < fieldCount; j++) {
                double a = 0.0;
                double b = 0.0;
                final int[] indexes = SimilarityVectorFrequencies.indexesWithJset(j, fieldCount);
                for(int i : indexes) {
                    a += g[i][0] * frequencies.getVectorFrequency(i)/mSum;
                    b += g[i][1] * frequencies.getVectorFrequency(i)/uSum;
                }
                m[j] = a;
                u[j] = b;
            }
            double pPreviousIteration = p;
            p = mSum/(double) pairCount;

            // if estimation converges break
            if (Math.abs(p  - pPreviousIteration) <= 0.00001) break;
        }
    }

    /**
     * Return current iteration.
     *
     * @return current iteration.
     */
    public int getIteration() {
        return iteration;
    }

    /**
     * Return current m probability for all selected fields.
     *
     * @return current m probability for all selected fields.
     */
    public double[] getM() {
        return m;
    }

    /**
     * Return current u probability for all selected fields.
     *
     * @return current u probability for all selected fields.
     */
    public double[] getU() {
        return u;
    }

    /**
     * Return current p.
     *
     * @return current p.
     */
    public double getP() {
        return p;
    }

    /**
     * Return pair count.
     *
     * @return pair count.
     */
    public long getPairCount() {
        return pairCount;
    }

    /**
     * Return field count.
     *
     * @return field count.
     */
    public int getFieldCount() {
        return fieldCount;
    }

    @Override
    public String toString() {
        StringBuilder msb = new StringBuilder();
        StringBuilder usb = new StringBuilder();
        msb.append("[").append(String.format(", %.7f",m[0]));
        usb.append("[").append(String.format(", %.7f",u[0]));
        assert m.length == u.length;
        for (int i = 1; i < m.length; i++) {
            msb.append(String.format(", %.7f",m[i]));
            usb.append(String.format(", %.7f",u[i]));
        }
        msb.append("]");
        usb.append("]");
        return "ExpectationMaximization{" +
                "pairCount=" + pairCount +
                ", fieldCount=" + fieldCount +
                ", iteration=" + iteration +
                ", m=" + msb.toString()  +
                ", u=" + usb.toString() +
                ", p=" + String.format("%.7f",p) +
                '}';
    }
}
