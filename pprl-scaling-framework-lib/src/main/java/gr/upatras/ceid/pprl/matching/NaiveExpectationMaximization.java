package gr.upatras.ceid.pprl.matching;

import java.util.Arrays;

/**
 * Naive Implementation of Expectation Maximization class.
 */
public class NaiveExpectationMaximization {

    public static final int MAX_ITERATIONS = 1000;  // Maximum iterations
    private int pairCount;                         // Count of pairs
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
    public NaiveExpectationMaximization(int fieldCount, double m0, double u0, double p0) {
        this.fieldCount = fieldCount;
        this.m = new double[fieldCount];
        Arrays.fill(this.m,m0);
        this.u = new double[fieldCount];
        Arrays.fill(this.u, u0);
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
    public NaiveExpectationMaximization(int fieldCount, double[] m0, double[] u0, double p0) {
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
    public NaiveExpectationMaximization(String[] fieldNames, double m0, double u0, double p0) {
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
    public NaiveExpectationMaximization(String[] fieldNames, double[] m0, double[] u0, double p0) {
        this(fieldNames.length,m0,u0,p0);
    }

    /**
     * Run algorithm
     *
     * @param matrix similarity matrix
     * @throws java.io.IOException
     */
    public void runAlgorithm(final SimilarityMatrix matrix) throws java.io.IOException {
        pairCount = matrix.getPairCount();
        for(iteration = 1; iteration <= MAX_ITERATIONS; iteration++ ) {
            g = runExpecationStep(matrix);
			double[] previousM = Arrays.copyOf(m,m.length);
			double[] previousU = Arrays.copyOf(u,u.length);
			double previousP = p;
			runMaximizationStep(matrix, g);
            if(converges(previousM,previousU,previousP)) break;
        }
    }

    /**
     * Returns current iteration.
     *
     * @return current iteration.
     */
    public int getIteration() {
        return iteration;
    }

    /**
     * Expecation step method.
     *
     * @param matrix similarity matrix
     * @return vectors g[*][2].
     */
    private double[][] runExpecationStep(final SimilarityMatrix matrix) {
        double[][] g = new double[pairCount][2];
        for(int i = 0; i < pairCount ; i++) {
            double a = p;
            double b = (1 - p);
            for(int j = 0; j < fieldCount ; j++) {
                a *= matrix.get(i,j) ? m[j] : (1 - m[j]);
                b *= matrix.get(i,j) ? u[j] : (1 - u[j]);
            }
            g[i][0] = a/(a+b);
            g[i][1] = b/(a+b);
        }
        return g;
    }

    /**
     * Maximization step method.
     *
     * @param matrix similarity matrix
     * @param g vectors g[*][2].
     */
    private void runMaximizationStep(final SimilarityMatrix matrix, double[][] g) {
        double mSum = 0.0;
        double uSum = 0.0;
        for(int j=0; j < fieldCount; j++) {
            double a = 0.0;
            double b = 0.0;
            for(int i=0;i< pairCount ;i++) {
                a += matrix.get(i,j) ? g[i][0] : 0;
                b += matrix.get(i,j) ? g[i][1] : 0;
                if(j == 0) {
                    mSum += g[i][0];
                    uSum += g[i][1];
                }
            }
            m[j] = a/mSum;
            u[j] = b/uSum;
        }
        p = mSum/(double) pairCount;
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
    public int getPairCount() {
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

    /**
     * Return true if probabilities m,u,p converge, false otherwise.
     *
     * @param previousM previous iteration m values.
     * @param previousU previous iteration u values.
     * @param previousU previous iteration p value.
     * @return true if probabilities m,u,p converge, false otherwise.
     */
	private boolean converges(double[] previousM,double[] previousU, double previousP) {
		 boolean converges = (Math.abs(p - previousP) <= 0.00001);
		 if(!converges) return false;
		for(int i = 0 ; i < previousM.length; i++)
			converges &= (Math.abs(m[i] - previousM[i]) <= 0.00001);
		if(!converges) return false;
		for(int i = 0 ; i < previousU.length; i++)
			converges &= (Math.abs(u[i] - previousU[i]) <= 0.00001);
		return converges;
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
        return "NaiveExpectationMaximization{" +
                "pairCount=" + pairCount +
                ", fieldCount=" + fieldCount +
                ", iteration=" + iteration +
                ", m=" + msb.toString()  +
                ", u=" + usb.toString() +
                ", p=" + String.format("%.7f",p) +
                '}';
    }
}
