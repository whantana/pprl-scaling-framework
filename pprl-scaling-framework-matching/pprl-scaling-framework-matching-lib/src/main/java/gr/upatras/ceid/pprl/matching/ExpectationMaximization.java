package gr.upatras.ceid.pprl.matching;

import java.util.Arrays;

public class ExpectationMaximization {
    public static final int MAX_ITERATIONS = 1000;
    private int pairCount;
    private int fieldCount;
    private int iteration;
    private double[] m;
    private double[] u;
    private double p;
    private double[][] g;

    public ExpectationMaximization(int fieldCount, double m0, double u0, double p0) {
        this.fieldCount = fieldCount;
        this.m = new double[fieldCount];
        Arrays.fill(this.m,m0);
        this.u = new double[fieldCount];
        Arrays.fill(this.u,u0);
        this.p = p0;
    }

    public ExpectationMaximization(int fieldCount, double[] m0, double[] u0, double p0) {
        assert fieldCount == m0.length;
        assert m0.length == u0.length;
        this.fieldCount = fieldCount;
        this.m = m0;
        this.u = u0;
        this.p  = p0;
    }

    public ExpectationMaximization(String[] fieldNames, double m0, double u0, double p0) {
        this(fieldNames.length,m0,u0,p0);
    }

    public ExpectationMaximization(String[] fieldNames, double[] m0, double[] u0, double p0) {
        this(fieldNames.length,m0,u0,p0);
    }

    public void runAlgorithm(final SimilarityMatrix matrix) {
        pairCount = 0;
        for(iteration = 1; iteration <= MAX_ITERATIONS; iteration++ ) {

            // Expectation Step - calculate g from matrix. Size of g is 2^fieldCount
            g = new double[1 << fieldCount][2];
            double mSum = 0;
            double uSum = 0;
            for (int i = 0; i < g.length; i++) {
                double a = p;
                double b = 1 - p;
                boolean[] row = SimilarityMatrix.index2Vector(i,fieldCount);
                for(int j = 0; j < fieldCount ; j++) {
                    a *= row[j] ? m[j] : (1 - m[j]);
                    b *= row[j] ? u[j] : (1 - u[j]);
                }
                g[i][0] = a/(a+b);
                g[i][1] = b/(a+b);
                mSum += g[i][0]*matrix.getVectorCounts()[i];
                uSum += g[i][1]*matrix.getVectorCounts()[i];
                if(iteration == 1)pairCount += matrix.getVectorCounts()[i];
            }

            // Maximization Step - Using g to estimate m,u and p.
            // For each j 2^fieldCount - 1 iterations occur.
            for (int j = 0; j < fieldCount; j++) {
                double a = 0.0;
                double b = 0.0;
                final int[] indexes = SimilarityMatrix.indexesWithJset(j,fieldCount);
                for(int i : indexes) {
                    a += g[i][0]*matrix.getVectorCounts()[i]/mSum;
                    b += g[i][1]*matrix.getVectorCounts()[i]/uSum;
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

    public int getIteration() {
        return iteration;
    }

    public double[] getM() {
        return m;
    }

    public double[] getU() {
        return u;
    }

    public double getP() {
        return p;
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
