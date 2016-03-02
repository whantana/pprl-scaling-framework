package gr.upatras.ceid.pprl.matching.test;

import java.util.Arrays;

public class NaiveExpectationMaximization {

    public static final int MAX_ITERATIONS = 1000;
        private int pairCount;
        private int fieldCount;
        private int iteration;
        private double[] m;
        private double[] u;
        private double p;
        private double[][] g;

        public NaiveExpectationMaximization(int fieldCount, double m0, double u0, double p0) {
            this.fieldCount = fieldCount;
            this.m = new double[fieldCount];
            Arrays.fill(this.m,m0);
            this.u = new double[fieldCount];
            Arrays.fill(this.u, u0);
            this.p = p0;
        }

        public NaiveExpectationMaximization(int fieldCount, double[] m0, double[] u0, double p0) {
            assert fieldCount == m0.length;
            assert m0.length == u0.length;
            this.fieldCount = fieldCount;
            this.m = m0;
            this.u = u0;
            this.p  = p0;
        }

        public NaiveExpectationMaximization(String[] fieldNames, double m0, double u0, double p0) {
            this(fieldNames.length,m0,u0,p0);
        }

        public NaiveExpectationMaximization(String[] fieldNames, double[] m0, double[] u0, double p0) {
            this(fieldNames.length,m0,u0,p0);
        }


        public void runAlgorithm(final NaiveSimilarityMatrix matrix) throws java.io.IOException {
            pairCount = matrix.getPairCount();
            for(iteration = 1; iteration <= MAX_ITERATIONS; iteration++ ) {
                g = runExpecationStep(matrix);
                boolean converges = runMaximizationStep(matrix, g);
                if(converges) break;
            }
        }

        public int getIteration() {
            return iteration;
        }

        private double[][] runExpecationStep(final NaiveSimilarityMatrix matrix) {
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

        private boolean runMaximizationStep(final NaiveSimilarityMatrix matrix, double[][] g) {
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
            double pPreviousIteration = p;
            p = mSum/(double) pairCount;
            return (Math.abs(p  - pPreviousIteration) <= 0.00001);
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

        public int getPairCount() {
            return pairCount;
        }

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
