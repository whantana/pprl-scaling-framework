package gr.upatras.ceid.pprl.blocking;

import org.apache.commons.math3.distribution.BinomialDistribution;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Blocking Utiltity class.
 */
public class HammingLSHBlockingUtil {

    public static final List<String> SCHEME_NAMES = new ArrayList<String>(); // Available Encoding Schemes
    static {
        SCHEME_NAMES.add("HLSH_FPS");
        SCHEME_NAMES.add("HLSH_FPS_MR_v0");
        SCHEME_NAMES.add("HLSH_FPS_MR_v1");
        SCHEME_NAMES.add("HLSH_FPS_MR_v2");
    }

    /**
     * Does nothing if scheme name is supported, throws exception othewise.
     *
     * @param scheme blocking scheme name.
     */
    public static void schemeNameSupported(final String scheme) {
        if(!SCHEME_NAMES.contains(scheme))
            throw new UnsupportedOperationException("Scheme name \"" + scheme +"\" does not belong in available schemes.");
    }

    public static double probOfBaseHashMatch(final int theta, final int S) {
        return 1.0d - ((double)theta/(double) S);
    }


    public static double probHashMatch(final double ptheta, final int K) {
        return Math.pow(ptheta,K);
    }

    public static int optimalBlockingGroupCount(final double delta, final double pthetaK) {
        return (int) Math.ceil(Math.log(delta)/Math.log(1.0 - pthetaK));
    }

    public static double avgCollisions(final int Lopt, final double pthetaK) {
        return ((double) Lopt)*pthetaK;
    }

    public static double avgCollisions1(final int Lopt, final double pthetaK) {
        BinomialDistribution bd = new BinomialDistribution(Lopt,pthetaK);
        return bd.getNumericalMean();
    }

    public static double stdDevCollisions(final int Lopt, final double pthetaK) {
        return Math.sqrt(avgCollisions(Lopt,pthetaK)*(1.0-pthetaK));
    }

    public static double stdDevCollisions1(final int Lopt, final double pthetaK) {
        BinomialDistribution bd = new BinomialDistribution(Lopt,pthetaK);
        return Math.sqrt(bd.getNumericalVariance());
    }

    public static long frequentPairLimit(final double avg, final double std) {
        return Math.round(avg - std);
    }

    public static short frequentPairLimit(final int Lopt, final double pthetaK) {
        final double avg = avgCollisions(Lopt,pthetaK);
        final double std = stdDevCollisions(Lopt,pthetaK);
        return (short) Math.round(avg - std);
    }

    public static int[] optimalBlockingGroupCountLimits(final double delta, final double pthetaK) {
        final int Lopt = optimalBlockingGroupCount(delta,pthetaK);
        final int C = frequentPairLimit(Lopt,pthetaK);
        final int Lc = (int)
                Math.round(((C-1) - Math.log(delta) +
                        Math.sqrt(Math.log(delta)*Math.log(delta) - 2*(C-1)*Math.log(delta)))/pthetaK);

        return new int[]{Lopt,Lc};
    }

    public static int optimalBlockingGroupCountIter(final double delta, final double pthetaK) {
        final int[] limits = optimalBlockingGroupCountLimits(delta,pthetaK);
        final int Lopt= limits[0];
        final int C = frequentPairLimit(Lopt,pthetaK);
        int L=limits[0];
        for(; L < limits[1]; L++)
            if(cdf(L,pthetaK,C) < delta) break;
        return L;
    }

    public static double cdf(final int Lopt, final double pthetaK, final int C) {
        BinomialDistribution bd = new BinomialDistribution(Lopt,pthetaK);
        return bd.cumulativeProbability(C);
    }
}
