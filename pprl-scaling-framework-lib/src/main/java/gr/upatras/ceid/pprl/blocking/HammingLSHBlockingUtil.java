package gr.upatras.ceid.pprl.blocking;

import org.apache.commons.math3.distribution.BinomialDistribution;

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
        SCHEME_NAMES.add("HLSH_FPS_MR_v3");
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


    /**
     * Returns the optimal HLSH/FPS parameters for given.
     * @param theta hamming theshold.
     * @param S Bloom-Filter size.
     * @param delta confidence factor.
     * @param K base hash count.
     * @return a 4-int array, optimal C , L and limits on L.
     */
    public static int[] optimalParameters(final int theta, final int S, final double delta, final int K) {
        final double ptheta = probOfBaseHashMatch(theta,S);
        final double pthetaK = probHashMatch(ptheta,K);
        return optimalFPSParameters(delta, pthetaK);
    }

    /**
     * Probability of matching base HLSH hash function
     *
     * @param theta a hamming threshold
     * @param S size of Bloom-Filter
     * @return
     */
    public static double probOfBaseHashMatch(final int theta, final int S) {
        return 1.0d - ((double)theta/(double) S);
    }


    /**
     * Probability of matching base HLSH hash function
     *
     * @param ptheta a base hash probability
     * @param K number of base hash functions
     * @return Probability of matching base HLSH hash function
     */
    public static double probHashMatch(final double ptheta, final int K) {
        return Math.pow(ptheta,K);
    }

    /**
     * Optimal hlsh blocking group count.
     *
     * @param delta confidence factor
     * @param pthetaK probability of collision within a certain threhold.
     * @return  Optimal hlsh blocking group count.
     */
    private static int optimalBlockingGroupCount(final double delta, final double pthetaK) {
        return (int) Math.ceil(Math.log(delta)/Math.log(1.0 - pthetaK));
    }

    /**
     * Average collisions of a biniomial distribution (random collision in HLSL).
     * @param Lopt trials count
     * @param pthetaK probability of success
     * @return Average collisions
     */
    private static double avgCollisions(final int Lopt, final double pthetaK) {
        return ((double) Lopt)*pthetaK;
    }

    /**
     * Std Deviation of of a biniomial distribution (random collision in HLSL).
     * @param Lopt trials count
     * @param pthetaK probability of success
     * @return
     */
    private static double stdDevCollisions(final int Lopt, final double pthetaK) {
        return Math.sqrt(avgCollisions(Lopt,pthetaK)*(1.0-pthetaK));
    }

    /**
     * Estimate the frequent pair limit.
     *
     * @param Lopt HLSH optimal blocking group count.
     * @param pthetaK probability of collision within a certain threhold.
     * @return  frequent pair limit
     */
    private static short frequentPairLimit(final int Lopt, final double pthetaK) {
        final double avg = avgCollisions(Lopt,pthetaK);
        final double std = stdDevCollisions(Lopt,pthetaK);
        return (short) Math.round(avg - std);
    }

    private static int[] optimalBlockingGroupCountLimits(final double delta, final double pthetaK) {
        final int Lopt = optimalBlockingGroupCount(delta,pthetaK);
        final int C = frequentPairLimit(Lopt,pthetaK);
        final int Lc = (int)
                Math.round(((C-1) - Math.log(delta) +
                        Math.sqrt(Math.log(delta)*Math.log(delta) - 2*(C-1)*Math.log(delta)))/pthetaK);

        return new int[]{Lopt,Lc};
    }

    /**
     * Estimate the HLSH/FPS optimal parameters.
     *
     * @param delta confidence factor.
     * @param pthetaK probability of collision within a certain threhold.
     * @return optimal collision limit C and blocking group count L
     */
    private static int[] optimalFPSParameters(final double delta, final double pthetaK) {
        final int[] limits = optimalBlockingGroupCountLimits(delta,pthetaK);
        final int Lopt= limits[0];
        final int C = frequentPairLimit(Lopt,pthetaK);
        int L=limits[0];
        for(; L < limits[1]; L++)
            if(cdf(L,pthetaK,C) < delta) break;
        return new int[]{C,L,limits[0],limits[1]};
    }

    /**
     * Return cdf from binomial distribution.
     * @param Lopt trials count
     * @param pthetaK probability of success
     * @param C limit
     * @return CDF
     */
    private static double cdf(final int Lopt, final double pthetaK, final int C) {
        BinomialDistribution bd = new BinomialDistribution(Lopt,pthetaK);
        return bd.cumulativeProbability(C);
    }
}
