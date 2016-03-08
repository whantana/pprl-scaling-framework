package gr.upatras.ceid.pprl.encoding;


import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BloomFilterEncodingUtil {

    private static final Logger LOG = LoggerFactory.getLogger(BloomFilterEncodingUtil.class);

    public static final List<String> SCHEME_NAMES = new ArrayList<String>();
    public static final Map<String,Class<?>> SCHEMES = new HashMap<String, Class<?>>();
    static {
        SCHEMES.put("FBF", FieldBloomFilterEncoding.class);
        SCHEMES.put("RBF", RowBloomFilterEncoding.class);
        SCHEMES.put("CLK", CLKEncoding.class);
        SCHEME_NAMES.addAll(SCHEMES.keySet());
    }

    public static final List<Schema.Type> SUPPORTED_TYPES = new ArrayList<Schema.Type>();
    static {
        SUPPORTED_TYPES.add(Schema.Type.INT);
        SUPPORTED_TYPES.add(Schema.Type.LONG);
        SUPPORTED_TYPES.add(Schema.Type.FLOAT);
        SUPPORTED_TYPES.add(Schema.Type.DOUBLE);
        SUPPORTED_TYPES.add(Schema.Type.BOOLEAN);
        SUPPORTED_TYPES.add(Schema.Type.STRING);
    }

    public static void schemeNameSupported(final String scheme)
            throws BloomFilterEncodingException {
        if(!SCHEME_NAMES.contains(scheme))
            throw new BloomFilterEncodingException("String \"" + scheme +"\" does not belong in available schemes.");
    }

    public static BloomFilterEncoding newInstance(final String scheme)
            throws BloomFilterEncodingException {
        schemeNameSupported(scheme);
        try {
            LOG.debug("New Encoding instance is {} .",scheme);
            return (BloomFilterEncoding) (SCHEMES.get(scheme).newInstance());
        } catch (InstantiationException e) {
            throw new BloomFilterEncodingException(e.getMessage());
        } catch (IllegalAccessException e) {
            throw new BloomFilterEncodingException(e.getMessage());
        }
    }

    public static boolean nameBelongsToSchema(final Schema schema, final String[] selectedNames) {
        for(String name : selectedNames) {
            boolean nameFound = false;
            for(Schema.Field field : schema.getFields())
                if(field.name().equals(name)) { nameFound = true ; break; }
            if(!nameFound) return false;
        }
        return true;
    }

    public static BloomFilterEncoding instanceFactory(final String scheme,
                                                      final int fieldCount, final int N,
                                                      final int fbfN, final int K, final int Q,
                                                      final double[] avgQgrams, final double[] weights) {
        final boolean clk = scheme.equals("CLK");
        final boolean fbf = scheme.equals("FBF");
        final boolean rbf = scheme.equals("RBF");

        if(clk && N < 0) throw new IllegalArgumentException("CLK Encoding requires N to be set.");
        if(rbf && (fieldCount < 2)) throw new IllegalArgumentException("RBF Encodings requires at least two fields.");

        final boolean fbfStatic = fbf && (fbfN > 0);
        final boolean fbfDynamic = fbf && (fbfN < 0);
        final boolean rbfUniform =  rbf && (N > 0);
        final boolean rbfWeighted =  rbf && (N < 0);
        final boolean rbfUniformFbfStatic = rbfUniform && (fbfN > 0);
        final boolean rbfUniformFbfDynamic = rbfUniform && (fbfN < 0);
        final boolean rbfWeighetdFbfStatic = rbfWeighted && (fbfN > 0);
        final boolean rbfWeighetdFbfDynamic = rbfWeighted && (fbfN < 0);

        if(clk) {
            LOG.debug("New Encoding instance is CLK.");
            return new CLKEncoding(N,K,Q);
        } else if (fbfStatic) {
            int fbfNs[] = FieldBloomFilterEncoding.staticsizes(fbfN, fieldCount);
            LOG.debug(String.format("New Encoding instance is FBF/Static : [fbfNs=%s,K=%d,Q=%d].",
                    Arrays.toString(fbfNs),K,Q));
            return  new FieldBloomFilterEncoding(fbfNs,K,Q);
        } else if (fbfDynamic) {
            if(containsNan(avgQgrams)) throw new IllegalArgumentException("Avg q grams contains NaN.");
            int fbfNs[] = FieldBloomFilterEncoding.dynamicsizes(avgQgrams, K);
            LOG.debug(String.format("New Encoding instance is FBF/Dynamic : [fbfNs=%s,K=%d,Q=%d].",
                    Arrays.toString(fbfNs),K,Q));
            return  new FieldBloomFilterEncoding(fbfNs,K,Q);
        } else if (rbfUniformFbfStatic) {
            int fbfNs[] = FieldBloomFilterEncoding.staticsizes(fbfN, fieldCount);
            LOG.debug(String.format("New Encoding instance is RBF/Uniform/FBF/Static : [fbfNs=%s,N=%d,K=%d,Q=%d].",
                    Arrays.toString(fbfNs),N,K,Q));
            return  new RowBloomFilterEncoding(fbfNs, N,K,Q);
        } else if (rbfUniformFbfDynamic) {
            if(containsNan(avgQgrams)) throw new IllegalArgumentException("Avg q grams contains NaN.");
            int fbfNs[] = FieldBloomFilterEncoding.dynamicsizes(avgQgrams, K);
            LOG.debug(String.format("New Encoding instance is RBF/Uniform/FBF/Dynamic : [fbfNs=%s,N=%d,K=%d,Q=%d].",
                    Arrays.toString(fbfNs),N,K,Q));
            return  new RowBloomFilterEncoding(fbfNs, N,K,Q);
        } else if (rbfWeighetdFbfStatic) {
            int fbfNs[] = FieldBloomFilterEncoding.staticsizes(fbfN, fieldCount);
            if(containsNan(weights)) throw new IllegalArgumentException("Weights contains NaN.");
            if(!doublesAddUpTo1(weights, 0.0001)) throw new IllegalArgumentException("Weights do not add up to 1.");
            LOG.debug(String.format("New Encoding instance is RBF/Weighted/FBF/Static : [fbfNs=%s,weights=%s,K=%d,Q=%d].",
                    Arrays.toString(fbfNs),Arrays.toString(weights),K,Q));
            return new RowBloomFilterEncoding(fbfNs, weights,K,Q);
        } else if (rbfWeighetdFbfDynamic) {
            if(containsNan(avgQgrams)) throw new IllegalArgumentException("Avg q grams contains NaN.");
            int fbfNs[] = FieldBloomFilterEncoding.dynamicsizes(avgQgrams, K);
            if(containsNan(weights)) throw new IllegalArgumentException("Weights contains NaN.");
            if(!doublesAddUpTo1(weights, 0.0001)) throw new IllegalArgumentException("Weights do not add up to 1.");
            LOG.debug(String.format("New Encoding instance is RBF/Weighted/FBF/Dynamic : [fbfNs=%s,weights=%s,K=%d,Q=%d].",
                    Arrays.toString(fbfNs),Arrays.toString(weights),K,Q));
            return new RowBloomFilterEncoding(fbfNs, weights,K,Q);
        } else throw new IllegalStateException("illegal state.");
    }

    public static boolean containsNan(final double[] doubles) {
        for(double d : doubles) if(Double.isNaN(d)) return true;
        return false;
    }

    public static boolean doublesAddUpTo1(final double[] doubles, double error) {
        double sum = 0;
        for(double d : doubles) sum += d;
        return Math.abs(1.0 - sum) <= error;
    }
}
