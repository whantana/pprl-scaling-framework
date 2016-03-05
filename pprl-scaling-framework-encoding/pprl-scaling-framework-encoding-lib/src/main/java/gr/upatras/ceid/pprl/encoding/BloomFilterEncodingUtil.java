package gr.upatras.ceid.pprl.encoding;


import org.apache.avro.Schema;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BloomFilterEncodingUtil {

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
            return (BloomFilterEncoding) (SCHEMES.get(scheme).newInstance());
        } catch (InstantiationException e) {
            throw new BloomFilterEncodingException(e.getMessage());
        } catch (IllegalAccessException e) {
            throw new BloomFilterEncodingException(e.getMessage());
        }
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
            throw new UnsupportedOperationException("Not supported.");
        } else if (fbfStatic) {
            int fbfNs[] = FieldBloomFilterEncoding.staticsizes(fbfN, fieldCount);
            return  new FieldBloomFilterEncoding(fbfNs,K,Q);
        } else if (fbfDynamic) {
            int fbfNs[] = FieldBloomFilterEncoding.dynamicsizes(avgQgrams, K);
            return  new FieldBloomFilterEncoding(fbfNs,K,Q);
        } else if (rbfUniformFbfStatic) {
            int fbfNs[] = FieldBloomFilterEncoding.staticsizes(fbfN, fieldCount);
            return  new RowBloomFilterEncoding(fbfNs, N,K,Q);
        } else if (rbfUniformFbfDynamic) {
            int fbfNs[] = FieldBloomFilterEncoding.dynamicsizes(avgQgrams, K);
            return  new RowBloomFilterEncoding(fbfNs, N,K,Q);
        } else if (rbfWeighetdFbfStatic) {
            int fbfNs[] = FieldBloomFilterEncoding.staticsizes(fbfN, fieldCount);
            return  new RowBloomFilterEncoding(fbfNs, weights,K,Q);
        } else if (rbfWeighetdFbfDynamic) {
            int fbfNs[] = FieldBloomFilterEncoding.dynamicsizes(avgQgrams, K);
            return new RowBloomFilterEncoding(fbfNs, weights,K,Q);
        } else throw new IllegalStateException("illegal state.");
    }
}
