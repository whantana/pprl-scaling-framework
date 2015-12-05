package gr.upatras.ceid.pprl.encoding;

import org.apache.avro.Schema;

public class FieldBloomFilterEncoding extends BloomFilterEncoding {

    public FieldBloomFilterEncoding() {}

    public FieldBloomFilterEncoding(double[] avgQgram, int K, int Q) {
        super(avgQgram, K, Q);
    }

    public FieldBloomFilterEncoding(int N, int K, int Q) {
        super(N, K, Q);
    }

    public String getName() {
        return "FBF_" + super.getName();
    }

    public static FieldBloomFilterEncoding makeFromSchema(final Schema schema,
                                      final String[] selectedFieldNames,
                                      final double[] avgQgrams, final int K, final int Q)

            throws BloomFilterEncodingException {
        FieldBloomFilterEncoding encoding = new FieldBloomFilterEncoding(avgQgrams,K,Q);
        encoding.makeFromSchema(schema, selectedFieldNames);
        return encoding;
    }

    public static FieldBloomFilterEncoding makeFromSchema(final Schema schema,
                                                          final String[] selectedFieldNames,
                                                          final int N, final int K, final int Q)
            throws BloomFilterEncodingException {
        FieldBloomFilterEncoding encoding = new FieldBloomFilterEncoding(N,K,Q);
        encoding.makeFromSchema(schema,selectedFieldNames);
        return encoding;
    }

    public static FieldBloomFilterEncoding makeFromSchema(final Schema schema,
                                      final String[] selectedFieldNames,
                                      final String[] restFieldNames,
                                      final double[] avgQgrams, final int K, final int Q)

            throws BloomFilterEncodingException {
        FieldBloomFilterEncoding encoding = new FieldBloomFilterEncoding(avgQgrams,K,Q);
        encoding.makeFromSchema(schema, selectedFieldNames,restFieldNames);
        return encoding;
    }

    public static FieldBloomFilterEncoding makeFromSchema(final Schema schema,
                                                          final String[] selectedFieldNames,
                                                          final String[] restFieldNames,
                                                          final int N, final int K, final int Q)
            throws BloomFilterEncodingException {
        FieldBloomFilterEncoding encoding = new FieldBloomFilterEncoding(N,K,Q);
        encoding.makeFromSchema(schema,selectedFieldNames,restFieldNames);
        return encoding;
    }
}
