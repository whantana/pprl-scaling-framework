package gr.upatras.ceid.pprl.encoding;

import org.apache.avro.Schema;

public class RowBFEncoding extends SimpleBFEncoding {

    public RowBFEncoding(String[] columns, int n, int k, int q) {
        super(columns, n, k, q);
    }

    public RowBFEncoding(Schema schema, String[] columns, int n, int k, int q) {
        super(schema, columns, n, k, q);
    }

    public RowBFEncoding(Schema schema, Schema encodingSchema,
                         String[] columns, int n, int k, int q) {
        super(schema, encodingSchema, columns, n, k, q);
    }

    public String getName() {
        return String.format("_RBF_%d_%d_%d_",N,K,Q);
    }
}
