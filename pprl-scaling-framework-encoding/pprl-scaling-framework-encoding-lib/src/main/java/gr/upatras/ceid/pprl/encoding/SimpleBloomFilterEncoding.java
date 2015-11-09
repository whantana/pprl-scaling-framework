package gr.upatras.ceid.pprl.encoding;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

import java.util.Collections;
import java.util.List;


public class SimpleBloomFilterEncoding extends BaseBloomFilterEncoding {

    public SimpleBloomFilterEncoding(Schema schema, String uidColumnName, List<String> selectedColumnNames,
                                     int N, int K, int Q) throws BloomFilterEncodingException {
        super(schema, uidColumnName, selectedColumnNames, N, K, Q);
    }

    public SimpleBloomFilterEncoding(Schema schema, Schema encodingSchema, String uidColumnName, List<String> selectedColumnNames,
                                     int n, int k, int q) throws BloomFilterEncodingException {
        super(schema, encodingSchema, uidColumnName, selectedColumnNames, n, k, q);
    }

    protected String getName() {
        return "_SIMPLE" + super.getName();
    }

    @Override
    public void generateEncodingColumnNames() throws BloomFilterEncodingException {
        super.generateEncodingColumnNames();
        StringBuilder sb = new StringBuilder("enc");
        sb.append(getName());
        for(String column : selectedColumnNames) sb.append(column).append("_");
        sb.deleteCharAt(sb.lastIndexOf("_"));
        encodingColumnNames = Collections.singletonList(sb.toString());
    }

    public String getEncodingColumnName() { return encodingColumnNames.get(0);}

    public Schema.Field getEncodingColumn() {
        return encodingColumns.get(0);
    }

    public GenericData.Fixed encode(Object obj, Class<?> clz, Schema encodingFieldSchema)
            throws UnsupportedOperationException {
        throw new UnsupportedOperationException("Not supported for " + getClass().getSimpleName());
    }

    public GenericData.Fixed encode(List<Object> objs, List<Class<?>> clzz, Schema encodingFieldSchema) {
//        byte[] randomBytes = new byte[(int) Math.ceil(N / 8)];
//        (new Random()).nextBytes(randomBytes);
//        return new GenericData.Fixed(encodingFieldSchema,randomBytes);
        byte[] one = new byte[(int) Math.ceil(N / 8)];
        one[0] = (byte) 1;
        return new GenericData.Fixed(encodingFieldSchema,one);
    }
}
