package gr.upatras.ceid.pprl.encoding;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MultiBloomFilterEncoding extends BaseBloomFilterEncoding {

    private Map<String,Schema.Field> nameToFieldMap;

    public MultiBloomFilterEncoding(Schema schema, String uidColumnName, List<String> selectedColumnNames, int N, int K, int Q)
            throws BloomFilterEncodingException {
        super(schema, uidColumnName, selectedColumnNames, N, K, Q);
    }

    public MultiBloomFilterEncoding(Schema schema, Schema encodingSchema, String uidColumnName, List<String> selectedColumnNames,
                                    int n, int k, int q) throws BloomFilterEncodingException {
        super(schema, encodingSchema, uidColumnName, selectedColumnNames, n, k, q);
        initMap();
    }

    protected String getName() {
        return "_MULTI" + super.getName();
    }

    public void initMap() {
        nameToFieldMap = new HashMap<String,Schema.Field>();
        for(String s : selectedColumnNames)
            for(Schema.Field f : encodingColumns)
                if(f.name().contains(s)) { nameToFieldMap.put(s,f); break;}
    }

    public Schema.Field getEncodingColumnForName(String name) throws BloomFilterEncodingException {
        if(nameToFieldMap == null) throw new BloomFilterEncodingException("Map not initialized.");
        Schema.Field f = nameToFieldMap.get(name);
        if(f == null) throw new BloomFilterEncodingException("Map not initialized properly!.");
        return f;
    }

    @Override
    public void generateEncodingColumnNames() throws BloomFilterEncodingException {
        super.generateEncodingColumnNames();
        encodingColumnNames = new ArrayList<String>();
        final StringBuilder sb = new StringBuilder("enc");
        sb.append(getName());
        for(String column : selectedColumnNames) {
            final StringBuilder nsb = new StringBuilder(sb.toString());
            nsb.append(column).append("_");
            nsb.deleteCharAt(nsb.lastIndexOf("_"));
            encodingColumnNames.add(nsb.toString());
        }
    }

    public GenericData.Fixed encode(Object obj, Class<?> clz, Schema encodingFieldSchema) {
        byte[] one = new byte[(int) Math.ceil(N / 8)];
        one[0] = (byte) 1;
        return new GenericData.Fixed(encodingFieldSchema,one);
    }

    public GenericData.Fixed encode(List<Object> objs, List<Class<?>> clzz, Schema encodingFieldSchema)
        throws UnsupportedOperationException {
        throw new UnsupportedOperationException("Not supported for " + getClass().getSimpleName());
    }
}
