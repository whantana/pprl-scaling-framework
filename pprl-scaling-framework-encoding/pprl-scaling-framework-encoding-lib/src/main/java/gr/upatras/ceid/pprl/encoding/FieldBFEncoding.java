package gr.upatras.ceid.pprl.encoding;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class FieldBFEncoding extends BaseBFEncoding {

    private Map<String,Schema.Field> nameToFieldMap;

    public FieldBFEncoding(Schema schema, String uidColumnName, List<String> selectedColumnNames, int N, int K, int Q)
            throws BFEncodingException {
        super(schema, uidColumnName, selectedColumnNames, N, K, Q);
    }

    public FieldBFEncoding(Schema schema, Schema encodingSchema, String uidColumnName, List<String> selectedColumnNames,
                           int n, int k, int q) throws BFEncodingException {
        super(schema, encodingSchema, uidColumnName, selectedColumnNames, n, k, q);
        initMap();
    }

    protected String getName() {
        return "_FBF" + super.getName();
    }

    public void initMap() {
        nameToFieldMap = new HashMap<String,Schema.Field>();
        for(String s : selectedColumnNames)
            for(Schema.Field f : encodingColumns)
                if(f.name().contains(s)) { nameToFieldMap.put(s,f); break;}
    }

    public Schema.Field getEncodingColumnForName(String name) throws BFEncodingException {
        if(nameToFieldMap == null) throw new BFEncodingException("Map not initialized.");
        Schema.Field f = nameToFieldMap.get(name);
        if(f == null) throw new BFEncodingException("Map not initialized properly!.");
        return f;
    }

    @Override
    public void generateEncodingColumnNames() throws BFEncodingException {
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
//        byte[] randomBytes = new byte[(int) Math.ceil(N / 8)];
//        (new Random()).nextBytes(randomBytes);
//        return new GenericData.Fixed(encodingFieldSchema,randomBytes);
        byte[] one = new byte[(int) Math.ceil(N / 8)];
        one[0] = (byte) 1;
        return new GenericData.Fixed(encodingFieldSchema,one);
    }

    public GenericData.Fixed encode(List<Object> objs, List<Class<?>> clzz, Schema encodingFieldSchema)
        throws UnsupportedOperationException {
        throw new UnsupportedOperationException("Not supported for " + getClass().getSimpleName());
    }
}
