package gr.upatras.ceid.pprl.encoding;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MultiBloomFilterEncoding extends BaseBloomFilterEncoding {

    protected Map<String,Schema.Field> encodingNamesToColumnsMap;

    public MultiBloomFilterEncoding(int N, int K, int Q) {
        super(N, K, Q);
    }

    public MultiBloomFilterEncoding(List<String> selectedColumnNames, int N, int K, int Q) {
        super(selectedColumnNames, N, K, Q);
    }

    public MultiBloomFilterEncoding(Schema schema, List<String> selectedColumnNames, int N, int K, int Q) {
        super(schema, selectedColumnNames, N, K, Q);
    }

    public MultiBloomFilterEncoding(Schema schema, Schema encodingSchema, List<String> selectedColumnNames,
                                    int N, int K, int Q) {
        super(schema, encodingSchema, selectedColumnNames, N, K, Q);
    }

    public MultiBloomFilterEncoding(Schema encodingSchema, int N, int K, int Q) {
        super(encodingSchema,N,K,Q);
    }

    public List<String> getEncodingColumnNames() {
        return Arrays.asList((String[])encodingNamesToColumnsMap.keySet().toArray());
    }

    public Schema.Field getEncodingColumnForName(final String name) {
        final StringBuilder sb = new StringBuilder("enc");
        sb.append("_");
        sb.append(getSmallName().replace("|","_"));
        sb.append("_").append(name);
        return encodingNamesToColumnsMap.get(sb.toString());
    }

    @Override
    public void setupFromEncodingSchema() {
        encodingNamesToColumnsMap = new HashMap<String,Schema.Field>();
        restEncodingColumns = new ArrayList<Schema.Field>();
        for(Schema.Field field : encodingSchema.getFields()) {
            if(field.name().contains("enc_" + getSmallName().replace("|","_")) &&
               field.schema().getType().equals(Schema.Type.FIXED))
                encodingNamesToColumnsMap.put(field.name(),field);
            else restEncodingColumns.add(field);
        }
    }

    @Override
    public String getSmallName() { return String.format("MULTI|%d|%d|%d", N, K, Q); }

    @Override
    public void createEncodingFields() throws BloomFilterEncodingException {
        if(encodingSchema != null) {
            setupFromEncodingSchema();
            return;
        }

        if(selectedColumnNames == null) throw new BloomFilterEncodingException("Selected column names not found");
        encodingNamesToColumnsMap = new HashMap<String,Schema.Field>();
        if(restColumns == null) splitSchemaColumns();

        final StringBuilder sb = new StringBuilder("enc");
        sb.append("_");
        sb.append(getSmallName().replace("|","_"));
        for (String column : selectedColumnNames) {
            final StringBuilder nsb = new StringBuilder(sb.toString());
            nsb.append("_").append(column);
            String encodingColumnName = nsb.toString();
            Schema.Field encodingColumn = new Schema.Field(encodingColumnName, Schema.createFixed(
                    encodingColumnName, null, null,
                    (int) Math.ceil(N / 8)),  // TODO Remember dynamic sizing
                    String.format("Encoding(%s) of column %s", getName(), column), null);

            encodingNamesToColumnsMap.put(encodingColumnName, encodingColumn);
        }

        restEncodingColumns = new ArrayList<Schema.Field>();
        for(Schema.Field f : restColumns) {
        Schema.Field field = new Schema.Field(f.name(), f.schema(), f.doc(), null);
            restEncodingColumns.add(field);
        }
    }

    @Override
    protected List<Schema.Field> generateEncodingSchemaFields() throws BloomFilterEncodingException {
        if(restColumns == null) splitSchemaColumns();
        if(restEncodingColumns == null) restEncodingColumns = new ArrayList<Schema.Field>();

        List<Schema.Field> finalColumns = new ArrayList<Schema.Field>();
        for(Schema.Field f : restColumns) {
            Schema.Field field = new Schema.Field(f.name(), f.schema(), f.doc(), null);
            finalColumns.add(field);
            restEncodingColumns.add(field);
        }
        finalColumns.addAll(encodingNamesToColumnsMap.values());
        return finalColumns;
    }

    @Override
    public boolean validateEncodingSchemaFields() throws BloomFilterEncodingException {
        if(restColumns == null) splitSchemaColumns();
        boolean properEncodingColumns = true;
        for(Schema.Field f : encodingSchema.getFields()) {

            if (restColumns.contains(f)) continue;
            if (!encodingNamesToColumnsMap.values().contains(f)) {
                properEncodingColumns = false;
                break;
            }
        }
        return properEncodingColumns;
    }

    @Override
    public GenericData.Fixed encode(Object obj, Class<?> clz, Schema encodingFieldSchema) {
        byte[] one = new byte[(int) Math.ceil(N / 8)];
        one[0] = (byte) 1;
        return new GenericData.Fixed(encodingFieldSchema,one);
    }

    @Override
    public GenericData.Fixed encode(List<Object> objs, List<Class<?>> clzz, Schema encodingFieldSchema)
            throws UnsupportedOperationException {
        throw new UnsupportedOperationException("Not supported for " + getClass().getSimpleName());
    }
}
