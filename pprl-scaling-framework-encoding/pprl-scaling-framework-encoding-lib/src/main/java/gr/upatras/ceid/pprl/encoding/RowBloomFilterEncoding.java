package gr.upatras.ceid.pprl.encoding;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class RowBloomFilterEncoding extends BaseBloomFilterEncoding {

    private Schema.Field encodingColumn;

    public RowBloomFilterEncoding(int N, int K, int Q) {
        super(N, K, Q);
    }

    public RowBloomFilterEncoding(List<String> selectedColumnNames, int N, int K, int Q) {
        super(selectedColumnNames, N, K, Q);
    }

    public RowBloomFilterEncoding(Schema schema, List<String> selectedColumnNames,
                                  int N, int K, int Q) {
        super(schema, selectedColumnNames, N, K, Q);
    }

    public RowBloomFilterEncoding(Schema schema, Schema encodingSchema, List<String> selectedColumnNames,
                                  int N, int K, int Q) {
        super(schema, encodingSchema, selectedColumnNames, N, K, Q);
    }

    public RowBloomFilterEncoding(Schema encodingSchema, int N, int K, int Q) {
        super(encodingSchema,N,K,Q);
    }

    public String getEncodingColumnName() { return getEncodingColumn().name();}

    public Schema.Field getEncodingColumn() {
        return encodingColumn;
    }

    @Override
    public void setupFromEncodingSchema() {
        restEncodingColumns = new ArrayList<Schema.Field>();
        for(Schema.Field field : encodingSchema.getFields()) {
            if(field.name().contains("enc_" + getSmallName().replace("|","_")) &&
               field.schema().getType().equals(Schema.Type.FIXED))
                encodingColumn = field;
            else restEncodingColumns.add(field);
        }
    }

    @Override
    public void createEncodingFields() throws BloomFilterEncodingException {
        if(selectedColumnNames == null) throw new BloomFilterEncodingException("Selected column names not found");
        if(restColumns == null) splitSchemaColumns();

        StringBuilder sb = new StringBuilder("enc");
        sb.append("_");
        sb.append(getSmallName().replace("|","_"));
        for (String column : selectedColumnNames) sb.append("_").append(column);
        String encodingColumnName = sb.toString();

        encodingColumn = new Schema.Field(encodingColumnName, Schema.createFixed(
                encodingColumnName, null, null,
                (int) Math.ceil(N / 8)),
                String.format("Encoding(%s) of column(s) %s", getSmallName(), selectedColumnNames), null);

        restEncodingColumns = new ArrayList<Schema.Field>();
        for(Schema.Field f : restColumns) {
        Schema.Field field = new Schema.Field(f.name(), f.schema(), f.doc(), null);
            restEncodingColumns.add(field);
        }
    }

    @Override
    protected List<Schema.Field> generateEncodingSchemaFields() throws BloomFilterEncodingException {
        if(restColumns == null) splitSchemaColumns();

        List<Schema.Field> finalColumns = new ArrayList<Schema.Field>();
        for(Schema.Field f : restColumns) {
            Schema.Field field = new Schema.Field(f.name(), f.schema(), f.doc(), null);
            finalColumns.add(field);
            restEncodingColumns.add(field);
        }
        finalColumns.add(encodingColumn);
        return finalColumns;
    }

    @Override
    public boolean validateEncodingSchemaFields() throws BloomFilterEncodingException {
        if(restColumns == null) splitSchemaColumns();
        boolean properEncodingColumns = true;
        for(Schema.Field f : encodingSchema.getFields()) {

            if (restColumns.contains(f)) continue;
            if (!encodingColumn.equals(f)) {
                properEncodingColumns = false;
                break;
            }
        }
        return properEncodingColumns;
    }

    @Override
    public String getSmallName() {
        return String.format("ROW|%d|%d|%d", N, K, Q);
    }

    @Override
    public GenericData.Fixed encode(Object obj, Class<?> clz, Schema encodingFieldSchema)
            throws UnsupportedOperationException{
        throw new UnsupportedOperationException("Not supported for " + getClass().getSimpleName());
    }

    @Override
    public GenericData.Fixed encode(List<Object> objs, List<Class<?>> clzz, Schema encodingFieldSchema) {
        byte[] one = new byte[(int) Math.ceil(N / 8)];
        one[0] = (byte) 1;
        return new GenericData.Fixed(encodingFieldSchema,one);
    }
}
