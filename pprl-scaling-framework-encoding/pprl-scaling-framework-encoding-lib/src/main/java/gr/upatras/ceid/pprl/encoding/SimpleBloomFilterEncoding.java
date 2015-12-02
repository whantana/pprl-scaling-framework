package gr.upatras.ceid.pprl.encoding;

import gr.upatras.ceid.pprl.datasets.QGramUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.util.Utf8;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;


public class SimpleBloomFilterEncoding extends BaseBloomFilterEncoding {

    private Schema.Field encodingColumn;

    private BloomFilter bloomFilter;

    public SimpleBloomFilterEncoding(int N, int K, int Q) {
        super(N, K, Q);
        bloomFilter = new BloomFilter(N,K);
    }

    public SimpleBloomFilterEncoding(List<String> selectedColumnNames, int N, int K, int Q) {
        super(selectedColumnNames, N, K, Q);
        bloomFilter = new BloomFilter(N,K);
    }

    public SimpleBloomFilterEncoding(Schema schema, List<String> selectedColumnNames,
                                     int N, int K, int Q) {
        super(schema,selectedColumnNames, N, K, Q);
        bloomFilter = new BloomFilter(N,K);
    }

    public SimpleBloomFilterEncoding(Schema schema, Schema encodingSchema, List<String> selectedColumnNames,
                                     int N, int K, int Q) {
        super(schema, encodingSchema, selectedColumnNames, N, K, Q);
        bloomFilter = new BloomFilter(N,K);
    }

    public SimpleBloomFilterEncoding(Schema encodingSchema, int N, int K, int Q) {
        super(encodingSchema,N,K,Q);
    }

    public String getEncodingColumnName() { return getEncodingColumn().name();}

    public Schema.Field getEncodingColumn() {
        return encodingColumn;
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
                (int) Math.ceil(N/(double)8)),
                String.format("Encoding(%s) of column(s) %s", getSmallName(), selectedColumnNames), null);

        restEncodingColumns = new ArrayList<Schema.Field>();
        for(Schema.Field f : restColumns) {
        Schema.Field field = new Schema.Field(f.name(), f.schema(), f.doc(), null);
            restEncodingColumns.add(field);
        }
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
    public String getSmallName() {
        return String.format("SIMPLE|%d|%d|%d", N, K, Q);
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
    public GenericData.Fixed encode(Object obj, Schema.Type type, Schema encodingFieldSchema)
            throws UnsupportedOperationException {
        throw new UnsupportedOperationException("Not supported for " + getClass().getSimpleName());
    }
    @Override
    public GenericData.Fixed encode(Object[] objs, Schema.Type[] types, Schema encodingFieldSchema)
            throws UnsupportedEncodingException {
        bloomFilter.clear();
        for (int i = 0; i < objs.length ; i++) {
            Object obj = objs[i];
            Schema.Type type = types[i];
            String[] qGrams = QGramUtil.generateQGrams(obj,type,Q);
            for(String qGram : qGrams) bloomFilter.addData(qGram.getBytes("UTF-8"));
        }
        return new GenericData.Fixed(encodingFieldSchema,bloomFilter.getByteArray());
    }
}
