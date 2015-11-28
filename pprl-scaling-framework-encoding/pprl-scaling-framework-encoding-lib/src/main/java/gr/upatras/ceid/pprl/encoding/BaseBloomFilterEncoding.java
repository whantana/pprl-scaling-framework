package gr.upatras.ceid.pprl.encoding;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public abstract class BaseBloomFilterEncoding {

    public static final List<String> AVAILABLE_METHODS = new ArrayList<String>();
    static {
        AVAILABLE_METHODS.add("SIMPLE");
        AVAILABLE_METHODS.add("MULTI");
        AVAILABLE_METHODS.add("ROW");
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

    protected List<String> selectedColumnNames;

    protected Schema schema;
    protected List<Schema.Field> restColumns;
    protected List<Schema.Field> selectedColumns;

    protected Schema encodingSchema;
    protected List<Schema.Field> restEncodingColumns;

    protected int N;
    protected int K;
    protected int Q;

    public BaseBloomFilterEncoding(int N, int K, int Q) {
        this.N = N;
        this.K = K;
        this.Q = Q;
    }

    public BaseBloomFilterEncoding(List<String> selectedColumnNames, int N, int K, int Q) {
        this(N,K,Q);
        this.selectedColumnNames = selectedColumnNames;
    }

    public BaseBloomFilterEncoding(Schema schema, List<String> selectedColumnNames,
                                   int N, int K, int Q) {
        this(selectedColumnNames, N,K,Q);
        this.schema = schema;
    }

    public BaseBloomFilterEncoding(Schema schema, Schema encodingSchema, List<String> selectedColumnNames,
                                   int N, int K, int Q) {
        this(schema,selectedColumnNames, N,K,Q);
        this.encodingSchema = encodingSchema;
    }

    public BaseBloomFilterEncoding(Schema encodingSchema, int N, int K, int Q) {
        this(N,K,Q);
        this.encodingSchema = encodingSchema;
    }



    protected void splitSchemaColumns() throws BloomFilterEncodingException {
        if (schema == null) throw new BloomFilterEncodingException("Schema is not set.");
        if (selectedColumnNames == null) throw new BloomFilterEncodingException("Selected column names not set.");

        restColumns = new ArrayList<Schema.Field>();
        selectedColumns = new ArrayList<Schema.Field>();
        for (Schema.Field f : this.schema.getFields()) {
            if (selectedColumnNames.contains(f.name())) {
                selectedColumns.add(f);
            } else {
                restColumns.add(f);
            }
        }
    }

    public void generateEncodingSchema() throws BloomFilterEncodingException {
        encodingSchema = Schema.createRecord(
                String.format("Encoding_%s_%s", getName().replace("|", "_").toLowerCase() , schema.getName()),
                String.format("PPRL Encoding of schema with name %s", schema.getName()),
                ("encoding." + getName().replace("|", ".").toLowerCase() + "." +schema.getNamespace()),
                false);

        final List<Schema.Field> fields = generateEncodingSchemaFields();
        encodingSchema.setFields(fields);
    }

    public String getName() {
        StringBuilder nsb = new StringBuilder(getSmallName());
        if(selectedColumnNames != null)
            for (String columnName : selectedColumnNames) nsb.append("|").append(columnName);
        return nsb.toString();
    }

    public boolean validateEncodingSchema() throws BloomFilterEncodingException{
        boolean properName = encodingSchema.getName().equals(
                String.format("Encoding_%s_%s", getName().replace("|", "_").toLowerCase() , schema.getName()));
        boolean properdoc = encodingSchema.getDoc().equals(
                String.format("PPRL Encoding of schema with name %s", schema.getName()));
        boolean properNamespace = encodingSchema.getNamespace().equals(
                "encoding." + getName().replace("|", ".").toLowerCase() + "." +schema.getNamespace());

        boolean properEncodingColumns = validateEncodingSchemaFields();

        return properEncodingColumns & properName & properdoc & properNamespace;
    }

    public abstract void setupFromEncodingSchema();

    public abstract void createEncodingFields() throws BloomFilterEncodingException;

    protected abstract List<Schema.Field> generateEncodingSchemaFields() throws BloomFilterEncodingException;

    public abstract boolean validateEncodingSchemaFields() throws BloomFilterEncodingException;

    public abstract String getSmallName();

    public abstract GenericData.Fixed encode(Object obj, Schema.Type type, Schema encodingFieldSchema);

    public abstract GenericData.Fixed encode(Object[] objs, Schema.Type[] types, Schema encodingFieldSchema) throws UnsupportedEncodingException;

    public Schema getSchema() {
        return schema;
    }

    public Schema getEncodingSchema() {
        return encodingSchema;
    }

    public List<String> getSelectedColumnNames() throws BloomFilterEncodingException {
        if(selectedColumnNames == null)
            throw new BloomFilterEncodingException("selectedColumnNames is null.");
        return selectedColumnNames;
    }

    public int getN() {
        return N;
    }

    public int getK() {
        return K;
    }

    public int getQ() { return Q; }

    public List<Schema.Field> getRestColumns() throws BloomFilterEncodingException {
        if(restColumns == null)
            throw new BloomFilterEncodingException("restColumns is null.");
        return restColumns;
    }

    public List<Schema.Field> getSelectedColumns() throws BloomFilterEncodingException {
        if(selectedColumns == null)
            throw new BloomFilterEncodingException("selectedColumns is null.");
        return selectedColumns;
    }

    public List<Schema.Field> getRestEncodingColumns() throws BloomFilterEncodingException {
        if(restEncodingColumns == null)
            throw new BloomFilterEncodingException("restEncodingColumns is null.");
        return restEncodingColumns;
    }

    public void setSchema(Schema schema) { this.schema = schema; }

    public void setEncodingSchema(Schema encodingSchema) {
        this.encodingSchema = encodingSchema;
    }

    public void setSelectedColumnNames(List<String> selectedColumnNames) { this.selectedColumnNames = selectedColumnNames; }

    public static String toString(final BaseBloomFilterEncoding encoding) {
        return encoding.getName();
    }

    public static BaseBloomFilterEncoding fromString(final String s) {
        String[] parts = s.split("|");
        final String method = parts[0];
        if (!BaseBloomFilterEncoding.AVAILABLE_METHODS.contains(method)) return null;

        final int N = Integer.parseInt(parts[1]);
        final int K = Integer.parseInt(parts[2]);
        final int Q = Integer.parseInt(parts[3]);
        if (parts.length == 4) {
            if (method.equals("SIMPLE")) return new SimpleBloomFilterEncoding(N, K, Q);
            else if (method.equals("ROW")) return new RowBloomFilterEncoding(N, K, Q);
            else return new MultiBloomFilterEncoding(N, K, Q);
        }
        final List<String> selectedColumnNames = new ArrayList<String>();
        selectedColumnNames.addAll(Arrays.asList(parts).subList(4, parts.length));
        if (method.equals("SIMPLE")) return new SimpleBloomFilterEncoding(selectedColumnNames, N, K, Q);
        else if (method.equals("ROW")) return new RowBloomFilterEncoding(selectedColumnNames, N, K, Q);
        else return new MultiBloomFilterEncoding(selectedColumnNames, N, K, Q);
    }
}
