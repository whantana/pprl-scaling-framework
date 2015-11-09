package gr.upatras.ceid.pprl.encoding;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

import java.util.ArrayList;
import java.util.List;

public abstract class BaseBloomFilterEncoding {

    protected String uidColumnName;
    protected List<String> selectedColumnNames;
    protected List<String> encodingColumnNames;

    protected Schema schema;
    protected List<Schema.Field> restColumns;
    protected List<Schema.Field> selectedColumns;

    protected Schema encodingSchema;
    protected List<Schema.Field> encodingColumns;

    protected int N;
    protected int K;
    protected int Q;

    public BaseBloomFilterEncoding(Schema schema, String uidColumnName, List<String> selectedColumnNames,
                                   int N, int K, int Q) throws BloomFilterEncodingException {
        this.schema = schema;
        this.uidColumnName = uidColumnName;
        this.selectedColumnNames = selectedColumnNames;
        if(this.selectedColumnNames.contains(this.uidColumnName))
            throw new BloomFilterEncodingException("uid column cannot be selected for encoding.");

        this.N = N;
        this.K = K;
        this.Q = Q;

        restColumns = new ArrayList<Schema.Field>();
        selectedColumns = new ArrayList<Schema.Field>();
        for (Schema.Field f : this.schema.getFields()) {
            if (selectedColumnNames.contains(f.name())) {
                selectedColumns.add(f);
            } else {
                restColumns.add(f);
            }
        }

        generateEncodingColumnNames();

        this.encodingSchema = null;
        this.encodingColumns = null;
    }

    public BaseBloomFilterEncoding(Schema schema, Schema encodingSchema, String uidColumnName, List<String> selectedColumnNames,
                                   int n, int k, int q) throws BloomFilterEncodingException {
        this(schema, uidColumnName, selectedColumnNames, n, k, q);

        this.encodingSchema = encodingSchema;

        encodingColumns = new ArrayList<Schema.Field>();
        for (Schema.Field f : this.encodingSchema.getFields())
            if (encodingColumnNames.contains(f.name())) encodingColumns.add(f);
    }

    protected String getName() {
        return String.format("_%d_%d_%d_", N, K, Q);
    }

    protected void generateEncodingColumnNames() throws BloomFilterEncodingException {
        if (selectedColumnNames == null) throw new BloomFilterEncodingException("Selected column names not set.");
        if (encodingColumnNames != null) throw new BloomFilterEncodingException("Encoding column names already set.");
    }

    public void makeEncodingSchema() throws BloomFilterEncodingException {
        if (encodingSchema != null) throw new BloomFilterEncodingException("Encoding schema already set.");

        encodingSchema = Schema.createRecord(
                String.format("Encoding%s%s", getName(), schema.getName()),
                String.format("PPRL Encoding(%s) : %s", getName(), schema.getName()),
                ("encoding" + getName().replace("_", ".") + schema.getNamespace()),
                false);

        makeEncodingColumns();
        encodingSchema.setFields(encodingColumns);
    }

    private void makeEncodingColumns() throws BloomFilterEncodingException {
        if (encodingColumns != null) throw new BloomFilterEncodingException("Encoding columns already set.");
        if (encodingColumnNames == null) throw new BloomFilterEncodingException("Encoding column names are not set.");

        final List<Schema.Field> fields = schema.getFields();
        encodingColumns = new ArrayList<Schema.Field>();
        for (Schema.Field f : fields) {
            if (!selectedColumnNames.contains(f.name())) {
                encodingColumns.add(new Schema.Field(f.name(), f.schema(), f.doc(), f.defaultValue(), f.order()));
            }
        }

        for (String name : encodingColumnNames) {
            encodingColumns.add(
                    new Schema.Field(name, Schema.createFixed(
                            name, null, null,
                            (int) Math.ceil(N / 8)),
                            String.format("Encoding(%s) of column(s) %s", getName(), name), null));
        }
    }

    public boolean validateEncodingSchema() throws BloomFilterEncodingException {
        if(schema == null) throw new BloomFilterEncodingException("Schema is not set.");
        if(encodingSchema == null) throw new BloomFilterEncodingException("Encoding schema is not set.");

        boolean properName = encodingSchema.getName().equals(
            String.format("Encoding%s%s", getName(), schema.getName()));
        boolean properdoc = encodingSchema.getDoc().equals(
            String.format("PPRL Encoding(%s) : %s", getName(), schema.getName()));
        boolean properNamespace = encodingSchema.getNamespace().equals(
            "encoding" + getName().replace("_", ".")  + schema.getNamespace());
        return validateEncodingColumns() & properName & properdoc & properNamespace;
    }

    public boolean validateEncodingColumns() throws BloomFilterEncodingException {
        if (encodingColumns == null) throw new BloomFilterEncodingException("Encoding columns already set.");

        for(Schema.Field f : encodingSchema.getFields()) {
            if(restColumns.contains(f)) continue;
            if(!encodingColumns.contains(f)) return false;
        }
        return true;
    }

    public abstract GenericData.Fixed encode(Object obj, Class<?> clz, Schema encodingFieldSchema);

    public abstract GenericData.Fixed encode(List<Object> objs, List<Class<?>> clzz, Schema encodingFieldSchema);

    public Schema getSchema() { return schema; }

    public Schema getEncodingSchema() {
        return encodingSchema;
    }

    public String getUidColumnName() {
        return uidColumnName;
    }

    public List<String> getSelectedColumnNames() {
        return selectedColumnNames;
    }

    public List<String> getEncodingColumnNames() {
        return encodingColumnNames;
    }

    public List<Schema.Field> getRestColumns() {
        return restColumns;
    }

    public List<Schema.Field> getSelectedColumns() {
        return selectedColumns;
    }

    public List<Schema.Field> getEncodingColumns() {
        return encodingColumns;
    }

    public int getN() {
        return N;
    }

    public int getK() {
        return K;
    }

    public int getQ() {
        return Q;
    }

    @Override
    public String toString() {
        return "{" +
                "selectedColumnNames=" + selectedColumnNames +
                ", encodingColumnNames=" + encodingColumnNames +
                ", N=" + N +
                ", K=" + K +
                ", Q=" + Q +
                '}';
    }
}
