package gr.upatras.ceid.pprl.encoding;

import org.apache.avro.Schema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class SimpleBFEncoding {

    protected Schema schema;
    protected Schema encodingSchema;
    protected List<String> columns;
    protected int N;
    protected int K;
    protected int Q;

    public SimpleBFEncoding(String[] columns, int n, int k, int q) {
        this.columns = Arrays.asList(columns);
        N = n;
        K = k;
        Q = q;
    }

    public SimpleBFEncoding(Schema schema,String[] columns, int n, int k, int q) {
        this(columns, n, k,q);
        this.schema = schema;
    }

    public SimpleBFEncoding(Schema schema,Schema encodingSchema,
                            String[] columns, int n, int k,int q) {
        this(schema, columns, n , k, q);
        this.encodingSchema = encodingSchema;
    }

    public void setupEncodingSchema() {
        final String name = String.format("Encoding%s%s", getName() ,schema.getName());
        final String doc = String.format("PPRL Encoding(%s) : %s",getName() ,schema.getName());
        final String namespace = "encoding" + getName().replace("_",".") + schema.getNamespace();
        encodingSchema = Schema.createRecord(name,doc,namespace,false);
        setupEncodingSchemaFields();
    }

    protected void setupEncodingSchemaFields() {
        final List<Schema.Field> fields = schema.getFields();
        final List<Schema.Field> outFields = new ArrayList<Schema.Field>();
        for(Schema.Field f : fields) {
            if(!columns.contains(f.name())) {
                outFields.add(new Schema.Field(f.name(),f.schema(),f.doc(),f.defaultValue(),f.order()));
            }
        }
        outFields.add(getEncodingField(columns));
        encodingSchema.setFields(outFields);
    }

    protected Schema.Field getEncodingField(final List<String> columns) {
        StringBuilder sb = new StringBuilder("enc");
        sb.append(getName());
        for(String column : columns) sb.append(column).append("_");
        sb.deleteCharAt(sb.lastIndexOf("_"));
        final String name = sb.toString();
        return new Schema.Field(
                name,
                Schema.createFixed(name,null,null,(int)Math.ceil(N/8)),
                String.format("Encoding(%s) of columns %s",getName(),columns),
                null
        );
    }

    public boolean validateEncodingSchema() {

        boolean properName = encodingSchema.getName().equals(
                String.format("Encoding%s%s", getName(), schema.getName()));
        boolean properdoc = encodingSchema.getDoc().equals(
                String.format("PPRL Encoding(%s) : %s", getName(), schema.getName()));
        boolean properNamespace = encodingSchema.getNamespace().equals(
                "encoding" + getName().replace("_", ".")  + schema.getNamespace());
        return validateEncodingSchemaFields() & properName & properdoc & properNamespace;
    }

    protected boolean validateEncodingSchemaFields() {

        final List<Schema.Field> fields = schema.getFields();
        final List<Schema.Field> encFields = encodingSchema.getFields();

        for (Schema.Field f : encFields) {
            if(f.equals(getEncodingField(columns))) continue;
            if(!fields.contains(f)) return false;
        }
        return true;
    }

    public String getName() {
        return String.format("_SBF_%d_%d_%d_",N,K,Q);
    }

    public Schema getEncodingSchema() {
        return encodingSchema;
    }

    public void setEncodingSchema(Schema encodingSchema) {
        this.encodingSchema = encodingSchema;
    }

    public Schema getSchema() {
        return schema;
    }

    public void setSchema(Schema schema) {
        this.schema = schema;
    }
}
