package gr.upatras.ceid.pprl.encoding;

import org.apache.avro.Schema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class FieldBFEncoding extends SimpleBFEncoding{

    public FieldBFEncoding(String[] columns, int n, int k ,int q) {
        super(columns, n, k, q);
    }

    public FieldBFEncoding(Schema schema, String[] columns, int n, int k, int q) {
        super(schema, columns, n, k, q);
    }

    public FieldBFEncoding(Schema schema, Schema encodingSchema,
                           String[] columns, int n, int k , int q) {
        super(schema, encodingSchema, columns, n, k, q);
    }

    public String getName() {
        return String.format("_FBF_%d_%d_%d_",N,K,Q);
    }

    protected void setupEncodingSchemaFields() {
        final List<Schema.Field> fields = schema.getFields();
        final List<Schema.Field> outFields = new ArrayList<Schema.Field>();
        for(Schema.Field f : fields) {
            if(!columns.contains(f.name())) {
                outFields.add(new Schema.Field(f.name(),f.schema(),f.doc(),f.defaultValue(),f.order()));
            }
        }
        for(String column : columns)
            outFields.add(getEncodingField(Collections.singletonList(column)));
        encodingSchema.setFields(outFields);
    }

    protected boolean validateEncodingSchemaFields() {

        final List<Schema.Field> fields = schema.getFields();
        final List<Schema.Field> encFields = encodingSchema.getFields();

        for (Schema.Field f : encFields) {
            boolean properEnc = false;
            for(String column : columns) properEnc |= f.name().contains(column);
            if(properEnc) continue;
            if(!fields.contains(f)) {
                return false;
            }
        }
        return true;
    }
}
