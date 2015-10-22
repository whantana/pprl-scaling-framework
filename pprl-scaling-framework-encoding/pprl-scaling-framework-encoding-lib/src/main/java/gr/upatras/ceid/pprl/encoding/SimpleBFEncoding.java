package gr.upatras.ceid.pprl.encoding;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.Path;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class SimpleBFEncoding {

    protected List<String> columns;
    protected int N;
    protected int K;

    public SimpleBFEncoding(String[] columns, int n, int k) {
        this.columns = Arrays.asList(columns);
        N = n;
        K = k;
    }

    public Schema getEncodingSchemaFromSchema(final Schema schema) {
        final String name = String.format("Encoding%s%s", getName() ,schema.getName());
        final String doc = String.format("PPRL Encoding(%s) : %s",getName() ,schema.getName());
        final String namespace = "encoding." + getName().replace("_",".") + "." + schema.getNamespace();
        final List<Schema.Field> fields = schema.getFields();
        final List<Schema.Field> outFields = new ArrayList<Schema.Field>();
        for(Schema.Field f : fields) {
            if(!columns.contains(f.name())) {
                outFields.add(new Schema.Field(f.name(),f.schema(),f.doc(),f.defaultValue(),f.order()));
            }
        }
        final Schema encodingSchema = Schema.createRecord(name,doc,namespace,false);
        encodingSchema.setFields(outFields);
        return encodingSchema;
    }

    private String getName() {
        return String.format("_SBF_%d_%d_",N,K);
    }
}
