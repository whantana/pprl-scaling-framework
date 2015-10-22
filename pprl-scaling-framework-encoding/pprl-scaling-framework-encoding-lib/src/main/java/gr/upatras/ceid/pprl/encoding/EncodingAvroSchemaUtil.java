package gr.upatras.ceid.pprl.encoding;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class EncodingAvroSchemaUtil {

    public static Schema getSchemaJsonFromHdfs(final Path schemaPath, final Configuration config) throws IOException {
        final FileSystem fs = FileSystem.get(config);
        final Schema schema = (new Schema.Parser()).parse(fs.open(schemaPath));
        fs.close();
        return schema;
    }

    public static Schema createEncodingSchema(final Schema schema , final List<String> columns, final String encodingMethod) {
        final String name = "Encoding" + encodingMethod + schema.getName();
        final String doc = "PPRL Encoding(" + encodingMethod +") dataset with name" + schema.getName();
        final String namespace = "encoding." + encodingMethod + "." + schema.getNamespace();
        final List<Schema.Field> fields = schema.getFields();
        final List<Schema.Field> outFields = new ArrayList<Schema.Field>();
        for(Schema.Field f : fields) {
            if(!columns.contains(f.name())) { // TODO about encoding fields
                outFields.add(new Schema.Field(f.name(),f.schema(),f.doc(),f.defaultValue(),f.order()));
            }
        }
        final Schema encodingSchema = Schema.createRecord(name,doc,namespace,false);
        encodingSchema.setFields(outFields);
        return encodingSchema;
    }

    public static boolean validateEncocingSchema(final Schema schema, final Schema encodingSchema,
                                          final List<String> columns, final String encodingMethod) {

        boolean properName = encodingSchema.getName().contains("Encoding");
        boolean properdoc = encodingSchema.getDoc().contains("Encoding(" + encodingMethod +")");
        boolean properNamespace = encodingSchema.getNamespace().contains(
                "encoding." + encodingMethod + "." + schema.getNamespace());
        final List<Schema.Field> fields = schema.getFields();
        final List<Schema.Field> encFields = encodingSchema.getFields();

        for(Schema.Field f : fields) {
            if(!columns.contains(f.name())) { // TODO about encoding fields
                if(!encFields.contains(f)) return false;
            }
        }

        return properName & properdoc & properNamespace;
    }
}
