package gr.upatras.ceid.pprl.encoding;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class EncodingAvroSchemaUtil {

    public static Schema loadAvroSchemaFromHdfs(final Path schemaPath, final Configuration config) throws IOException {
        final FileSystem fs = FileSystem.get(config);
        Schema schema = readAvroSchemaFromInputStream(fs.open(schemaPath));
        fs.close();
        return schema;
    }

    public static Schema loadAvroSchemaFromFile(final File schemaFile) throws IOException {
        return readAvroSchemaFromInputStream(new FileInputStream(schemaFile));
    }

    private static Schema readAvroSchemaFromInputStream(final InputStream is) throws IOException {
        return (new Schema.Parser()).parse(is);
    }

    public static void saveAvroSchemaOnHdfs(final Schema schema,final Path schemaPath, final Configuration config) throws IOException {
        final FileSystem fs = FileSystem.get(config);
        writeAvroSchemaToOutputStream(schema,fs.create(schemaPath,true));
        fs.close();
    }
    public static void saveAvroSchemaToFile(final Schema schema,final File schemaFile) throws IOException {
        if(!schemaFile.exists()) schemaFile.createNewFile();
        writeAvroSchemaToOutputStream(schema, new FileOutputStream(schemaFile,false));
    }
    private static void writeAvroSchemaToOutputStream(final Schema schema,final OutputStream os) throws IOException {
        os.write(schema.toString(true).getBytes());
    }
}
