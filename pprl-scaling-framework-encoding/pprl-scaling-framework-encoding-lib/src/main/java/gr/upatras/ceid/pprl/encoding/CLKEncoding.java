package gr.upatras.ceid.pprl.encoding;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.util.List;

public class CLKEncoding extends BloomFilterEncoding{ // TODO not so simple

    @Override
    public String toString() {
        return null;
    }

    @Override
    public void initialize() throws BloomFilterEncodingException {

    }

    @Override
    public List<Schema.Field> setupSelectedFields(String[] selectedFieldNames) throws BloomFilterEncodingException {
        return null;
    }

    @Override
    public GenericRecord encodeRecord(GenericRecord record) throws BloomFilterEncodingException {
        return null;
    }

    @Override
    public void setupFromSchema(Schema encodingSchema) throws BloomFilterEncodingException {

    }
}
