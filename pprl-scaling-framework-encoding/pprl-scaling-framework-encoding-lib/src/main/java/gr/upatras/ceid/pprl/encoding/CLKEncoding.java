package gr.upatras.ceid.pprl.encoding;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class CLKEncoding extends BloomFilterEncoding{ // TODO not so simple

    private static final Logger LOG = LoggerFactory.getLogger(CLKEncoding.class);


    @Override
    public String schemeName() {
        return "CLK";
    }

    @Override
    public void initialize() throws BloomFilterEncodingException {

    }

    @Override
    public String toString() {
        return super.toString();
    }

    @Override
    public List<Schema.Field> setupSelectedForEncodingFields(String[] selectedFieldNames) throws BloomFilterEncodingException {
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
