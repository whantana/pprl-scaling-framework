package gr.upatras.ceid.pprl.encoding;

import gr.upatras.ceid.pprl.qgram.QGramUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CLKEncoding extends BloomFilterEncoding{

    private static final Logger LOG = LoggerFactory.getLogger(CLKEncoding.class);
    private String encodingFieldName;
    private BloomFilter bf;

    public CLKEncoding(){}

    public CLKEncoding(final int N, final int K, final int Q){
        setN(new int[]{N});
        setK(K);
        setQ(Q);
    }

    @Override
    public String schemeName() {
        return "CLK";
    }

    @Override
    public void initialize() throws BloomFilterEncodingException {
        try {
            final Set<String> names = name2nameMap.keySet();
            for(String name : names) {
                final String mappedName = name2nameMap.get(name);
                if (mappedName.startsWith("encoding_field")) {
                    if (encodingFieldName == null) encodingFieldName = getMappedFieldName(name);
                    else assert encodingFieldName.equals(getMappedFieldName(name));
                }
            }
            bf = new BloomFilter(getCLKN(), K);
            LOG.debug("Initialized for CLK Encoding.");
        } catch (NoSuchAlgorithmException e) {
            throw new BloomFilterEncodingException(e.getMessage());
        } catch (InvalidKeyException e) {
            throw new BloomFilterEncodingException(e.getMessage());
        }
    }

    public int getCLKN() {
        return N[0];
    }

    @Override
    public List<Schema.Field> setupSelectedForEncodingFields(String[] selectedFieldNames) throws BloomFilterEncodingException {
        assert N != null && (N.length == 1) && getK() > 0 && getQ() > 0;
        StringBuilder sb = new StringBuilder(String.format("encoding_field_%d_%d_%d", getCLKN(), getK(), getQ()));
        for (String selectedFieldName : selectedFieldNames) sb.append("_src_").append(selectedFieldName);
        String encodingFieldName = sb.toString();

        for(String fieldName : selectedFieldNames)
            addMappedName(fieldName, encodingFieldName);

        Schema.Field[] encodingField = new Schema.Field[1];
        encodingField[0] = new Schema.Field(encodingFieldName,
                Schema.createFixed(
                        encodingFieldName, null, null,
                        (int) Math.ceil(getCLKN()/(double)8)),"CLK Encoding",null);
        return Arrays.asList(encodingField);
    }

    @Override
    public GenericRecord encodeRecord(GenericRecord record) throws BloomFilterEncodingException {
        final GenericRecord encodingRecord = new GenericData.Record(getEncodingSchema());
        bf.clear();
        for (Map.Entry<String,String> entry : name2nameMap.entrySet()) {
            final String name = entry.getKey();
            final String mappedName = entry.getValue();
            if (name.equals(mappedName))
                encodingRecord.put(mappedName, record.get(name));
            else {
                final Object obj = record.get(name);
                final Schema.Type type = record.getSchema().getField(name).schema().getType();
                encodeObject(obj, type, getQ(), bf);
            }
        }
        final Schema schema = encodingRecord.getSchema().getField(encodingFieldName).schema();
        final GenericData.Fixed fixed = new GenericData.Fixed(schema,
                Arrays.copyOf(
                        bf.getByteArray(),
                        bf.getByteArray().length)
        );
        encodingRecord.put(encodingFieldName,fixed);

        return encodingRecord;
    }

    private void encodeObject(final Object obj, final Schema.Type type, final int Q,
                              final BloomFilter bloomFilter)
            throws BloomFilterEncodingException {
        try{
            final String[] qGrams = QGramUtil.generateQGrams(obj, type, Q);
            for(String qGram : qGrams) bloomFilter.addData(qGram.getBytes("UTF-8"));
        } catch (UnsupportedEncodingException e) {
            throw new BloomFilterEncodingException(e.getMessage());
        }
    }

    @Override
    public void setupFromSchema(Schema encodingSchema) throws BloomFilterEncodingException {
        assert N == null && getEncodingSchema() == null && encodingSchema != null;

        String ns = encodingSchema.getNamespace();
        String s = ns.substring("encoding.schema.".length());
        String[] sParts = s.split("\\.");
        assert sParts.length == 3;
        setK(Integer.valueOf(sParts[1]));
        setQ(Integer.valueOf(sParts[2]));

        for(Schema.Field field : encodingSchema.getFields()) {
            if (field.name().startsWith("encoding_field_")) {
                String restName = field.name().substring("encoding_field_".length());
                String[] parts = restName.split("_src_");
                int clkN = Integer.parseInt(parts[0].split("_")[0]);
                for (int j = 1; j < parts.length; j++)
                    addMappedName(parts[j], field.name());
                int fbfCount = parts.length - 1;
                assert fbfCount > 0;
                setN(new int[1]);
                setN(clkN,0);
            } else name2nameMap.put(field.name(),field.name());
        }
        setEncodingSchema(encodingSchema);
    }
}
