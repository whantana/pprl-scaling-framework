package gr.upatras.ceid.pprl.encoding;

import gr.upatras.ceid.pprl.qgram.QGramUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Cryptographically Long Key (CLK) Encoding class.
 */
public class CLKEncoding extends BloomFilterEncoding {

    private BloomFilter bf; // bloom filter (only need 1 bloom filter)

    /**
     * Constructor
     */
    public CLKEncoding(){}

    /**
     * Constructor.
     *
     * @param N bloom filter size.
     * @param K number of hash values.
     * @param Q as in Q-Grams.
     */
    public CLKEncoding(final int N, final int K, final int Q){
        setN(new int[]{N});
        setK(K);
        setQ(Q);
    }


    /**
     * Returns "CLK".
     *
     * @return "CLK".
     */
    @Override
    public String schemeName() {
        return "CLK";
    }

    /**
     * Initializes encoding (makes it ready to encode records).
     *
     * @throws BloomFilterEncodingException
     */
    @Override
    public void initialize() throws BloomFilterEncodingException {
        try {
            final Set<String> names = name2nameMap.keySet();
            for(String name : names) {
                final String mappedName = name2nameMap.get(name);
                if (mappedName.startsWith(ENCODING_FIELD_PREFIX)) {
                    if (encodingFieldName == null) encodingFieldName = getMappedFieldName(name);
                    else assert encodingFieldName.equals(getMappedFieldName(name));
                }
            }
            bf = new BloomFilter(getCLKN(), K);
        } catch (NoSuchAlgorithmException e) {
            throw new BloomFilterEncodingException(e.getMessage());
        } catch (InvalidKeyException e) {
            throw new BloomFilterEncodingException(e.getMessage());
        }
    }

    /**
     * Returns bloom filter's length.
     *
     * @return bloom filter's length.
     */
    public int getCLKN() {
        return N[0];
    }

    @Override
    public int getBFN() { return getCLKN();}

    /**
     * Setup source field (selected field names) to return encoding fields.
     *
     * @param selectedFieldNames selected field names
     * @return encoding fields.
     * @throws BloomFilterEncodingException
     */
    @Override
    public List<Schema.Field> setupSelectedForEncodingFields(String[] selectedFieldNames) throws BloomFilterEncodingException {
        assert N != null && (N.length == 1) && getK() > 0 && getQ() > 0;
        StringBuilder sb = new StringBuilder(String.format("%s%d_%d_%d",ENCODING_FIELD_PREFIX, getCLKN(), getK(), getQ()));
        for (String selectedFieldName : selectedFieldNames) sb.append(BloomFilterEncoding.FIELD_DELIMITER).append(selectedFieldName);
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

    /**
     * Returns encoded record based on the encoding scheme and input record.
     *
     * @param record input generic record.
     * @return encoded records (generic record).
     * @throws BloomFilterEncodingException
     */
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

    /**
     * Retrieve bloom filter from the record.
     *
     * @param record generic record.
     * @return a <code>BloomFilter</code> instance
     */
    public BloomFilter retrieveBloomFilter(final GenericRecord record) {
        GenericData.Fixed fixed = (GenericData.Fixed) record.get(encodingFieldName);
        return new BloomFilter(getCLKN(),fixed.bytes());
    }

    /**
     * Encodes object by adding it's q-grams to its bloom filter.
     *
     * @param obj object
     * @param type type
     * @param Q Q as in q-gram.
     * @param bloomFilter a bloom filter.
     * @throws BloomFilterEncodingException
     */
    private void encodeObject(final Object obj, final Schema.Type type, final int Q,
                              final BloomFilter bloomFilter)
            throws BloomFilterEncodingException {
        final String[] qGrams = QGramUtil.generateQGrams(obj, type, Q);
        for(String qGram : qGrams) bloomFilter.addData(qGram);
    }

    /**
     * Setup an encoding based on existing encoding schema.
     *
     * @param encodingSchema encoding schema.
     * @throws BloomFilterEncodingException
     */
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
            if (field.name().startsWith(ENCODING_FIELD_PREFIX)) {
                String restName = field.name().substring(ENCODING_FIELD_PREFIX.length());
                String[] parts = restName.split(BloomFilterEncoding.FIELD_DELIMITER);
                int clkN = Integer.parseInt(parts[0].split("_")[0]);
                for (int j = 1; j < parts.length; j++)
                    addMappedName(parts[j], field.name());
                int fbfCount = parts.length - 1;
                assert fbfCount > 0;
                setN(new int[1]);
                setN(clkN,0);
                encodingFieldName = field.name();
            } else name2nameMap.put(field.name(),field.name());
        }
        setEncodingSchema(encodingSchema);
    }
}
