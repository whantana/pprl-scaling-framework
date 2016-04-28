package gr.upatras.ceid.pprl.encoding;

import gr.upatras.ceid.pprl.qgram.QGramUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Field Bloom Filter (FBF) encoding class.
 */
public class FieldBloomFilterEncoding extends BloomFilterEncoding {

    protected Map<String,BloomFilter> name2FBFMap = new HashMap<String,BloomFilter>(); // fieldName to bloom filter map.
    protected Map<String,Integer> name2indexMap = new HashMap<String,Integer>();   // fieldName to index map.

    /**
     * Constructor
     */
    public FieldBloomFilterEncoding(){}

    public FieldBloomFilterEncoding(final int N, final int Ncount , final int K, final int Q){
        this(staticsizes(N,Ncount),K,Q);
    }

    /**
     * Constructor (dynamic FBF).
     *
     * @param avgQCount average Q grams count per field.
     * @param K number of hash values.
     * @param Q Q as in Q-Grams.
     */
    public FieldBloomFilterEncoding(final double[] avgQCount, final int K, final int Q) {
        this(dynamicsizes(avgQCount,K),K,Q);
    }

    /**
     * Constructor (Static FBF).
     *
     * @param N bloom filter size per field.
     * @param K number of hash values.
     * @param Q Q as in Q-Grams.
     */
    public FieldBloomFilterEncoding(final int[] N, final int K, final int Q) {
        setN(N);
        setK(K);
        setQ(Q);
    }

    /**
     * Returns "FBF".
     *
     * @return "FBF
     */
    @Override
    public String schemeName() { return "FBF"; }

    /**
     * Initializes encoding (makes it ready to encode records).
     *
     * @throws BloomFilterEncodingException
     */
    @Override
    public void initialize() throws BloomFilterEncodingException {
        for(String name : name2indexMap.keySet()) addFBF(name);
    }

    @Override
    public String toString() {
        return super.toString() +
                ", name2indexMap=" + name2indexMap +
                '}';
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
        int Nlength = 0;
        for(Schema.Field field : encodingSchema.getFields())
            if(field.name().startsWith("encoding_field_")) Nlength++;
        setN(new int[Nlength]);
        int i = 0;
        for(Schema.Field field : encodingSchema.getFields()) {
            final String name = field.name();
            if(name.startsWith("encoding_field_")) {
                String restName = name.substring("encoding_field_".length());
                String[] partss = restName.split("_src_");
                assert partss.length == 2;
                String[] parts = partss[0].split("_");
                assert parts.length == 3;
                setN(Integer.parseInt(parts[0]),i);
                assert getK() == Integer.parseInt(parts[1]);
                assert getQ() == Integer.parseInt(parts[2]);
                addIndex(partss[1], i);
                addMappedName(partss[1], name);
                i++;
            } else addMappedName(name, name);
        }
        setEncodingSchema(encodingSchema);
    }

    /**
     * Setup source field (selected field names) to return encoding fields.
     *
     * @param selectedFieldNames selected field names
     * @return encoding fields.
     * @throws BloomFilterEncodingException
     */
    public List<Schema.Field> setupSelectedForEncodingFields(final String[] selectedFieldNames) throws BloomFilterEncodingException {
        assert N != null && N.length == selectedFieldNames.length && getK() > 0 && getQ() > 0;
        Schema.Field[] encodingFields = new Schema.Field[selectedFieldNames.length];
        int i = 0;
        for(String fieldName : selectedFieldNames) {
            String encodingFieldName = String.format("encoding_field_%d_%d_%d_src_%s", getN(i), getK(), getQ(), fieldName);
            encodingFields[i] =new Schema.Field(
                    encodingFieldName, Schema.createFixed(
                    encodingFieldName, null, null,
                    (int) Math.ceil(getN(i)/(double)8)),
                    String.format("Encoding(%s) of field %s", schemeName(), fieldName), null);
            addIndex(fieldName, i);
            addMappedName(fieldName, encodingFieldName);
            i++;
        }
        return Arrays.asList(encodingFields);
    }

    /**
     * Returns encoded record based on the encoding scheme and input record.
     *
     * @param record input generic record.
     * @return encoded records (generic record).
     * @throws BloomFilterEncodingException
     */
    @Override
    public GenericRecord encodeRecord(GenericRecord record)
            throws BloomFilterEncodingException {
        final GenericRecord encodingRecord = new GenericData.Record(getEncodingSchema());
        for (Map.Entry<String,String> entry : name2nameMap.entrySet()) {
            final String name = entry.getKey();
            final String mappedName = entry.getValue();
            if(name.equals(mappedName))
                encodingRecord.put(mappedName,record.get(name));
            else {
                final Object obj = record.get(name);
                final Schema.Type type = record.getSchema().getField(name).schema().getType();
                encodeObject(obj, type, getQ(), getFBF(name));
                final Schema schema = encodingRecord.getSchema().getField(mappedName).schema();
                final GenericData.Fixed fixed = new GenericData.Fixed(schema,
                        Arrays.copyOf(
                                getFBF(name).getByteArray(),
                                getFBF(name).getByteArray().length)
                );
                encodingRecord.put(mappedName,fixed);
            }
        }
        return encodingRecord;
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
    protected void encodeObject(final Object obj, final Schema.Type type, final int Q,
                                final BloomFilter bloomFilter)
            throws BloomFilterEncodingException {
        try{
            final String[] qGrams = QGramUtil.generateQGrams(obj,type,Q);
            bloomFilter.clear();
            for(String qGram : qGrams) bloomFilter.addData(qGram.getBytes("UTF-8"));
        } catch (UnsupportedEncodingException e) {
            throw new BloomFilterEncodingException(e.getMessage());
        }
    }

    /**
     * Adds a Bloom-Filter for an input field name.
     *
     * @param fieldName field name.
     * @throws BloomFilterEncodingException
     */
    protected void addFBF(final String fieldName)
            throws BloomFilterEncodingException{
        try {
            final int index = getIndex(fieldName);
            final int fbfN = getN(index);
            final int K = getK();
            name2FBFMap.put(fieldName, new BloomFilter(fbfN, K));
        } catch (NoSuchAlgorithmException e) {
            throw new BloomFilterEncodingException(e.getMessage());
        } catch (InvalidKeyException e) {
            throw new BloomFilterEncodingException(e.getMessage());
        }
    }

    /**
     * Returns the Bloom-Filter instance for an input field name.
     *
     * @param fieldName field name.
     * @return bloom filter instance.
     */
    public BloomFilter getFBF(final String fieldName) {
        if(!name2FBFMap.containsKey(fieldName))
            throw new IllegalArgumentException("Cannot find FBF fieldName for fieldName " + fieldName);
        return name2FBFMap.get(fieldName);
    }

    /**
     * Maps index to field name (adds to a map).
     *
     * @param fieldName field name.
     * @param index index an integer.
     */
    protected void addIndex(final String fieldName, final int index) {
        name2indexMap.put(fieldName, index);
    }

    /**
     * Returns index mapped with a name.
     *
     * @param fieldName field name.
     * @return index (an integer).
     */
    public int getIndex(final String fieldName) {
        if(!name2indexMap.containsKey(fieldName))
            throw new IllegalArgumentException("Cannot find index fieldName for fieldName " + fieldName);
        return name2indexMap.get(fieldName);
    }

    /**
     * Returns an integer array filled with N (single static size for all fbfs).
     *
     * @param N bloom filter size.
     * @param Ncount number of fields to be encoded.
     * @return an integer array filled with N
     */
    public static int[] staticsizes(int N,int Ncount) {
        int[] Ns = new int[Ncount];
        Arrays.fill(Ns,N);
        return Ns;
    }

    /**
     * Calculate and return the dynamic size of a bloom filter according to g and K.
     *
     * @param g expected number of elements.
     * @param K number of hash values.
     * @return the dynamic size of a bloom filter
     */
    public static int dynamicsize(double g, int K) {
        return (int) Math.ceil((1 / (1 - Math.pow(0.5, (double) 1 / (g * K)))));
    }

    /**
     * Calculate and return the dynamic size of all field bloom filters according to g and K.
     *
     * @param g expected number of elements per field.
     * @param K number of hash values.
     * @return the dynamic size of a bloom filter
     */
    public static int[] dynamicsizes(double[] g, int K) {
        int Ns[] = new int[g.length];
        for (int i = 0; i < g.length; i++) {
            Ns[i] = dynamicsize(g[i],K);
        }
        return Ns;
    }
}
