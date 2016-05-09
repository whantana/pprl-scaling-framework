package gr.upatras.ceid.pprl.encoding;

import gr.upatras.ceid.pprl.qgram.QGramUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Field Bloom Filter (FBF) encoding class.
 */
public class FieldBloomFilterEncoding extends BloomFilterEncoding {

    protected Map<String,BloomFilter> name2FBFMap = new LinkedHashMap<String, BloomFilter>(); // fieldName to bloom filter map.
    protected Map<String,Integer> name2indexMap = new LinkedHashMap<String,Integer>();        // fieldName to index map.
    private BloomFilter bf;                                                                   // composite bloom filter.
    private int bfN;                                                                          // composite bloom filter N.


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

    @Override
    public String toString() {
        return super.toString() + ", name2indexMap=" + name2indexMap + '}';
    }

    /**
     * Initializes encoding (makes it ready to encode records).
     *
     * @throws BloomFilterEncodingException
     */
    @Override
    public void initialize() throws BloomFilterEncodingException {
        assert bfN != 0;
        for(String name : name2indexMap.keySet()) {
            if(encodingFieldName == null)
                encodingFieldName = getMappedFieldName(name);
            addFBF(name);
        }
        try {
            bf = new BloomFilter(bfN,K);
        } catch (NoSuchAlgorithmException e) {
            throw new BloomFilterEncodingException(e.getMessage());
        } catch (InvalidKeyException e) {
            throw new BloomFilterEncodingException(e.getMessage());
        }
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
                final String doc = field.doc();
                final String[] docs = doc.split(",");
                setN(new int[docs.length]);
                for (int i = 0; i < docs.length; i++)
                    setN(Integer.parseInt(docs[0]), i);
                final String name = field.name();
                String restName = name.substring(ENCODING_FIELD_PREFIX.length());
                String[] partss = restName.split(FIELD_DELIMITER);
                String[] parts = partss[0].split("_");
                bfN = Integer.parseInt(parts[0]);
                assert getK() == Integer.parseInt(parts[1]);
                assert getQ() == Integer.parseInt(parts[2]);
                for (int i = 1; i < partss.length; i++) {
                    addIndex(partss[i],i-1);
                    addMappedName(partss[i],name);
                }
                encodingFieldName = field.name();
            } else addMappedName(field.name(), field.name());
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

        bfN = 0;
        for(int i = 0; i < selectedFieldNames.length ; i++) bfN += getN(i);

        final StringBuilder nsb = new StringBuilder(
                String.format("%s%d_%d_%d", ENCODING_FIELD_PREFIX, bfN, getK(), getQ()));
        final StringBuilder docSb = new StringBuilder();

        for(int i = 0; i < selectedFieldNames.length ; i++) {
            nsb.append(FIELD_DELIMITER).append(selectedFieldNames[i]);
            docSb.append(((i != 0) ? "," : "")).append(getN(i));
            addIndex(selectedFieldNames[i], i);
        }

        encodingFieldName = nsb.toString();
        final String encodingDoc = docSb.toString();
        final Schema.Field encodingField = new Schema.Field(
                encodingFieldName,
                Schema.createFixed(encodingFieldName, null, null, (int) Math.ceil(bfN/(double)8)),
                encodingDoc,null);

        for (String selectedFieldName : selectedFieldNames)
            addMappedName(selectedFieldName, encodingFieldName);

        return Collections.singletonList(encodingField);
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
            if (name.equals(mappedName))
                encodingRecord.put(mappedName, record.get(name));
            else {
                final Object obj = record.get(name);
                final Schema.Type type = record.getSchema().getField(name).schema().getType();
                encodeObject(obj, type, getQ(), getFBF(name));
            }
        }

        bf.clear();
        int Npart = 0;
        for(Map.Entry<String,BloomFilter> entry : name2FBFMap.entrySet()) {
            final BloomFilter fbf = entry.getValue();
            final int fbfN = fbf.getN();
            for (int bit = 0; bit < fbfN; bit++) {
                bf.setBit(Npart + bit,fbf.getBit(bit));
            }
            Npart = fbf.getN();
        }

        final Schema schema = encodingRecord.getSchema().getField(encodingFieldName).schema();
        final GenericData.Fixed fixed = new GenericData.Fixed(schema,
            Arrays.copyOf(bf.getByteArray(), bf.getByteArray().length)
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
        return new BloomFilter(getFBFN(),fixed.bytes());
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
     * Returns the total bit count of this encoding.
     *
     * @return the total bit count of this encoding.
     */
    public int getFBFN() {
        return bfN;
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
