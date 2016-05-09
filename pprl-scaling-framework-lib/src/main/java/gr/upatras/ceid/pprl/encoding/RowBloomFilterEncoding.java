package gr.upatras.ceid.pprl.encoding;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/**
 * Row Bloom Filter (RBF) Encoding class.
 */
public class RowBloomFilterEncoding extends FieldBloomFilterEncoding {

    private static final SecureRandom SECURE_RANDOM_GENERATOR = new SecureRandom(); // random generator

    private int[] rbfCompositionCount; // how many bits to be sampled from each FBF.
    private int[] rbfCompositionSeeds; // seeds for producing random sequence of integers (limit : rbfCompositionCount).
    private int rbfBitPermutationSeed; // a seed for the random bit permutation

    private int[][] selectedBits;      // selected bits from each FBF.
    private BloomFilter rbf;           // the Bloom filter for the rbf
    private int[] bitPermutation;      // the random bit permutation

    /**
     * Constructor
     */
    public RowBloomFilterEncoding(){}

    /**
     * Constructor for RBF(Uniform bit selection, Dynamic FBFs).
     *
     * @param avgQCount average q-gram count per field.
     * @param N bloom filter (RBF) length.
     * @param K number of hash values.
     * @param Q Q as in Q-Grams.
     */
    public RowBloomFilterEncoding(final double[] avgQCount, final int N, final int K, final int Q) {
        int[] fbfNs = FieldBloomFilterEncoding.dynamicsizes(avgQCount, K);
        int[] Ns = new int[fbfNs.length + 1];
        System.arraycopy(fbfNs,0,Ns,0,fbfNs.length);
        Ns[fbfNs.length] = N;

        setN(Ns);
        setK(K);
        setQ(Q);

        double selectedBitCount = getRBFN()/((double)fbfNs.length);
        rbfCompositionCount = new int[fbfNs.length];
        rbfCompositionSeeds = new int[fbfNs.length];
        for (int i = 0; i < fbfNs.length; i++) {
            rbfCompositionCount[i] =  (i == 0) ?
                    (int) Math.ceil(selectedBitCount) :
                    (int) Math.floor(selectedBitCount);
            rbfCompositionSeeds[i] = SECURE_RANDOM_GENERATOR.nextInt(1000);
        }
        rbfBitPermutationSeed = SECURE_RANDOM_GENERATOR.nextInt(1000);
    }

    /**
     * Constructor for RBF(Weighted bit selection, Dynamic FBFs).
     *
     * @param avgQCount average q-gram count per field.
     * @param weights weighted selection of bits from the FBFs.
     * @param K number of hash values.
     * @param Q Q as in Q-Grams.
     */
    public RowBloomFilterEncoding(final double[] avgQCount, final double[] weights, final int K, final int Q) {
        assert avgQCount.length == weights.length;
        int[] fbfNs = FieldBloomFilterEncoding.dynamicsizes(avgQCount,K);
        int[] Ns = new int[fbfNs.length + 1];
        System.arraycopy(fbfNs, 0, Ns, 0, fbfNs.length);
        Ns[fbfNs.length] = weightedsize(fbfNs,weights);
        setN(Ns);
        setK(K);
        setQ(Q);

        rbfCompositionCount = new int[fbfNs.length];
        rbfCompositionSeeds = new int[fbfNs.length];
        for (int i = 0; i < fbfNs.length; i++) {
            rbfCompositionCount[i] = (int) ((double) getRBFN()*weights[i]);
            rbfCompositionSeeds[i] = SECURE_RANDOM_GENERATOR.nextInt(1000);
        }
        rbfBitPermutationSeed = SECURE_RANDOM_GENERATOR.nextInt(1000);
    }

    /**
     * Constructor for RBF(Uniform bit selection, Static FBFs).
     *
     * @param fbfNs sizes of the FBFs.
     * @param N bloom filter (RBF) length.
     * @param K number of hash values.
     * @param Q Q as in Q-Grams.
     */
    public RowBloomFilterEncoding(int[] fbfNs, final int N, final int K, final int Q) {
        int[] Ns = new int[fbfNs.length + 1];
        System.arraycopy(fbfNs,0,Ns,0,fbfNs.length);
        Ns[fbfNs.length] = N;

        setN(Ns);
        setK(K);
        setQ(Q);

        double selectedBitCount = getRBFN()/((double)fbfNs.length);
        rbfCompositionCount = new int[fbfNs.length];
        rbfCompositionSeeds = new int[fbfNs.length];
        for (int i = 0; i < fbfNs.length; i++) {
            rbfCompositionCount[i] =  (i == 0) ?
                    (int) Math.ceil(selectedBitCount) :
                    (int) Math.floor(selectedBitCount);
            rbfCompositionSeeds[i] = SECURE_RANDOM_GENERATOR.nextInt(1000);
        }
        rbfBitPermutationSeed = SECURE_RANDOM_GENERATOR.nextInt(1000);
    }

    /**
     * Constructor for RBF(Weighted bit selection, Static FBFs).
     *
     * @param fbfNs sizes of the FBFs.
     * @param weights weighted selection of bits from the FBFs.
     * @param K number of hash values.
     * @param Q Q as in Q-Grams.
     */
    public RowBloomFilterEncoding(int[] fbfNs, final double[] weights, final int K, final int Q) {
        assert fbfNs.length == weights.length;
        int[] Ns = new int[fbfNs.length + 1];
        System.arraycopy(fbfNs, 0, Ns, 0, fbfNs.length);
        Ns[fbfNs.length] = weightedsize(fbfNs,weights);
        setN(Ns);
        setK(K);
        setQ(Q);

        rbfCompositionCount = new int[fbfNs.length];
        rbfCompositionSeeds = new int[fbfNs.length];
        for (int i = 0; i < fbfNs.length; i++) {
            rbfCompositionCount[i] = (int) ((double) getRBFN()*weights[i]);
            rbfCompositionSeeds[i] = SECURE_RANDOM_GENERATOR.nextInt(1000);
        }
        rbfBitPermutationSeed = SECURE_RANDOM_GENERATOR.nextInt(1000);
    }

    /**
     * Returns "RBF".
     *
     * @return "RBF".
     */
    @Override
    public String schemeName() { return "RBF"; }

    /**
     * Returns the size of the bloom filter.
     *
     * @return the size of the bloom filter.
     */
    public int getRBFN() {
        return N[N.length - 1];
    }

    /**
     * Initializes encoding (makes it ready to encode records).
     *
     * @throws BloomFilterEncodingException
     */
    @Override
    public void initialize()
            throws BloomFilterEncodingException {
        try {
            for(String name : name2indexMap.keySet()) addFBF(name);
            final Set<String> names = name2FBFMap.keySet();
            selectedBits = new int[names.size()][];
            for (String name : names) {
                int i = getIndex(name);
                if(encodingFieldName == null)
                    encodingFieldName = getMappedFieldName(name);
                else assert encodingFieldName.equals(getMappedFieldName(name));
                int bitCount = rbfCompositionCount[i];
                int seed = rbfCompositionSeeds[i];
                int maxBit = getN(i);
                selectedBits[i] = RowBloomFilterEncoding.randomBitSelection(bitCount,maxBit,seed);
            }
            bitPermutation  = randomBitPermutation(getRBFN(), rbfBitPermutationSeed);
            rbf = new BloomFilter(getRBFN(),getK());
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
    public void setupFromSchema(Schema encodingSchema)
            throws BloomFilterEncodingException {
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
                String[] parts = restName.split(FIELD_DELIMITER);
                int rbfN = Integer.parseInt(parts[0].split("_")[0]);
                for (int j = 1; j < parts.length; j++)
                    addMappedName(parts[j], field.name());
                int fbfCount = parts.length - 1;
                assert fbfCount > 0;
                setN(new int[fbfCount + 1]);
                setN(rbfN,fbfCount);
                rbfCompositionCount = new int[fbfCount];
                rbfCompositionSeeds = new int[fbfCount];
                String[] docParts = field.doc().split("_");
                assert docParts.length == (fbfCount + 1);
                for (int i = 0; i < fbfCount; i++) {
                    final String[] partss = docParts[i].split(",");
                    assert partss.length == 3;
                    setN(Integer.parseInt(partss[0]),i);
                    addIndex(parts[i + 1], i);
                    rbfCompositionCount[i] = Integer.parseInt(partss[1]);
                    rbfCompositionSeeds[i] = Integer.parseInt(partss[2]);
                }
                rbfBitPermutationSeed = Integer.parseInt(docParts[fbfCount]);
                encodingFieldName = field.name();
            } else name2nameMap.put(field.name(),field.name());
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
    @Override
    public List<Schema.Field> setupSelectedForEncodingFields(final String[] selectedFieldNames) throws BloomFilterEncodingException {
        assert N != null && (N.length == selectedFieldNames.length + 1) && getK() > 0 && getQ() > 0;

        StringBuilder sb = new StringBuilder(String.format("%s%d_%d_%d",ENCODING_FIELD_PREFIX, getRBFN(), getK(), getQ()));
        StringBuilder docSb = new StringBuilder();
        for (int i = 0; i < selectedFieldNames.length; i++) {
            docSb.append(String.valueOf(getN(i))).append(",")
                    .append(rbfCompositionCount[i]).append(",")
                    .append(rbfCompositionSeeds[i]).append("_");
            sb.append(FIELD_DELIMITER).append(selectedFieldNames[i]);
        }
        docSb.append(rbfBitPermutationSeed);
        String encodingFieldName = sb.toString();

        int i = 0;
        for(String fieldName : selectedFieldNames) {
            addMappedName(fieldName, encodingFieldName);
            addIndex(fieldName, i);
            i++;
        }

        Schema.Field[] encodingField = new Schema.Field[1];
        encodingField[0] = new Schema.Field(encodingFieldName,
                Schema.createFixed(
                        encodingFieldName, null, null,
                        (int) Math.ceil(getRBFN()/(double)8)),
                docSb.toString(),
                null);
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
        rbf.clear();
        int rbfBit = 0;
        for(String name : name2FBFMap.keySet()) {
            final int i = getIndex(name);
            final BloomFilter fbf = getFBF(name);
            for (int fbfBit : selectedBits[i])
                rbf.setBit(bitPermutation[rbfBit++], fbf.getBit(fbfBit));
        }

        final Schema schema = encodingRecord.getSchema().getField(encodingFieldName).schema();
        final GenericData.Fixed fixed = new GenericData.Fixed(schema,
                Arrays.copyOf(
                        rbf.getByteArray(),
                        rbf.getByteArray().length)
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
        return new BloomFilter(getRBFN(),fixed.bytes());
    }

    @Override
    public String toString() {
        return super.toString() +
                ", rbfCompositionCount=" + Arrays.toString(rbfCompositionCount) +
                ", rbfCompositionSeeds=" + Arrays.toString(rbfCompositionSeeds) +
                ", rbfBitPermutationSeed=" + rbfBitPermutationSeed +
                ", selectedBits=" + Arrays.toString(selectedBits) +
                ", encodingFieldName='" + encodingFieldName + '\'' +
                '}';
    }

    /**
     * Generate and return a random bit selection from a bit sequence.
     *
     * @param bitCount size of bit selection.
     * @param maxBit bit sequence length
     * @param seed an integer as a seed.
     * @return a random bit selection from a bit sequence.
     */
    public static int[] randomBitSelection(final int bitCount, final int maxBit,final int seed) {
        final int[] randomBits = new int[bitCount];
        Random random = new Random(seed);
        for (int i = 0; i < bitCount; i++)
            randomBits[i] = random.nextInt(maxBit);
        return randomBits;
    }

    /**
     * Generate and return a random permutation of a bit sequence.
     *
     * @param maxBit bit sequence length
     * @param seed an integer as a seed.
     * @return a random permutation of a bit sequence.
     */
    public static int[] randomBitPermutation(final int maxBit, final int seed) {
        List<Integer> c = new ArrayList<Integer>();
        for (int i = 0; i < maxBit; i++) c.add(i);
        Collections.shuffle(c, new Random(seed));
        Integer[] array = c.toArray(new Integer[c.size()]);
        int[] retArray = new int[maxBit];
        int i = 0;
        for(Integer ii : array) {
            retArray[i] = ii;
            i++;
        }
        return retArray;
    }

    /**
     * Calculate and return the RBF bloom filter size when in weighted
     * bit selection from the FBFs. The weight that maximizes the RBF is chosen.
     *
     * @param fbfNs sizes of the FBFs.
     * @param weights weighted selection of bits from the FBFs.
     * @return the RBF bloom filter size
     */
    public static int weightedsize(final int[] fbfNs, final double[] weights) {
        int N = 0;
        for (int i = 0; i < fbfNs.length; i++) {
            int rbfN = (int)(((double)fbfNs[i])/weights[i]);
            if(rbfN> N) N = rbfN;
        }
        return N;
    }
}
