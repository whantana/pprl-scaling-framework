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

public class RowBloomFilterEncoding extends FieldBloomFilterEncoding {

    private static final SecureRandom SECURE_RANDOM_GENERATOR = new SecureRandom();

    private int[] rbfCompositionCount;
    private int[] rbfCompositionSeeds;
    private int rbfBitPermutationSeed;

    private int[][] selectedBits;
    private BloomFilter rbf;
    private int[] bitPermutation;
    private String encodingFieldName;

    public RowBloomFilterEncoding(){}

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

    public String getName() {
        return "RBF_" + String.format("%d_%d",K,Q);
    }

    public String toString() { return "RBF"; }

    public int getRBFN() {
        return N[N.length - 1];
    }

    public void initialize()
            throws BloomFilterEncodingException {
        try {
            super.initialize();
            final Set<String> names = name2FBFMap.keySet();
            selectedBits = new int[names.size()][];
            for (String name : names) {
                int i = getIndex(name);
                if(encodingFieldName == null)
                    encodingFieldName = getFieldName(name);
                else assert encodingFieldName.equals(getFieldName(name));
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
            if (field.name().startsWith("encoding_field_")) {
                String restName = field.name().substring("encoding_field_".length());
                String[] parts = restName.split("_src_");
                int rbfN = Integer.parseInt(parts[0].split("_")[0]);
                for (int j = 1; j < parts.length; j++)
                    addToMap(parts[j], field.name());
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
                    addToMap(parts[i + 1], i);
                    rbfCompositionCount[i] = Integer.parseInt(partss[1]);
                    rbfCompositionSeeds[i] = Integer.parseInt(partss[2]);
                }
                rbfBitPermutationSeed = Integer.parseInt(docParts[fbfCount]);
            } else name2nameMap.put(field.name(),field.name());
        }
        setEncodingSchema(encodingSchema);
    }

    public List<Schema.Field> setupSelectedFields(final String[] selectedFieldNames) throws BloomFilterEncodingException {
        assert N != null && (N.length == selectedFieldNames.length + 1) && getK() > 0 && getQ() > 0;

        StringBuilder sb = new StringBuilder(String.format("encoding_field_%d_%d_%d", getRBFN(), getK(), getQ()));
        StringBuilder docSb = new StringBuilder();
        for (int i = 0; i < selectedFieldNames.length; i++) {
            docSb.append(String.valueOf(getN(i))).append(",")
                    .append(rbfCompositionCount[i]).append(",")
                    .append(rbfCompositionSeeds[i]).append("_");
            sb.append("_src_").append(selectedFieldNames[i]);
        }
        docSb.append(rbfBitPermutationSeed);
        String encodingFieldName = sb.toString();

        int i = 0;
        for(String fieldName : selectedFieldNames) {
            addToMap(fieldName, encodingFieldName);
            addToMap(fieldName, i);
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
        final GenericData.Fixed fixed = new GenericData.Fixed(schema, rbf.getByteArray());
        encodingRecord.put(encodingFieldName,fixed);

        return encodingRecord;
    }


    public static int[] randomBitSelection(final int bitCount, final int maxBit,final int seed) {
        final int[] randomBits = new int[bitCount];
        Random random = new Random(seed);
        for (int i = 0; i < bitCount; i++)
            randomBits[i] = random.nextInt(maxBit);
        return randomBits;
    }

    public static int[] randomBitPermutation(final int bitCount, final int seed) {
        List<Integer> c = new ArrayList<Integer>();
        for (int i = 0; i < bitCount; i++) c.add(i);
        Collections.shuffle(c, new Random(seed));
        Integer[] array = c.toArray(new Integer[c.size()]);
        int[] retArray = new int[bitCount];
        int i = 0;
        for(Integer ii : array) {
            retArray[i] = ii;
            i++;
        }
        return retArray;
    }

    public static int weightedsize(final int[] fbfNs, final double[] weights) {
        int N = 0;
        for (int i = 0; i < fbfNs.length; i++) {
            int rbfN = (int)(((double)fbfNs[i])/weights[i]);
            if(rbfN> N) N = rbfN;
        }
        return N;
    }
}
