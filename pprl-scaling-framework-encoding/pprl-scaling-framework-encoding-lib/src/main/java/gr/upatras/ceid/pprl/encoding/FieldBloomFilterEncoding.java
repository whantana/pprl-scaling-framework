package gr.upatras.ceid.pprl.encoding;

import gr.upatras.ceid.pprl.datasets.QGramUtil;
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

public class FieldBloomFilterEncoding extends BloomFilterEncoding {

    protected Map<String,BloomFilter> name2FBFMap = new HashMap<String,BloomFilter>();
    protected Map<String,Integer> name2indexMap = new HashMap<String,Integer>();

    public FieldBloomFilterEncoding(){}

    public FieldBloomFilterEncoding(final int N, final int Ncount , final int K, final int Q){
        this(staticsizes(N,Ncount),K,Q);
    }

    public FieldBloomFilterEncoding(final double[] avgQCount, final int K, final int Q) {
        this(dynamicsizes(avgQCount,K),K,Q);
    }

    public FieldBloomFilterEncoding(final int[] N, final int K, final int Q) {
        setN(N);
        setK(K);
        setQ(Q);
    }

    public String getName() {
        return "FBF_" + super.getName();
    }

    public String toString() { return "FBF"; }

    @Override
    public void initialize() throws BloomFilterEncodingException {
        for(String name : name2indexMap.keySet()) addFBF(name);
    }

    @Override
    public void setupFromSchema(Schema encodingSchema) throws BloomFilterEncodingException {
        assert N == null && getEncodingSchema() == null && encodingSchema != null;

        String ns = encodingSchema.getNamespace();
        String s = ns.substring("encoding.schema.".length());
        String[] sParts = s.split("\\.");
        assert sParts.length == 3;
        final String method = sParts[0].toUpperCase();
        belongsInAvailableMethods(method);
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
                addToMap(partss[1], i);
                addToMap(partss[1], name);
                i++;
            } else addToMap(name, name);
        }
        setEncodingSchema(encodingSchema);
    }

    public List<Schema.Field> setupSelectedFields(final String[] selectedFieldNames) throws BloomFilterEncodingException {
        assert N != null && N.length != 0 &&
                getK() > 0 && getQ() > 0 &&
                getEncodingSchema() != null;
        Schema.Field[] encodingFields = new Schema.Field[selectedFieldNames.length];
        int i = 0;
        for(String fieldName : selectedFieldNames) {
            String encodingFieldName = String.format("encoding_field_%d_%d_%d_src_%s", getN(i), getK(), getQ(), fieldName);
            encodingFields[i] =new Schema.Field(
                    encodingFieldName, Schema.createFixed(
                    encodingFieldName, null, null,
                    (int) Math.ceil(getN(i)/(double)8)),
                    String.format("Encoding(%s) of field %s", getName(), fieldName), null);
            addToMap(fieldName, i);
            addToMap(fieldName, encodingFieldName);
            i++;
        }
        return Arrays.asList(encodingFields);
    }

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
                final GenericData.Fixed fixed = new GenericData.Fixed(schema, getFBF(name).getByteArray());
                encodingRecord.put(mappedName,fixed);
            }
        }
        return encodingRecord;
    }

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

    protected void addFBF(final String name)
            throws BloomFilterEncodingException{
        try {
            final int index = getIndex(name);
            final int fbfN = getN(index);
            final int K = getK();
            name2FBFMap.put(name, new BloomFilter(fbfN, K));
        } catch (NoSuchAlgorithmException e) {
            throw new BloomFilterEncodingException(e.getMessage());
        } catch (InvalidKeyException e) {
            throw new BloomFilterEncodingException(e.getMessage());
        }
    }

    public BloomFilter getFBF(final String name) {
        if(!name2FBFMap.containsKey(name)) return null;
        return name2FBFMap.get(name);
    }

    protected void addToMap(final String name, final int index) {
        name2indexMap.put(name, index);
    }

    public int getIndex(final String name) {
        if(!name2indexMap.containsKey(name)) return -1;
        return name2indexMap.get(name);
    }

    public static int[] staticsizes(int N,int Ncount) {
        int[] Ns = new int[Ncount];
        Arrays.fill(Ns,N);
        return Ns;
    }

    public static int dynamicsize(double g, int K) {
        return (int) Math.ceil((1 / (1 - Math.pow(0.5, (double) 1 / (g * K)))));
    }

    public static int[] dynamicsizes(double[] g, int K) {
        int Ns[] = new int[g.length];
        for (int i = 0; i < g.length; i++) {
            Ns[i] = dynamicsize(g[i],K);
        }
        return Ns;
    }
}
