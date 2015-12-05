package gr.upatras.ceid.pprl.encoding;

import org.apache.avro.Schema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class BloomFilterEncoding {
    protected Schema encodingSchema;
    protected Schema.Field[] encodingFields;
    protected int[] N;
    protected BloomFilter[] bloomFilters;
    protected int K;
    protected int Q;

    public BloomFilterEncoding() {
        encodingSchema = null;
        encodingFields = null;
        N = null;
    }

    public BloomFilterEncoding(double[] avgQgram, int K, int Q) {
        this.K = K;
        this.Q = Q;
        this.N = new int[avgQgram.length];
        for (int i = 0; i < avgQgram.length; i++)
            N[i] = dynamicsize(avgQgram[i],K);
    }

    public BloomFilterEncoding(Schema encodingSchema, Schema.Field[] encodingFields, double[] avgQgram, int K, int Q)
            throws BloomFilterEncodingException {
        this(avgQgram,K,Q);
        if(avgQgram.length != encodingFields.length)
            throw new BloomFilterEncodingException("avgQgram.length " +
                    "must aggre with encodingFields.length");
        this.encodingSchema = encodingSchema;
        this.encodingFields = encodingFields;
        for (int i = 0; i < N.length; i++) bloomFilters[i] = new BloomFilter(N[i],K);
    }

    public BloomFilterEncoding(int N, int K, int Q) {
        this.K = K;
        this.Q = Q;
        this.N = new int[]{N};
    }

    public BloomFilterEncoding(Schema encodingSchema, Schema.Field[] encodingFields, int N, int K, int Q) {
        this(N,K,Q);
        this.encodingSchema = encodingSchema;
        this.encodingFields = encodingFields;
        bloomFilters = new BloomFilter[encodingFields.length];
        for (int i = 0; i < encodingFields.length; i++) bloomFilters[i] = new BloomFilter(N,K);
    }

    public void setEncodingSchema(Schema encodingSchema) {
        this.encodingSchema = encodingSchema;
    }

    public Schema.Field[] getEncodingFields() {
        return encodingFields;
    }

    public void setEncodingFields(Schema.Field[] encodingFields) {
        this.encodingFields = encodingFields;
    }

    public int[] getN() {
        return N;
    }

    public int getN(int i) {
        return N[i];
    }

    public void setN(int[] n) {
        N = n;
    }

    public void setN(int n,int i) {
        N[i] = n;
    }

    public int getK() {
        return K;
    }

    public void setK(int k) {
        K = k;
    }

    public int getQ() {
        return Q;
    }

    public void setQ(int q) {
        Q = q;
    }

    public Schema getEncodingSchema() {
        return encodingSchema;
    }

    public BloomFilter[] getBloomFilters() {
        return bloomFilters;
    }

    public void setBloomFilters(BloomFilter[] bloomFilters) {
        this.bloomFilters = bloomFilters;
    }

    public String getName() {
        return hasMultiN() ? String.format("%d_%d",K,Q) : String.format("%d_%d_%d",N[0],K,Q);
    }

    protected boolean hasMultiN() {
        return N.length > 1;
    }

    protected boolean hasSingleN() {
        return N.length == 1;
    }

    public void makeFromSchema(final Schema schema,
                               final String[] selectedFieldNames)
            throws BloomFilterEncodingException {
        makeFromSchema(schema,selectedFieldNames,new String[0]);
    }

    public void makeFromSchema(final Schema schema,
                               final String[] selectedFieldNames, final String[] restFieldNames)
            throws BloomFilterEncodingException {

        if(!areSelectedNamesInSchema(schema,selectedFieldNames))
            throw new BloomFilterEncodingException("At least one of the selected field names " +
                    "does not belong in schema. Selected Field Names " + Arrays.toString(selectedFieldNames));

        setEncodingSchema(Schema.createRecord(
                String.format("Encoding_Schema_%s_%s", getName(), schema.getName()),
                String.format("PPRL Encoding of schema with name %s", schema.getName()),
                String.format("encoding.schema.%s",getName().replace("_", ".").toLowerCase()),
                false));

        Schema.Field[] encodingFields = new Schema.Field[selectedFieldNames.length];
        int i = 0;
        for(String fieldName : selectedFieldNames) {
            String encodingFieldName = getEncodingFieldName(hasMultiN()?N[i]:N[0],K,Q,fieldName);
            encodingFields[i] =new Schema.Field(
                    encodingFieldName, Schema.createFixed(
                    encodingFieldName, null, null,
                    (int) Math.ceil(getN(hasMultiN() ? i : 0)/(double)8)),
                    String.format("Encoding(%s) of field %s", getName(), fieldName), null);
            i++;
        }
        setEncodingFields(encodingFields);

        List<Schema.Field> finalFieldList = getNonSelectedFields(schema,selectedFieldNames,restFieldNames);
        finalFieldList.addAll(Arrays.asList(encodingFields));
        encodingSchema.setFields(finalFieldList);
    }

    public boolean isEncodingOfSchema(final Schema schema)
            throws BloomFilterEncodingException {

        if(encodingSchema == null || encodingFields == null || encodingFields.length == 0)
            throw new BloomFilterEncodingException("Not properly initialized encoding.");

        boolean isSchemaValid = encodingSchema.getName().contains("Encoding_Schema") &&
                encodingSchema.getName().endsWith(schema.getName()) &&
                encodingSchema.getNamespace().startsWith("encoding.schema");

        boolean areFieldsValid = true;
        for(Schema.Field encodingField : encodingSchema.getFields()) {
            boolean isFieldNotSelected = schema.getFields().contains(encodingField);
            if(isFieldNotSelected) continue;

            boolean isFieldSelected = false;
            for(Schema.Field field : schema.getFields())
                if(encodingField.name().endsWith(field.name())) { isFieldSelected = true ; break; }
            areFieldsValid = isFieldSelected;
        }

        return isSchemaValid && areFieldsValid;
    }

    public void makeFromSchema(final Schema encodingSchema)
            throws BloomFilterEncodingException {
        String ns = encodingSchema.getNamespace();
        String s = ns.substring("encoding.schema.".length());
        String[] sParts = s.split("\\.");
        switch(sParts.length) {
            case 2:
                K = Integer.valueOf(sParts[0]);
                Q = Integer.valueOf(sParts[1]);
                break;
            case 3:
                if (!sParts[0].matches("\\p{Lower}+"))
                    N = new int[]{Integer.valueOf(sParts[0])};
                K = Integer.valueOf(sParts[1]);
                Q = Integer.valueOf(sParts[2]);
                break;
            case 4:
                N = new int[]{Integer.valueOf(sParts[1])};
                K = Integer.valueOf(sParts[2]);
                Q = Integer.valueOf(sParts[3]);
                break;
            default:
                throw new BloomFilterEncodingException("Cannot read from schema. Namespace String : " + ns + " .");
        }
        setEncodingSchema(encodingSchema);

        ArrayList<Schema.Field> efList = new ArrayList<Schema.Field>();
        for(Schema.Field field : encodingSchema.getFields())
            if(field.name().startsWith("encoding_field_")) efList.add(field);
        setEncodingFields(efList.toArray(new Schema.Field[efList.size()]));

        if(N != null) return;
        N = new int[encodingFields.length];
        int i = 0;
        for(Schema.Field field : encodingFields) {
            String restName = field.name().substring("encoding_field_".length());
            String[] parts = restName.split("_");
            if(parts.length != 4 && parts.length != 3)
                throw new BloomFilterEncodingException("Parts must be 4 but they are " + parts.length +".");
            N[i] = Integer.parseInt(parts[0]);
            if(K != Integer.parseInt(parts[1]))
                throw new BloomFilterEncodingException("K :" + K + " in schema does not agree with " +
                        "schema in field. K found : " + Integer.parseInt(parts[1]) +".");
            if(Q != Integer.parseInt(parts[2]))
                throw new BloomFilterEncodingException("Q :" + Q + " in schema does not agree with " +
                        "schema in field. Q found : " + Integer.parseInt(parts[2]) +".");
            i++;
        }
    }

    public static String getEncodingFieldName(final int N, final int K, final int Q,final String name) {
        return String.format("encoding_field_%d_%d_%d_%s",N,K,Q,name);
    }

    private static boolean areSelectedNamesInSchema(final Schema schema, final String[] selectedNames) {
        for(String name : selectedNames) {
            boolean nameFound = false;
            for(Schema.Field field : schema.getFields())
                if(field.name().equals(name)) { nameFound = true ; break; }
            if(!nameFound) return false;
        }
        return true;
    }

    public static List<Schema.Field> getNonSelectedFields(final Schema schema, final String[] selectedNames)
            throws BloomFilterEncodingException {
        return getNonSelectedFields(schema,selectedNames,new String[0]);
    }

    public static List<Schema.Field> getNonSelectedFields(final Schema schema, final String[] selectedNames,
                                                           final String[] names)
            throws BloomFilterEncodingException {
        if(!areSelectedNamesInSchema(schema,selectedNames))
            throw new BloomFilterEncodingException("At least one of the selected field names " +
                    "does not belong in schema. Selected Field Names " + Arrays.toString(selectedNames));

        List<Schema.Field> nonSelectedFields = new ArrayList<Schema.Field>();
        for(Schema.Field field : schema.getFields()) {
            if(Arrays.asList(selectedNames).contains(field.name())) continue;
            if(names != null && names.length != 0 && !Arrays.asList(names).contains(field.name())) continue;
            nonSelectedFields.add(new Schema.Field(field.name(), field.schema(), field.doc(), null));
        }
        return nonSelectedFields;
    }

    public static int dynamicsize(double g, int K) {
        return (int) Math.ceil((1 / (1 - Math.pow(0.5, (double) 1 / (g * K)))));
    }
}
