package gr.upatras.ceid.pprl.encoding;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class BloomFilterEncoding {

    public static final List<String> AVAILABLE_METHODS = new ArrayList<String>();
    public static final Map<String,Class<?>> AVAILABLE_METHODS_MAP = new HashMap<String, Class<?>>();
    static {
        AVAILABLE_METHODS_MAP.put("FBF", FieldBloomFilterEncoding.class);
        AVAILABLE_METHODS_MAP.put("RBF", RowBloomFilterEncoding.class);
        AVAILABLE_METHODS.addAll(AVAILABLE_METHODS_MAP.keySet());
    }

    public static final List<Schema.Type> SUPPORTED_TYPES = new ArrayList<Schema.Type>();
    static {
        SUPPORTED_TYPES.add(Schema.Type.INT);
        SUPPORTED_TYPES.add(Schema.Type.LONG);
        SUPPORTED_TYPES.add(Schema.Type.FLOAT);
        SUPPORTED_TYPES.add(Schema.Type.DOUBLE);
        SUPPORTED_TYPES.add(Schema.Type.BOOLEAN);
        SUPPORTED_TYPES.add(Schema.Type.STRING);
    }

    protected Schema encodingSchema;
    protected int[] N;
    protected int K;
    protected int Q;

    public BloomFilterEncoding() {}

    public void setEncodingSchema(Schema encodingSchema) {
        this.encodingSchema = encodingSchema;
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

    public void setN(double[] avgQgrams) {
        setN(new int[avgQgrams.length]);
        for (int i = 0; i < avgQgrams.length; i++)
            setN(dynamicsize(avgQgrams[i], K), i);
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

    public String getName() {
        return hasMultiN() ? String.format("%d_%d",K,Q) : String.format("%d_%d_%d",N[0],K,Q);
    }

    public abstract String toString();

    public abstract GenericData.Fixed encode(Object obj, final Schema.Type type,
                                             final Schema.Field field, final int N, final int K, final int Q)
            throws BloomFilterEncodingException;

    public boolean isValid() {
        return !isInvalid();
    }

    public boolean isInvalid() {
        return N==null | K <= 0 | Q <= 0 | encodingSchema == null;

    }

    public boolean hasMultiN() {
        return N.length > 1;
    }

    public boolean hasSingleN() {
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

        if(!areSelectedNamesInSchema(schema,restFieldNames))
            throw new BloomFilterEncodingException("At least one of the rest field names " +
                    "does not belong in schema. Rest Field Names " + Arrays.toString(restFieldNames));

        setEncodingSchema(Schema.createRecord(
                String.format("PPRL_Encoding_%s_%s", getName(), schema.getName()),
                String.format("PPRL Encoding of %s", schema.getName()),
                String.format("encoding.schema.%s",getName().replace("_", ".").toLowerCase()),
                false));

        Schema.Field[] encodingFields = new Schema.Field[selectedFieldNames.length];
        int i = 0;
        for(String fieldName : selectedFieldNames) {
            String encodingFieldName = getEncodingFieldName(getN(hasMultiN()?i:0),getK(),getQ(),fieldName);
            encodingFields[i] =new Schema.Field(
                    encodingFieldName, Schema.createFixed(
                    encodingFieldName, null, null,
                    (int) Math.ceil(getN(hasMultiN() ? i : 0)/(double)8)),
                    String.format("Encoding(%s) of field %s", getName(), fieldName), null);
            i++;
        }

        List<Schema.Field> finalFieldList = getNonSelectedFields(schema,restFieldNames);
        finalFieldList.addAll(Arrays.asList(encodingFields));
        encodingSchema.setFields(finalFieldList);
    }

    public boolean isEncodingOfSchema(final Schema schema)
            throws BloomFilterEncodingException {

        if(encodingSchema == null)
            throw new BloomFilterEncodingException("Not properly initialized encoding.");

        boolean isSchemaValid = encodingSchema.getName().contains("PPRL_Encoding") &&
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
                setK(Integer.valueOf(sParts[0]));
                setQ(Integer.valueOf(sParts[1]));
                break;
            case 3:
                if (!sParts[0].matches("\\p{Lower}+"))
                    setN(new int[]{Integer.valueOf(sParts[0])});
                setK(Integer.valueOf(sParts[1]));
                setQ(Integer.valueOf(sParts[2]));
                break;
            case 4:
                setN(new int[]{Integer.valueOf(sParts[1])});
                setK(Integer.valueOf(sParts[2]));
                setQ(Integer.valueOf(sParts[3]));
                break;
            default:
                throw new BloomFilterEncodingException("Cannot read from schema. Namespace String : " + ns + " .");
        }
        setEncodingSchema(encodingSchema);

        List<Schema.Field> encodingFieldsList = new ArrayList<Schema.Field>();
        for(Schema.Field field : encodingSchema.getFields())
            if(field.name().startsWith("encoding_field_")) encodingFieldsList.add(field);

        if(N != null) return;
        setN(new int[encodingFieldsList.size()]);
        int i = 0;
        for(Schema.Field field : encodingFieldsList) {
            String restName = field.name().substring("encoding_field_".length());
            String[] parts = restName.split("_");
            if(parts.length != 4 && parts.length != 3)
                throw new BloomFilterEncodingException("Parts must be 4 but they are " + parts.length +".");
            setN(Integer.parseInt(parts[0]),i);
            if(getK() != Integer.parseInt(parts[1]))
                throw new BloomFilterEncodingException("K :" + getK() + " in schema does not agree with " +
                        "schema in field. K found : " + Integer.parseInt(parts[1]) +".");
            if(getQ() != Integer.parseInt(parts[2]))
                throw new BloomFilterEncodingException("Q :" + getQ() + " in schema does not agree with " +
                        "schema in field. Q found : " + Integer.parseInt(parts[2]) +".");
            i++;
        }
    }

    public Schema.Field getEncodingField(final int N, final int K, final int Q,final String name) {
        return encodingSchema.getField(getEncodingFieldName(N,K,Q,name));
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


    public static List<Schema.Field> getNonSelectedFields(final Schema schema, final String[] restFieldNames)
            throws BloomFilterEncodingException {
        List<Schema.Field> nonSelectedFields = new ArrayList<Schema.Field>();
        for(Schema.Field field : schema.getFields()) {
            if(Arrays.asList(restFieldNames).contains(field.name()))
                nonSelectedFields.add(new Schema.Field(field.name(), field.schema(), field.doc(), null));
        }
        return nonSelectedFields;
    }

    public static int dynamicsize(double g, int K) {
        return (int) Math.ceil((1 / (1 - Math.pow(0.5, (double) 1 / (g * K)))));
    }

    public static void belongsInAvailableMethods(final String methodName)
            throws BloomFilterEncodingException {
        if(!AVAILABLE_METHODS.contains(methodName))
            throw new BloomFilterEncodingException("String \"" + methodName +"\" does not belong in available methods.");
    }

    public static BloomFilterEncoding newInstanceOfMethod(final String methodName)
            throws BloomFilterEncodingException {
        belongsInAvailableMethods(methodName);
        try {
            return (BloomFilterEncoding) (AVAILABLE_METHODS_MAP.get(methodName).newInstance());
        } catch (InstantiationException e) {
            throw new BloomFilterEncodingException(e.getMessage());
        } catch (IllegalAccessException e) {
            throw new BloomFilterEncodingException(e.getMessage());
        }
    }

    public static BloomFilterEncoding newInstanceOfMethod(final String methodName, final int N, int K, int Q)
            throws BloomFilterEncodingException {
        belongsInAvailableMethods(methodName);
        try {
            BloomFilterEncoding encoding =
                    (BloomFilterEncoding)  (AVAILABLE_METHODS_MAP.get(methodName).newInstance());
            encoding.setN(new int[1]);
            encoding.setN(N,0);
            encoding.setK(K);
            encoding.setQ(Q);
            return encoding;
        } catch (InstantiationException e) {
            throw new BloomFilterEncodingException(e.getMessage());
        } catch (IllegalAccessException e) {
            throw new BloomFilterEncodingException(e.getMessage());
        }
    }

    public static BloomFilterEncoding newInstanceOfMethod(final String methodName, final double[] avgQgrams, int K, int Q)
            throws BloomFilterEncodingException {
        belongsInAvailableMethods(methodName);
        try {
            BloomFilterEncoding encoding =
                    (BloomFilterEncoding)  (AVAILABLE_METHODS_MAP.get(methodName).newInstance());
            encoding.setN(avgQgrams);
            encoding.setK(K);
            encoding.setQ(Q);
            return encoding;
        } catch (InstantiationException e) {
            throw new BloomFilterEncodingException(e.getMessage());
        } catch (IllegalAccessException e) {
            throw new BloomFilterEncodingException(e.getMessage());
        }
    }

    public static BloomFilterEncoding newInstanceOfMethod(final String methodName, final int[] N, int K, int Q)
            throws BloomFilterEncodingException {
        belongsInAvailableMethods(methodName);
        try {
            BloomFilterEncoding encoding =
                    (BloomFilterEncoding)  (AVAILABLE_METHODS_MAP.get(methodName).newInstance());
            encoding.setN(N);
            encoding.setK(K);
            encoding.setQ(Q);
            return encoding;
        } catch (InstantiationException e) {
            throw new BloomFilterEncodingException(e.getMessage());
        } catch (IllegalAccessException e) {
            throw new BloomFilterEncodingException(e.getMessage());
        }
    }

    public static BloomFilterEncoding fromString(final String s)
            throws BloomFilterEncodingException{
        belongsInAvailableMethods(s);
        return newInstanceOfMethod(s);
    }


    public static GenericRecord encodeRecord(final GenericRecord record,
                                             final BloomFilterEncoding encoding,
                                             final Schema schema,
                                             final String[] selectedFieldNames, final String[] restFieldNames,
                                             final int[] N, final int K, final int Q)
            throws BloomFilterEncodingException, UnsupportedEncodingException {

        final GenericRecord encodingRecord = new GenericData.Record(encoding.getEncodingSchema());

        for (int i = 0; i < selectedFieldNames.length; i++) {
            String fieldName = selectedFieldNames[i];
            final Object obj = record.get(fieldName);
            final Schema.Type type = schema.getField(fieldName).schema().getType();
            final Schema.Field field = encoding.getEncodingField(N[i], K, Q, fieldName);
            encodingRecord.put(field.name(),encoding.encode(obj,type,field,N[i],K,Q));
        }

        for (String fieldName : restFieldNames) encodingRecord.put(fieldName,record.get(fieldName));
        return encodingRecord;
    }
}
