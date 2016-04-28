package gr.upatras.ceid.pprl.encoding;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * An abstract class for a PPRL Bloom-Filter Encoding scheme.
 */
public abstract class BloomFilterEncoding {

    protected Schema encodingSchema;                                // avro schema for encoding dataset
    protected Map<String,String> name2nameMap = new HashMap<String,String>();   // Mapping between source field name to encoding field name
    protected int[] N;                                              // Array of bloom filter lengths for the encoding
    protected int K;                                                // Number of hash value for each data put in a bloom filter
    protected int Q;                                                // Q as in Q-grams.

    /**
     * Constructor
     */
    public BloomFilterEncoding() {}

    /**
     * Set encoding schema.
     *
     * @param encodingSchema encoding schema.
     */
    public void setEncodingSchema(Schema encodingSchema) {
        this.encodingSchema = encodingSchema;
    }

    /**
     * Returns the number of hash values.
     *
     * @return the number of hash values.
     */
    public int getK() { return K; }

    /**
     * Sets the number of hash values.
     *
     * @param K the number of hash values.
     */
    public void setK(int K) { this.K = K; }

    /**
     * Returns Q as in Q-Grams.
     *
     * @return Q as in Q-Grams.
     */
    public int getQ() { return Q; }

    /**
     * Sets Q as in Q-Grams.
     *
     * @param Q as in Q-Grams.
     */
    public void setQ(int Q) { this.Q = Q; }

    /**
     * Returns sizes of bloom filters.
     *
     * @return sizes of bloom filters.
     */
    public int[] getN() { return N; }

    /**
     * Returns size of the i-th bloom filter.
     *
     * @param i index of i-th bloom filter
     * @return size of the i-th bloom filter.
     */
    public int getN(int i) { return N[i]; }

    /**
     * Set sizes of bloom filters.
     *
     * @param N sizes of bloom filters.
     */
    public void setN(int[] N) { this.N = N;}

    /**
     * Set size of the i-th bloom filter.
     *
     * @param N bloom filter size.
     * @param i index of i-th bloom filter.
     */
    public void setN(int N,int i) { this.N[i] = N; }

    /**
     * Returns encoding schema.
     *
     * @return encoding schema.
     */
    public Schema getEncodingSchema() { return encodingSchema; }

    /**
     * Returns name of encoding like : schemeName_K_Q;
     *
     * @return name of encoding.
     */
    public String getName() {
        return String.format("%s_%d_%d",schemeName(), K, Q);
    }

    /**
     * Add mapping from name to mappedName (usually source fields to encoding fields).
     *
     * @param name name.
     * @param mappedName mappedName.
     */
    protected void addMappedName(final String name, final String mappedName) {
        name2nameMap.put(name, mappedName);
    }

    /**
     * Returns mapped name for name.
     *
     * @param name name.
     * @return mappedName.
     */
    public String getMappedFieldName(final String name) {
        if(!name2nameMap.containsKey(name))
            throw new IllegalArgumentException("Cannot find mapped name for name " + name);
        return name2nameMap.get(name);
    }

    @Override
    public String toString() {
        return schemeName() + "{" +
                "name2nameMap=" + name2nameMap +
                ", N=" + Arrays.toString(N) +
                ", K=" + K +
                ", Q=" + Q;
    }

    /**
     * Returns scheme name.
     *
     * @return scheme name.
     */
    public abstract String schemeName();

    /**
     * Initializes encoding (makes it ready to encode records).
     *
     * @throws BloomFilterEncodingException
     */
    public abstract void initialize() throws BloomFilterEncodingException;

    /**
     * Setup source field (selected field names) to return encoding fields.
     *
     * @param selectedFieldNames selected field names
     * @return encoding fields.
     * @throws BloomFilterEncodingException
     */
    public abstract List<Schema.Field> setupSelectedForEncodingFields(final String[] selectedFieldNames)
            throws BloomFilterEncodingException;

    /**
     * Returns encoded record based on the encoding scheme and input record.
     *
     * @param record input generic record.
     * @return encoded records (generic record).
     * @throws BloomFilterEncodingException
     */
    public abstract GenericRecord encodeRecord(final GenericRecord record)
            throws BloomFilterEncodingException;

    /**
     * Setup an encoding based on existing encoding schema.
     *
     * @param encodingSchema encoding schema.
     * @throws BloomFilterEncodingException
     */
    public abstract void setupFromSchema(final Schema encodingSchema) throws BloomFilterEncodingException;

    /**
     * Setup an encoding based on an input schema, selected fields and included fields.
     *
     * @param schema schema
     * @param selectedFieldNames selected field names.
     * @param includedFieldNames included field names
     * @throws BloomFilterEncodingException
     */
    public void makeFromSchema(final Schema schema,
                               final String[] selectedFieldNames, final String[] includedFieldNames)
            throws BloomFilterEncodingException {

        if(!BloomFilterEncodingUtil.nameBelongsToSchema(schema, selectedFieldNames))
            throw new BloomFilterEncodingException("At least one of the selected for encoding field names " +
                    "does not belong in schema. Selected Field Names " + Arrays.toString(selectedFieldNames));

        if(!BloomFilterEncodingUtil.nameBelongsToSchema(schema, includedFieldNames))
            throw new BloomFilterEncodingException("At least one of the included field names " +
                    "does not belong in schema. Rest Field Names " + Arrays.toString(includedFieldNames));
        Schema encodingSchema = Schema.createRecord(
                String.format("PPRL_Encoding_%s_%s", getName(), schema.getName()),
                String.format("PPRL Encoding of %s", schema.getName()),
                String.format("encoding.schema.%s", getName().replace("_", ".").toLowerCase()),
                false);

        final List<Schema.Field> restFields = BloomFilterEncodingUtil.setupIncludedFields(schema,
                includedFieldNames);
        for (String fieldName : includedFieldNames)
            addMappedName(fieldName,fieldName);
        final List<Schema.Field> encodingFields = setupSelectedForEncodingFields(selectedFieldNames);
        restFields.addAll(encodingFields);
        encodingSchema.setFields(restFields);
        setEncodingSchema(encodingSchema);
    }

    /**
     * Returns true if this encoding is based on the input schema, false otherwise.
     *
     * @param schema input schema.
     * @return true if this encoding is based on the input schema, false otherwise.
     * @throws BloomFilterEncodingException
     */
    public boolean isEncodingOfSchema(final Schema schema)
            throws BloomFilterEncodingException {
        assert getEncodingSchema() != null;

        boolean isSchemaValid = getEncodingSchema().getName().contains("PPRL_Encoding") &&
                getEncodingSchema().getName().endsWith(schema.getName()) &&
                getEncodingSchema().getNamespace().startsWith("encoding.schema");

        boolean areFieldsValid = true;
        for(Schema.Field encodingField : getEncodingSchema().getFields()) {
            boolean isFieldNotSelected = schema.getFields().contains(encodingField);
            if(isFieldNotSelected) continue;

            boolean isFieldSelected = false;
            for(Schema.Field field : schema.getFields())
                if(encodingField.name().contains(field.name())) { isFieldSelected = true ; break; }
            areFieldsValid = isFieldSelected;
        }
        return isSchemaValid && areFieldsValid;
    }
}
