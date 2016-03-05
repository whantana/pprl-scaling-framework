package gr.upatras.ceid.pprl.encoding;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class BloomFilterEncoding {

    protected Schema encodingSchema;
    protected Map<String,String> name2nameMap = new HashMap<String,String>();
    protected int[] N;
    protected int K;
    protected int Q;

    public BloomFilterEncoding() {}

    public void setEncodingSchema(Schema encodingSchema) {
        this.encodingSchema = encodingSchema;
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

    public Schema getEncodingSchema() {
        return encodingSchema;
    }

    public String getName() {
        return String.format("%d_%d", K, Q);
    }

    protected void addToMap(String name, String mappedName) {
        name2nameMap.put(name, mappedName);
    }

    public String getFieldName(String name) {
        if(!name2nameMap.containsKey(name)) return null;
        return name2nameMap.get(name);
    }

    public String getFullName() {
        if(encodingSchema !=null) {
            StringBuilder sb = new StringBuilder(getName());
            for(Schema.Field field : encodingSchema.getFields()) {
                if(field.name().startsWith("encoding_field_")) {
                    String[] fieldNameParts = field.name().split(("_src_"));
                    for (int i = 1; i < fieldNameParts.length ; i++)
                        sb.append("_").append((fieldNameParts[fieldNameParts.length - 1]));
                }
            }
            return sb.toString();
        } else
            return "(encoding schema not set)";
    }

    public abstract String toString();

    public abstract void initialize() throws BloomFilterEncodingException;

    public abstract List<Schema.Field> setupSelectedFields(final String[] selectedFieldNames)
            throws BloomFilterEncodingException;

    public abstract GenericRecord encodeRecord(final GenericRecord record)
            throws BloomFilterEncodingException;

    public abstract void setupFromSchema(final Schema encodingSchema) throws BloomFilterEncodingException;

    public void makeFromSchema(final Schema schema,
                               final String[] selectedFieldNames, final String[] restFieldNames)
            throws BloomFilterEncodingException {

        if(!areSelectedNamesInSchema(schema,selectedFieldNames))
            throw new BloomFilterEncodingException("At least one of the selected field names " +
                    "does not belong in schema. Selected Field Names " + Arrays.toString(selectedFieldNames));

        if(!areSelectedNamesInSchema(schema,restFieldNames))
            throw new BloomFilterEncodingException("At least one of the rest field names " +
                    "does not belong in schema. Rest Field Names " + Arrays.toString(restFieldNames));
        Schema encodingSchema = Schema.createRecord(
                String.format("PPRL_Encoding_%s_%s", getName(), schema.getName()),
                String.format("PPRL Encoding of %s", schema.getName()),
                String.format("encoding.schema.%s", getName().replace("_", ".").toLowerCase()),
                false);

        final List<Schema.Field> restFields = setupRestFields(schema, restFieldNames);
        final List<Schema.Field> encodingFields = setupSelectedFields(selectedFieldNames);
        restFields.addAll(encodingFields);
        encodingSchema.setFields(restFields);

        setEncodingSchema(encodingSchema);
    }

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

    private boolean areSelectedNamesInSchema(final Schema schema, final String[] selectedNames) {
        for(String name : selectedNames) {
            boolean nameFound = false;
            for(Schema.Field field : schema.getFields())
                if(field.name().equals(name)) { nameFound = true ; break; }
            if(!nameFound) return false;
        }
        return true;
    }


    private List<Schema.Field> setupRestFields(final Schema schema, final String[] restFieldNames)
            throws BloomFilterEncodingException {
        List<Schema.Field> nonSelectedFields = new ArrayList<Schema.Field>();
        for(Schema.Field field : schema.getFields()) {
            if(Arrays.asList(restFieldNames).contains(field.name())) {
                nonSelectedFields.add(new Schema.Field(field.name(), field.schema(), field.doc(), null));
                addToMap(field.name(), field.name());
            }
        }
        return nonSelectedFields;
    }
}
