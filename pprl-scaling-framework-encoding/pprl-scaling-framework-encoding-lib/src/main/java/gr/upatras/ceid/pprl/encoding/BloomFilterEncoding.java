package gr.upatras.ceid.pprl.encoding;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class BloomFilterEncoding {

    private static final Logger LOG = LoggerFactory.getLogger(BloomFilterEncoding.class);

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
        return String.format("%s_%d_%d",schemeName(), K, Q);
    }

    protected void addMappedName(final String name, final String mappedName) {
        LOG.debug("Mapping name {} -to-> {}",name,mappedName);
        name2nameMap.put(name, mappedName);
    }

    public String getMappedFieldName(final String name) {
        if(!name2nameMap.containsKey(name))
            throw new IllegalArgumentException("Cannot find mapped name for name " + name);
        return name2nameMap.get(name);
    }

//    public String getFullName() {
//        if(encodingSchema !=null) {
//            StringBuilder sb = new StringBuilder(getName());
//            for(Schema.Field field : encodingSchema.getFields()) {
//                if(field.name().startsWith("encoding_field_")) {
//                    String[] fieldNameParts = field.name().split(("_src_"));
//                    for (int i = 1; i < fieldNameParts.length ; i++)
//                        sb.append("_").append((fieldNameParts[fieldNameParts.length - 1]));
//                }
//            }
//            return sb.toString();
//        } else
//            return "(encoding schema not set)";
//    }


    @Override
    public String toString() {
        return schemeName() + "{" +
                "name2nameMap=" + name2nameMap +
                ", N=" + Arrays.toString(N) +
                ", K=" + K +
                ", Q=" + Q;
    }

    public abstract String schemeName();

    public abstract void initialize() throws BloomFilterEncodingException;

    public abstract List<Schema.Field> setupSelectedForEncodingFields(final String[] selectedFieldNames)
            throws BloomFilterEncodingException;

    public abstract GenericRecord encodeRecord(final GenericRecord record)
            throws BloomFilterEncodingException;

    public abstract void setupFromSchema(final Schema encodingSchema) throws BloomFilterEncodingException;

    public void makeFromSchema(final Schema schema,
                               final String[] selectedFieldNames, final String[] includedtFieldNames)
            throws BloomFilterEncodingException {

        if(!BloomFilterEncodingUtil.nameBelongsToSchema(schema, selectedFieldNames))
            throw new BloomFilterEncodingException("At least one of the selected for encoding field names " +
                    "does not belong in schema. Selected Field Names " + Arrays.toString(selectedFieldNames));

        if(!BloomFilterEncodingUtil.nameBelongsToSchema(schema, includedtFieldNames))
            throw new BloomFilterEncodingException("At least one of the included field names " +
                    "does not belong in schema. Rest Field Names " + Arrays.toString(includedtFieldNames));
        Schema encodingSchema = Schema.createRecord(
                String.format("PPRL_Encoding_%s_%s", getName(), schema.getName()),
                String.format("PPRL Encoding of %s", schema.getName()),
                String.format("encoding.schema.%s", getName().replace("_", ".").toLowerCase()),
                false);

        final List<Schema.Field> restFields = setupIncludedFields(schema, includedtFieldNames);
        final List<Schema.Field> encodingFields = setupSelectedForEncodingFields(selectedFieldNames);
        restFields.addAll(encodingFields);
        encodingSchema.setFields(restFields);
        LOG.debug("Encoding Schema ready :\n-----------------------\n" +
                  encodingSchema.toString(true) + "-----------------------\n");
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
        LOG.debug("isSchemaValid={} && areFieldsValid={}",isSchemaValid,areFieldsValid);
        return isSchemaValid && areFieldsValid;
    }


    private List<Schema.Field> setupIncludedFields(final Schema schema, final String[] includedFieldNames)
            throws BloomFilterEncodingException {
        List<Schema.Field> nonSelectedFields = new ArrayList<Schema.Field>();
        for(Schema.Field field : schema.getFields()) {
            if(Arrays.asList(includedFieldNames).contains(field.name())) {
                nonSelectedFields.add(new Schema.Field(field.name(), field.schema(), field.doc(), null));
                addMappedName(field.name(), field.name());
            }
        }
        return nonSelectedFields;
    }
}
