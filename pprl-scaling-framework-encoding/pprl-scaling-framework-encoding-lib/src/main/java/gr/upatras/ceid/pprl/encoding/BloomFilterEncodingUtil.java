package gr.upatras.ceid.pprl.encoding;


import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BloomFilterEncodingUtil {

    private static final Logger LOG = LoggerFactory.getLogger(BloomFilterEncodingUtil.class);

    public static final List<String> SCHEME_NAMES = new ArrayList<String>();
    public static final Map<String,Class<?>> SCHEMES = new HashMap<String, Class<?>>();
    static {
        SCHEMES.put("FBF", FieldBloomFilterEncoding.class);
        SCHEMES.put("RBF", RowBloomFilterEncoding.class);
        SCHEMES.put("CLK", CLKEncoding.class);
        SCHEME_NAMES.addAll(SCHEMES.keySet());
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

    public static void schemeNameSupported(final String scheme)
            throws BloomFilterEncodingException {
        if(!SCHEME_NAMES.contains(scheme))
            throw new BloomFilterEncodingException("String \"" + scheme +"\" does not belong in available schemes.");
    }

    public static BloomFilterEncoding newInstance(final Schema schema)
            throws BloomFilterEncodingException {
        String ns = schema.getNamespace();
        if(!ns.startsWith("encoding.schema"))
            throw new BloomFilterEncodingException("Invalid encoding schema.");
        String s = ns.substring("encoding.schema.".length());
        String[] sParts = s.split("\\.");
        assert sParts.length == 3;
        return newInstance(sParts[0].toUpperCase());
    }

    public static BloomFilterEncoding newInstance(final String scheme)
            throws BloomFilterEncodingException {
        schemeNameSupported(scheme);
        try {
            LOG.debug("New Encoding instance is {} .",scheme);
            return (BloomFilterEncoding) (SCHEMES.get(scheme).newInstance());
        } catch (InstantiationException e) {
            throw new BloomFilterEncodingException(e.getMessage());
        } catch (IllegalAccessException e) {
            throw new BloomFilterEncodingException(e.getMessage());
        }
    }

    public static boolean nameBelongsToSchema(final Schema schema, final String[] selectedNames) {
        for(String name : selectedNames) {
            boolean nameFound = false;
            for(Schema.Field field : schema.getFields())
                if(field.name().equals(name)) { nameFound = true ; break; }
            if(!nameFound) return false;
        }
        return true;
    }

    public static BloomFilterEncoding instanceFactory(final String scheme,
                                                      final int fieldCount, final int N,
                                                      final int fbfN, final int K, final int Q,
                                                      final double[] avgQgrams, final double[] weights) {
        final boolean clk = scheme.equals("CLK");
        final boolean fbf = scheme.equals("FBF");
        final boolean rbf = scheme.equals("RBF");

        if(clk && N < 0) throw new IllegalArgumentException("CLK Encoding requires N to be set.");
        if(rbf && (fieldCount < 2)) throw new IllegalArgumentException("RBF Encodings requires at least two fields.");

        final boolean fbfStatic = fbf && (fbfN > 0);
        final boolean fbfDynamic = fbf && (fbfN < 0);
        final boolean rbfUniform =  rbf && (N > 0);
        final boolean rbfWeighted =  rbf && (N < 0);
        final boolean rbfUniformFbfStatic = rbfUniform && (fbfN > 0);
        final boolean rbfUniformFbfDynamic = rbfUniform && (fbfN < 0);
        final boolean rbfWeighetdFbfStatic = rbfWeighted && (fbfN > 0);
        final boolean rbfWeighetdFbfDynamic = rbfWeighted && (fbfN < 0);

        if(clk) {
            LOG.debug("New Encoding instance is CLK.");
            return new CLKEncoding(N,K,Q);
        } else if (fbfStatic) {
            int fbfNs[] = FieldBloomFilterEncoding.staticsizes(fbfN, fieldCount);
            LOG.debug(String.format("New Encoding instance is FBF/Static : [fbfNs=%s,K=%d,Q=%d].",
                    Arrays.toString(fbfNs),K,Q));
            return  new FieldBloomFilterEncoding(fbfNs,K,Q);
        } else if (fbfDynamic) {
            if(containsNan(avgQgrams)) throw new IllegalArgumentException("Avg q grams contains NaN.");
            int fbfNs[] = FieldBloomFilterEncoding.dynamicsizes(avgQgrams, K);
            LOG.debug(String.format("New Encoding instance is FBF/Dynamic : [fbfNs=%s,K=%d,Q=%d].",
                    Arrays.toString(fbfNs),K,Q));
            return  new FieldBloomFilterEncoding(fbfNs,K,Q);
        } else if (rbfUniformFbfStatic) {
            int fbfNs[] = FieldBloomFilterEncoding.staticsizes(fbfN, fieldCount);
            LOG.debug(String.format("New Encoding instance is RBF/Uniform/FBF/Static : [fbfNs=%s,N=%d,K=%d,Q=%d].",
                    Arrays.toString(fbfNs),N,K,Q));
            return  new RowBloomFilterEncoding(fbfNs, N,K,Q);
        } else if (rbfUniformFbfDynamic) {
            if(containsNan(avgQgrams)) throw new IllegalArgumentException("Avg q grams contains NaN.");
            int fbfNs[] = FieldBloomFilterEncoding.dynamicsizes(avgQgrams, K);
            LOG.debug(String.format("New Encoding instance is RBF/Uniform/FBF/Dynamic : [fbfNs=%s,N=%d,K=%d,Q=%d].",
                    Arrays.toString(fbfNs),N,K,Q));
            return  new RowBloomFilterEncoding(fbfNs, N,K,Q);
        } else if (rbfWeighetdFbfStatic) {
            int fbfNs[] = FieldBloomFilterEncoding.staticsizes(fbfN, fieldCount);
            if(containsNan(weights)) throw new IllegalArgumentException("Weights contains NaN.");
            if(!doublesAddUpTo1(weights, 0.0001)) throw new IllegalArgumentException("Weights do not add up to 1.");
            LOG.debug(String.format("New Encoding instance is RBF/Weighted/FBF/Static : [fbfNs=%s,weights=%s,K=%d,Q=%d].",
                    Arrays.toString(fbfNs),Arrays.toString(weights),K,Q));
            return new RowBloomFilterEncoding(fbfNs, weights,K,Q);
        } else if (rbfWeighetdFbfDynamic) {
            if(containsNan(avgQgrams)) throw new IllegalArgumentException("Avg q grams contains NaN.");
            int fbfNs[] = FieldBloomFilterEncoding.dynamicsizes(avgQgrams, K);
            if(containsNan(weights)) throw new IllegalArgumentException("Weights contains NaN.");
            if(!doublesAddUpTo1(weights, 0.0001)) throw new IllegalArgumentException("Weights do not add up to 1.");
            LOG.debug(String.format("New Encoding instance is RBF/Weighted/FBF/Dynamic : [fbfNs=%s,weights=%s,K=%d,Q=%d].",
                    Arrays.toString(fbfNs),Arrays.toString(weights),K,Q));
            return new RowBloomFilterEncoding(fbfNs, weights,K,Q);
        } else throw new IllegalStateException("illegal state.");
    }

    public static boolean containsNan(final double[] doubles) {
        for(double d : doubles) if(Double.isNaN(d)) return true;
        return false;
    }

    public static boolean doublesAddUpTo1(final double[] doubles, double error) {
        double sum = 0;
        for(double d : doubles) sum += d;
        return Math.abs(1.0 - sum) <= error;
    }

    public static List<Schema.Field> setupIncludedFields(final Schema schema, final String[] includedFieldNames) {
        List<Schema.Field> nonSelectedFields = new ArrayList<Schema.Field>();
        for(Schema.Field field : schema.getFields()) {
            if(Arrays.asList(includedFieldNames).contains(field.name())) {
                nonSelectedFields.add(new Schema.Field(field.name(), field.schema(), field.doc(), null));
            }
        }
        return nonSelectedFields;
    }

    public static Schema basedOnExistingSchema(final Schema schema,
                                               final String[] selectedFieldNames, final String[] includedFieldNames,
                                               final Schema existingSchema,
                                               final String[] existingFieldNames)
            throws BloomFilterEncodingException {
        if(!nameBelongsToSchema(schema, selectedFieldNames))
            throw new BloomFilterEncodingException("At least one of the selected for encoding field names " +
                    "does not belong in schema. Selected Field Names " + Arrays.toString(selectedFieldNames));

        if(!nameBelongsToSchema(schema, includedFieldNames))
            throw new BloomFilterEncodingException("At least one of the included field names " +
                    "does not belong in schema. Rest Field Names " + Arrays.toString(includedFieldNames));

        String[] parts = existingSchema.getName().substring("PPRL_Encoding_".length()).split("_",4);
        assert parts.length >= 3;
        final String name = parts[0] + "_" + parts[1] + "_" + parts[2];

        Schema encodingSchema = Schema.createRecord(
                String.format("PPRL_Encoding_%s_%s", name, schema.getName()),
                String.format("PPRL Encoding of %s", schema.getName()),
                String.format("encoding.schema.%s", name.replace("_", ".").toLowerCase()),
                false);

        final List<Schema.Field> restFields = setupIncludedFields(schema, includedFieldNames);
        final List<Schema.Field> encodingFields = new ArrayList<Schema.Field>();

        for (Schema.Field existingField : existingSchema.getFields()) {
            if(existingField.name().startsWith("encoding.field")) {
                String encodingFieldName = existingField.name();
                for (int i = 0; i < existingFieldNames.length; i++) {
                    final String toReplace = "_src_" + existingFieldNames[i];
                    final String replacement = "_src_" + selectedFieldNames[i];
                    encodingFieldName = encodingFieldName.replace(toReplace,replacement);
                }
                encodingFields.add(new Schema.Field(encodingFieldName,
                        existingField.schema(), existingField.doc(), null));
            }
        }

        restFields.addAll(encodingFields);
        encodingSchema.setFields(restFields);
        LOG.debug("Encoding Schema ready :\n-----------------------\n" +
                encodingSchema.toString(true) + "-----------------------\n");
        return encodingSchema;
    }
}
