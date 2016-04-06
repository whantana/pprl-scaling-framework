package gr.upatras.ceid.pprl.encoding;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Bloom Filter Encoding utility class.
 */
public class BloomFilterEncodingUtil {

    private static final Logger LOG = LoggerFactory.getLogger(BloomFilterEncodingUtil.class);

    public static final List<String> SCHEME_NAMES = new ArrayList<String>(); // Available Encoding Schemes
    public static final Map<String,Class<?>> SCHEMES = new HashMap<String, Class<?>>();
    static {
        SCHEMES.put("FBF", FieldBloomFilterEncoding.class);
        SCHEMES.put("RBF", RowBloomFilterEncoding.class);
        SCHEMES.put("CLK", CLKEncoding.class);
        SCHEME_NAMES.addAll(SCHEMES.keySet());
    }

    public static final List<Schema.Type> SUPPORTED_TYPES = new ArrayList<Schema.Type>(); // Supported field types.
    static {
        SUPPORTED_TYPES.add(Schema.Type.INT);
        SUPPORTED_TYPES.add(Schema.Type.LONG);
        SUPPORTED_TYPES.add(Schema.Type.FLOAT);
        SUPPORTED_TYPES.add(Schema.Type.DOUBLE);
        SUPPORTED_TYPES.add(Schema.Type.BOOLEAN);
        SUPPORTED_TYPES.add(Schema.Type.STRING);
    }

    /**
     * Does nothing if scheme name is supported, throws exception othewise.
     *
     * @param scheme encoding scheme name.
     * @throws BloomFilterEncodingException
     */
    public static void schemeNameSupported(final String scheme)
            throws BloomFilterEncodingException {
        if(!SCHEME_NAMES.contains(scheme))
            throw new BloomFilterEncodingException("String \"" + scheme +"\" does not belong in available schemes.");
    }

    /**
     * Retrieves and returns scheme name from an encoded dataset schema.
     *
     * @param schema encoded dataset schema.
     * @return encoding schema name.
     * @throws BloomFilterEncodingException
     */
    public static String retrieveSchemeName(final Schema schema)
            throws BloomFilterEncodingException {
        String ns = schema.getNamespace();
        if(!ns.startsWith("encoding.schema"))
            throw new BloomFilterEncodingException("Invalid encoding schema.");
        String s = ns.substring("encoding.schema.".length());
        String[] sParts = s.split("\\.");
        assert sParts.length == 3;
        return sParts[0].toUpperCase();
    }

    /**
     * Creates new instance of an encoding based on an encoded dataset schema.
     * This instance is uninitialized and not setup fully.
     *
     * @param schema encoded dataset schema.
     * @return instance of the encoding
     * @throws BloomFilterEncodingException
     */
    public static BloomFilterEncoding newInstance(final Schema schema)
            throws BloomFilterEncodingException {
        return newInstance(retrieveSchemeName(schema));
    }

    /**
     * Creates new instance of an encoding based on an encoding scheme name.
     * This instance is uninitialized and not setup fully.
     *
     * @param scheme encoding scheme name.
     * @return instance of the encoding
     * @throws BloomFilterEncodingException
     */
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

    /**
     * Returns true if all field names are members (fields) of the input schema. Retursn false otherwise.
     *
     * @param schema  input avro schema.
     * @param fieldNames field names.
     * @return true if all field names are members, false otherwise.
     */
    public static boolean nameBelongsToSchema(final Schema schema, final String... fieldNames) {
        for(String name : fieldNames) {
            boolean nameFound = false;
            for(Schema.Field field : schema.getFields())
                if(field.name().equals(name)) { nameFound = true ; break; }
            if(!nameFound) return false;
        }
        return true;
    }

    /**
     * Instance factory method. New instances now can be one of the folowing:
     * <ul>
     *     <li>CLK</li>
     *     <li>FBF/Static</li>
     *     <li>FBF/Dynamic</li>
     *     <li>RBF/Uniform/FBF/Static</li>
     *     <li>RBF/Uniform/FBF/Dynamic</li>
     *     <li>RBF/Weighted/FBF/Static</li>
     *     <li>RBF/Weighted/FBF/Dynamic</li>
     * </ul>
     * But still require an input avro schema to complete and initialize.
     *
     *
     * @param scheme scheme name.
     * @param fieldCount field count.
     * @param N bloom filter size (for encoding RBF/Uniform and CLK).
     * @param fbfN bloom filter size for Field Bloom Filters (for encoding FBF/Static)
     * @param K number of hash values.
     * @param Q Q as in Q-Grams.
     * @param avgQgrams avg q-gram count for each field to be encoded.
     * @param weights weights that each FBF should be sampled for RBF encoding.
     * @return new instance.
     */
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

    /**
     * Return true if the input array contains a NaN, false otherwise.
     *
     * @param doubles input double array.
     * @return true if the input array contains a NaN, false otherwise.
     */
    public static boolean containsNan(final double[] doubles) {
        for(double d : doubles) if(Double.isNaN(d)) return true;
        return false;
    }

    /**
     * Returns true if the input array elements add up to 1 (within a small error), false otherwise.
     *
     * @param doubles input double array.
     * @param error error.
     * @return true if the input array elements add up to 1, false othwerwise.
     */
    public static boolean doublesAddUpTo1(final double[] doubles, double error) {
        double sum = 0;
        for(double d : doubles) sum += d;
        return Math.abs(1.0 - sum) <= error;
    }

    /**
     * Returns a list of the included fields based on the included field names.
     *
     * @param schema avro schema of a dataset.
     * @param includedFieldNames included field names.
     * @return  a list of the included fields.
     */
    public static List<Schema.Field> setupIncludedFields(final Schema schema, final String[] includedFieldNames) {
        List<Schema.Field> nonSelectedFields = new ArrayList<Schema.Field>();
        for(Schema.Field field : schema.getFields()) {
            if(Arrays.asList(includedFieldNames).contains(field.name())) {
                nonSelectedFields.add(new Schema.Field(field.name(), field.schema(), field.doc(), null));
            }
        }
        return nonSelectedFields;
    }

    /**
     * Returns an encoding schema based on an existing schema, with the respected encoding fields changed
     * according to the user's selection.
     *
     * @param schema input avro schema.
     * @param selectedFieldNames selected field names.
     * @param includedFieldNames included field names.
     * @param existingSchema existing encoding schema.
     * @param existingFieldNames existing encoding field names.
     * @return an encoding schema based on an existing schema.
     * @throws BloomFilterEncodingException
     */
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
