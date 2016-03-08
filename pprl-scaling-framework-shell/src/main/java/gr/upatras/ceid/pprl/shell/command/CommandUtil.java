package gr.upatras.ceid.pprl.shell.command;

import gr.upatras.ceid.pprl.datasets.DatasetFieldStatistics;
import gr.upatras.ceid.pprl.datasets.DatasetStatistics;
import gr.upatras.ceid.pprl.encoding.FieldBloomFilterEncoding;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CommandUtil {

    public static String retrieveString(final String str, final String dstr) {
        return (str == null) ? dstr : str;
    }
    public static String[] retrieveStrings(final String str) { return retrieveStrings(str,","); }
    public static String[] retrieveStrings(final String str,final String sep) {
        return (str == null) ? new String[0] : str.split(sep);
    }

    public static boolean retrieveBoolean(final String str, boolean defaultBoolean) {
        return (str == null) ? defaultBoolean : Boolean.valueOf(str);
    }
    public static boolean[] retrieveBooleans(final String str) { return retrieveBooleans(str,",");}
    public static boolean[] retrieveBooleans(final String str, final String sep) {
        if(str == null) return new boolean[0];
        if(str.contains(sep)) {
            String[] strings = retrieveStrings(str, ",");
            boolean[] booleans = new boolean[strings.length];
            for (int i = 0; i < booleans.length; i++) booleans[i] = Boolean.valueOf(str);
            return booleans;
        } else return new boolean[]{Boolean.parseBoolean(str)};
    }

    public static int retrieveInt(final String str, int defaultInt) {
        return (str == null) ? defaultInt : Integer.valueOf(str);
    }
    public static int[] retrieveInts(final String str) { return retrieveInts(str, ","); }
    public static int[] retrieveInts(final String str, final String sep) {
        if(str == null) return new int[0];
        if(str.contains(sep)) {
            final String[] parts = retrieveStrings(str, sep);
            final int[] ks = new int[parts.length];
            for (int i = 0; i < parts.length; i++)
                ks[i] = Integer.valueOf(parts[i]);
            return ks;
        } else return new int[]{Integer.valueOf(str)};
    }

    public static double retrieveDouble(final String str, double defaultDouble) {
        return (str == null) ? defaultDouble : Double.valueOf(str);

    }
    public static double[] retrieveDoubles(final String str) { return retrieveDoubles(str, ","); }
    public static double[] retrieveDoubles(final String str, final String sep) {
        if(str == null) return new double[0];
        if(str.contains(sep)) {
            final String[] parts = retrieveStrings(str, sep);
            final double[] weights = new double[parts.length];
            for (int i = 0; i < parts.length; i++)
                weights[i] = Double.parseDouble(parts[i]);
            return weights;
        } else return new double[]{Double.parseDouble(str)};
    }


    public static Path retrievePath(final String str) {
        if(str == null) return null;
        return new Path(str);
    }
    public static Path[] retrievePaths(final String str) {
        return retrievePaths(str,",");
    }
    public static Path[] retrievePaths(final String str,final String sep) {
        if(str == null) return new Path[0];
        if(str.contains(sep)) {
            final String[] strs = retrieveStrings(str, sep);
            final Path[] paths = new Path[strs.length];
            for (int i = 0; i < strs.length; i++) paths[i] = new Path(strs[i]);
            return paths;
        } else return new Path[]{new Path(str)};
    }


    public static boolean isValidName(final String name) {
        return name.matches("^[a-zA-Z1-9_]+$");
    }
    public static boolean isValidFieldName(final String name) { return name.matches("^[a-z_A-Z][a-z_A-Z0-9]*$");}

    public static String[] retrieveFields(final String fieldsStr) throws IllegalArgumentException {
        if(fieldsStr == null) return new String[0];
        String[] fields;
        if (fieldsStr.contains(",")) {
            fields = fieldsStr.split(",");
            for (String f : fields)
                if (!isValidFieldName(f))
                    throw new IllegalArgumentException("Field names must contain only alphanumeric characters and underscores.");
        } else {
            String f = fieldsStr;
            if (!isValidFieldName(f))
                throw new IllegalArgumentException("Field names must contain only alphanumeric characters and underscores.");
            fields = new String[1];
            fields[0] = fieldsStr;
        }
        return fields;
    }

    public static double retrieveProbability(final String str, final double dvalue) {
        double d = retrieveDouble(str,dvalue);
        if(d < 0 || d > 1 ) throw new IllegalArgumentException("Probability value is not between 0 and 1.");
        return d;
    }
    public static double[] retrieveProbabilities(final String str, final int dsize, final double dvalue) throws IllegalArgumentException {
        double[] ds = retrieveDoubles(str);
        if(ds.length == 0) {
            ds = new double[dsize];
            Arrays.fill(ds,dvalue);
        } else if(ds.length != dsize) throw new IllegalArgumentException("Input must be of size " + dsize +" elements.");
        for (double mm : ds) if(mm < 0 || mm > 1) throw new IllegalArgumentException("Probability value is not between 0 and 1.");
        return ds;
    }

    public static String prettySchemaDescription(final Schema schema) {
        final Map<String, String> description = new HashMap<String, String>();
        for (Schema.Field f : schema.getFields())
            description.put(f.name(), f.schema().getType().toString());
        final StringBuilder sb = new StringBuilder();
        int i = 1;
        for(Map.Entry<String,String> entry : description.entrySet())
            sb.append(String.format("%d, %s %s\n",i++,entry.getKey(),entry.getValue()));
        return sb.toString();
    }

    public static String prettyRecords(final GenericRecord[] records,
                                       final Schema schema) {
        final StringBuilder sb = new StringBuilder();
        final List<Schema.Field> fields = schema.getFields();
        final List<Schema.Type> types = new ArrayList<Schema.Type>();
        final List<String> fieldNames = new ArrayList<String>();

        for (int i = 0; i < fields.size() ; i++) {
            fieldNames.add(i, fields.get(i).name());
            types.add(i,fields.get(i).schema().getType());
        }
        sb.append("#Records =").append(records.length).append("\n");
        sb.append("#Fields =").append(fields.size()).append("\n");

        final StringBuilder hsb = new StringBuilder();
        for (int i = 0; i < fields.size() ; i++) {
            final Schema.Type type = types.get(i);
            hsb.append(String.format(
                    (type.equals(Schema.Type.FIXED)) ? "%100s|" : "%25s|",
                    String.format("%s (%s)", fieldNames.get(i),types.get(i))));
        }
        final String header = hsb.toString();
        sb.append(header).append("\n");
        sb.append(new String(new char[header.length()]).replace("\0", "-")).append("\n");
        for (GenericRecord record : records) {
            final StringBuilder rsb = new StringBuilder();
            for (int i = 0; i < fields.size(); i++) {
                final String fieldName = fieldNames.get(i);
                final Schema.Type type = types.get(i);
                if (type.equals(Schema.Type.FIXED)) {
                    GenericData.Fixed fixed = (GenericData.Fixed) record.get(i);
                    String val = prettyBinary(fixed.bytes());
                    if(fixed.bytes().length * 8 < 100)
                        rsb.append(String.format("%100s|", val));
                    else
                        rsb.append(String.format("%100s|",
                                val.substring(0,48) + "..." + val.substring(val.length()-48,val.length())));
                } else {
                    String val = String.valueOf(record.get(fieldName));
                    if (val.length() > 25) {
                        val = val.substring(0, 10) + "..." + val.substring(val.length() - 10);
                    }
                    rsb.append(String.format("%25s|", val));
                }
            }
            sb.append(rsb.toString()).append("\n");
        }

        return sb.toString();
    }

    public static String prettyStats(DatasetStatistics statistics) {
        final StringBuilder sb = new StringBuilder();
        sb.append("#Records=").append(statistics.getRecordCount()).append("\n");
        sb.append("#Fields=").append(statistics.getFieldCount()).append("\n");
        sb.append("#Pairs=").append(statistics.getEmPairs()).append("\n");
        sb.append("#Expectation Maximization Estimator iterations=")
                .append(statistics.getEmAlgorithmIterations())
                .append("\n");
        sb.append("#Estimated Duplicate Portion(p)=")
                .append(String.format("%.3f", statistics.getP()))
                .append("\n");
        final StringBuilder hsb = new StringBuilder(String.format("%50s","Metric\\Field name"));
        final Set<String> fieldNames = statistics.getFieldStatistics().keySet();
        for (String fieldName : fieldNames)
            hsb.append(String.format("|%25s", fieldName));
        final String header = hsb.toString();
        sb.append(header).append("\n");
        sb.append(new String(new char[header.length()]).replace("\0", "-")).append("\n");
        for (int i = 0; i < DatasetFieldStatistics.description.length; i++) {
            final StringBuilder ssb = new StringBuilder(
                    String.format("%50s",DatasetFieldStatistics.description[i]));
            for (String fieldName : fieldNames) {
                final double value = statistics.getFieldStatistics().get(fieldName).getStats()[i];
                ssb.append(String.format("|%25s", String.format((value < 0.00001) ? "%6.3e" : "%.5f", value)));
            }
            sb.append(ssb.toString()).append("\n");
        }
        sb.append(new String(new char[header.length()]).replace("\0", "-")).append("\n");

        return sb.toString();
    }

    public static String prettyBinary(final byte[] binary) {
        final StringBuilder sb = new StringBuilder();
        for (int i = (binary.length - 1); i >= 0 ; i--) {
            byte b = binary[i];
            sb.append(String.format("%8s", Integer.toBinaryString(b & 0xFF)).replace(' ', '0'));
        }
        return sb.toString();
    }

    public static String prettyBFEStats(Map<String, DatasetFieldStatistics> fieldStatistics, final int K, final int Q) {
        assert K > 0 && Q >= 2 && Q <= 4;
        final StringBuilder sb = new StringBuilder();
        Map<String,Integer> fbfNs = new HashMap<String,Integer>();
        Map<String,Integer> fbfNsUQ = new HashMap<String,Integer>();
        Map<String,Integer> rbfNs = new HashMap<String,Integer>();
        sb.append("#Encoding Bloom Filters K=").append(K).append("\n");
        sb.append("#Encoding Bloom Filters Q=").append(Q).append("\n");

        final StringBuilder hsb = new StringBuilder(String.format("%50s","Metric\\Field name"));
        final Set<String> fieldNames = fieldStatistics.keySet();
        for (String fieldName : fieldNames)
            hsb.append(String.format("|%25s", fieldName));
        final String header = hsb.toString();
        sb.append(header).append("\n");
        sb.append(new String(new char[header.length()]).replace("\0", "-")).append("\n");
        StringBuilder ssb = new StringBuilder(String.format("%50s","Dynamic FBF size"));
        for (String fieldName : fieldStatistics.keySet()) {
            double g = fieldStatistics.get(fieldName).getQgramCount(Q);
            int fbfN = FieldBloomFilterEncoding.dynamicsize(g,K);
            fbfNs.put(fieldName,fbfN);
            ssb.append(String.format("|%25s", String.format("%d",fbfN)));
        }
        sb.append(ssb.toString()).append("\n");

//        StringBuilder ssb = new StringBuilder(String.format("%50s","Dynamic FBF size (unique q-grams)"));
//        for (String fieldName : fieldStatistics.keySet()) {
//            double g = fieldStatistics.get(fieldName).getUniqueQgramCount(Q);
//            int fbfN = FieldBloomFilterEncoding.dynamicsize(g,K);
//            fbfNsUQ.put(fieldName,fbfN);
//            ssb.append(String.format("|%25s", String.format("%d",fbfN)));
//        }
//        sb.append(ssb.toString()).append("\n");

        ssb = new StringBuilder(String.format("%50s","Candidate RBF length"));
        for (String fieldName : fieldStatistics.keySet()) {
            double fbfN = fbfNs.get(fieldName);
            double nr = fieldStatistics.get(fieldName).getNormalizedRange();
            int rbfN  = (int) Math.ceil(fbfN/ nr);
            rbfNs.put(fieldName,rbfN);
            ssb.append(String.format("|%25s", String.format("%d", rbfN)));
        }
        sb.append(ssb.toString()).append("\n");

        int rbfN = Collections.max(rbfNs.values());
        sb.append(new String(new char[header.length()]).replace("\0", "-")).append("\n");
        sb.append("#RBF length=").append(rbfN).append("\n");
        sb.append(new String(new char[header.length()]).replace("\0", "-")).append("\n");

        ssb = new StringBuilder(String.format("%50s","Selected bit length"));
        for (String fieldName : fieldStatistics.keySet()) {
            double nr = fieldStatistics.get(fieldName).getNormalizedRange();
            int selectedBitCount = (int)Math.ceil(rbfN * nr);
            ssb.append(String.format("|%25s",
                    String.format("%d (%.1f %%)",
                            selectedBitCount,nr*100)
            ));
        }
        sb.append(ssb.toString()).append("\n");

        return sb.toString();
    }
}
