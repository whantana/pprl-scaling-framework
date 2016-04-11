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

}
