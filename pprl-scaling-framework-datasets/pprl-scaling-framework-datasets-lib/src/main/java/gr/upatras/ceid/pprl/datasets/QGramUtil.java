package gr.upatras.ceid.pprl.datasets;

import org.apache.avro.Schema;

public class QGramUtil {
    
    public static int calcQgramsCount(final Object obj, final Schema.Type type, final int Q) {
        switch(type) {
            case BOOLEAN:
                return calcQgramsCount((Boolean) obj, Q);
            case STRING:
                return calcQgramsCount(String.valueOf(obj), Q);
            case INT:
            case LONG:
            case DOUBLE:
            case FLOAT:
                return calcQgramsCount((Number) obj, Q);
            default:
                return 0;
        }
    }

    public static int calcQgramsCount(final String string, final int Q) {
        String onlyAlnum = string.replaceAll("[^\\pL\\pN\\s]+", " ");
        String onlyCapitals = onlyAlnum.toUpperCase();
        String replaceSpaces = onlyCapitals.replaceAll("\\s+","_");
        replaceSpaces = (replaceSpaces.startsWith("_")) ?
                replaceSpaces : "_" + replaceSpaces;
        replaceSpaces = (replaceSpaces.endsWith("_")) ?
                replaceSpaces : replaceSpaces + "_";
        String finalString = replaceSpaces;
        return  finalString.length() - Q + 1;
    }

    public static int calcQgramsCount(final Number number, final int Q) {
        String string = String.valueOf(number);
        String onlyNumbers = string.replaceAll("[^\\pN\\s]+", "_");
        String finalString = (!onlyNumbers.startsWith("_")) ? "" +
                "_" + onlyNumbers : onlyNumbers;
        finalString = (!onlyNumbers.endsWith("_")) ?
                finalString +"_" : finalString;
        return  finalString.length() - Q + 1;
    }

    public static int calcQgramsCount(final Boolean bool, final int Q) {
        String string = String.valueOf(bool);
        String finalString = "_" + string + "_";
        return finalString.length() - Q + 1;
    }

    public static String[] generateQGrams(final Object obj, final Schema.Type type, final int Q) {
        switch(type) {
            case BOOLEAN:
                return generateQGrams((Boolean) obj, Q);
            case STRING:
                return generateQGrams(String.valueOf(obj), Q);
            case INT:
            case LONG:
            case DOUBLE:
            case FLOAT:
                return generateQGrams((Number) obj,Q);
            default:
                return new String[0];
        }
    }

    public static String[] generateQGrams(final String string, final int Q) {
        String onlyAlnum = string.replaceAll("[^\\pL\\pN\\s]+", " ");
        String onlyCapitals = onlyAlnum.toUpperCase();
        String replaceSpaces = onlyCapitals.replaceAll("\\s+","_");
        replaceSpaces = (replaceSpaces.startsWith("_")) ?
                replaceSpaces : "_" + replaceSpaces;
        replaceSpaces = (replaceSpaces.endsWith("_")) ?
                replaceSpaces : replaceSpaces + "_";
        String finalString = replaceSpaces;
        int len = finalString.length();
        int qGramsCount = len - Q + 1;
        if(qGramsCount < 1) return new String[0];
        String[] qGrams = new String[qGramsCount];
        for (int i = 0; i < qGramsCount; i++)
            qGrams[i] = finalString.substring(i, i + Q);
        return qGrams;
    }

    public static String[] generateQGrams(final Number number, final int Q) {
        String string = String.valueOf(number);
        String onlyNumbers = string.replaceAll("[^\\pN\\s]+", "_");
        String finalString = (!onlyNumbers.startsWith("_")) ? "" +
                "_" + onlyNumbers : onlyNumbers;
        finalString = (!onlyNumbers.endsWith("_")) ?
                finalString +"_" : finalString;
        int len = finalString.length();
        int qGramsCount = len - Q + 1;
        if(qGramsCount < 1) return new String[0];
        String[] qGrams = new String[qGramsCount];
        for (int i = 0; i < qGramsCount; i++)
            qGrams[i] = finalString.substring(i, i + Q);
        return qGrams;
    }

    public static String[] generateQGrams(final Boolean bool, final int Q) {
        String string = bool ? "T":"F";
        String finalString = "_" + string + "_";
        int len = finalString.length();
        int qGramsCount = len - Q + 1;
        if(qGramsCount < 1) return new String[0];
        String[] qGrams = new String[qGramsCount];
        for (int i = 0; i < qGramsCount; i++)
            qGrams[i] = finalString.substring(i, i + Q);
        return qGrams;
    }
}