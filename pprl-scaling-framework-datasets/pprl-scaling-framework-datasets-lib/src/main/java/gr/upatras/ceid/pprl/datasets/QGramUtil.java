package gr.upatras.ceid.pprl.datasets;

public class QGramUtil {

    public static int calcQgramsCount(final String string, int Q) {
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

    public static int calcQgramsCount(final Number number, int Q) {
        String string = String.valueOf(number);
        String onlyNumbers = string.replaceAll("[^\\pN\\s]+", "_");
        String finalString = (!onlyNumbers.startsWith("_")) ? "" +
                "_" + onlyNumbers : onlyNumbers;
        finalString = (!onlyNumbers.endsWith("_")) ?
                finalString +"_" : finalString;
        return  finalString.length() - Q + 1;
    }

    public static int calcQgramsCount(final Boolean bool, int Q) {
        String string = bool ? "T":"F";
        String finalString = "_" + string + "_";
        return finalString.length() - Q + 1;
    }

    public static String[] generateQGrams(final String string, int Q) {
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

    public static String[] generateQGrams(final Number number, int Q) {
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

    public static String[] generateQGrams(final Boolean bool, int Q) {
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