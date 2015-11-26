package gr.upatras.ceid.pprl.encoding;

public class QGram {

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
        if(qGramsCount < 1) return null;
        String[] qGrams = new String[qGramsCount];
        for (int i = 0; i < qGramsCount; i++)
            qGrams[i] = finalString.substring(i, i + Q);

        return qGrams;
    }

    public static String[] generateQGrams(final Number number, int Q) {
        String string = String.valueOf(number);
        String finalString = string.replaceAll("[^\\pN\\s]+", "_");

        int len = finalString.length();
        int qGramsCount = len - Q + 1;
        if(qGramsCount < 1) return null;
        String[] qGrams = new String[qGramsCount];
        for (int i = 0; i < qGramsCount; i++)
            qGrams[i] = finalString.substring(i, i + Q);

        return qGrams;
    }
}