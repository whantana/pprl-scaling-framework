package gr.upatras.ceid.pprl.qgram;

import org.apache.avro.Schema;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Q-Gram utility class.
 */
public class QGramUtil {

    private static final String PADDING_STRING = "_";

    /**
     * Returns "proper" string as in all space are replaced with padding string.
     * Input string is also padded.
     *
     * @param string input string
     * @return "proper" string.
     */
    public static String properString(final String string) {
        String proper = string.replaceAll("\\s+", PADDING_STRING);

        proper = (proper.startsWith(PADDING_STRING)) ? proper : PADDING_STRING + proper;
        proper = (proper.endsWith(PADDING_STRING)) ? proper : proper + PADDING_STRING;
        return proper;
    }

    /**
     * Returns the number of q-grams on the "proper" string.
     *
     * @param proper input "proper" string.
     * @param Q Q as in Q-grams.
     * @return count of Q-Grams.
     */
    private static int calcQgramsCountOnProper(final String proper, final int Q) {
        int qGramCount = proper.length() - Q + 1;
        return (qGramCount > 0) ? qGramCount : 0 ;
    }

    /**
     * Returns the number of q-grams of the object. Infers class with the use of type.
     *
     * @param obj input object.
     * @param type object avro type.
     * @param Q Q as in Q-grams.
     * @return count of Q-Grams.
     */
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

    /**
     * Returns the number of q-grams of the string.
     *
     * @param string input string.
     * @param Q Q as in Q-grams.
     * @return count of Q-Grams.
     */
    private static int calcQgramsCount(final String string, final int Q) {
        if(string.isEmpty()) return 0;
        int qGramCount = properString(string).length() - Q + 1;
        return (qGramCount > 0) ? qGramCount : 0 ;
    }

    /**
     * Returns the number of q-grams of the number.
     *
     * @param number input number.
     * @param Q Q as in Q-grams.
     * @return count of Q-Grams.
     */
    private static int calcQgramsCount(final Number number, final int Q) {
        String string = String.valueOf(number);
        if(string.isEmpty()) return 0;
        string = (string.startsWith(PADDING_STRING)) ? string : PADDING_STRING + string;
        string = (string.endsWith(PADDING_STRING)) ? string : string + PADDING_STRING;
        int qGramCount = string.length() - Q + 1;
        return (qGramCount > 0) ? qGramCount : 0 ;
    }

    /**
     * Returns the number of q-grams of the boolean ("T") or ("F").
     *
     * @param bool input bool
     * @param Q as in Q-Grams
     * @return count of Q-Grams.
     */
    private static int calcQgramsCount(final Boolean bool, final int Q) {
        if(bool == null) return 0;
        String string = "_" + (bool ? "T" : "F") + "_";
        if(Q >= string.length()) return 1;
        int qGramCount = string.length() - Q + 1;
        return (qGramCount > 0) ? qGramCount : 0 ;
    }

    /**
     * Returns the number of unique q-grams of the object. Infers class with the use of type.
     *
     * @param obj input object.
     * @param type object avro type.
     * @param Q Q as in Q-grams.
     * @return count of unique Q-Grams.
     */
    public static int calcUniqueQgramsCount(final Object obj, final Schema.Type type, final int Q) {
        switch(type) {
            case BOOLEAN:
                return calcUniqueQgramsCount((Boolean) obj, Q);
            case STRING:
                return calcUniqueQgramsCount(String.valueOf(obj), Q);
            case INT:
            case LONG:
            case DOUBLE:
            case FLOAT:
                return calcUniqueQgramsCount((Number) obj, Q);
            default:
                return 0;
        }
    }

    /**
     * Returns the number of unique q-grams of the string.
     *
     * @param string input string.
     * @param Q Q as in Q-grams.
     * @return count of unique Q-Grams.
     */
    private static int calcUniqueQgramsCount(final String string, final int Q) {
        if(string.isEmpty()) return 0;
        return generateUniqueQGrams(string,Q).length;
    }

    /**
     * Returns the number of unique q-grams of the number.
     *
     * @param number input number.
     * @param Q Q as in Q-grams.
     * @return count of unique Q-Grams.
     */
    private static int calcUniqueQgramsCount(final Number number, final int Q) {
        return generateUniqueQGrams(number,Q).length;
    }

    /**
     * Returns the number of unique q-grams of the boolean.
     *
     * @param bool input boolean
     * @param Q Q as in Q-grams.
     * @return count of unique Q-Grams.
     */
    private static int calcUniqueQgramsCount(final Boolean bool, final int Q) {
        return generateUniqueQGrams(bool,Q).length;
    }

    /**
     * Returns the Q-grams array of the object. Infers class with the use of type.
     *
     * @param obj input object.
     * @param type object avro type.
     * @param Q Q as in Q-grams.
     * @return array of Q-Grams.
     */
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

    /**
     * Returns the Q-grams array of the string.
     *
     * @param string input string.
     * @param Q Q as in Q-grams.
     * @return array of Q-Grams.
     */
    private static String[] generateQGrams(final String string, final int Q) {
        if(string.isEmpty()) return new String[0];
        final String proper = properString(string);
        int qGramsCount = calcQgramsCountOnProper(proper, Q);
        String[] qGrams = new String[qGramsCount];
        int i = 0;
        final QGramIterator iterator = new QGramIterator(proper,Q);
        while(iterator.hasNext()) qGrams[i++] = iterator.next();
        return qGrams;
    }

    /**
     * Returns the Q-grams array of the number.
     *
     * @param number input number.
     * @param Q Q as in Q-grams.
     * @return array of Q-Grams.
     */
    private static String[] generateQGrams(final Number number, final int Q) {
        String string = String.valueOf(number);
        if(string.isEmpty()) return new String[0];
        string = (string.startsWith(PADDING_STRING)) ? string : PADDING_STRING + string;
        string = (string.endsWith(PADDING_STRING)) ? string :  string + PADDING_STRING;
        int qGramCount = string.length() - Q + 1;
        String[] qGrams = new String[qGramCount];
        int i = 0;
        final QGramIterator iterator = new QGramIterator(string,Q);
        while(iterator.hasNext()) qGrams[i++] = iterator.next();
        return qGrams;
    }

    /**
     * Returns the Q-grams array of the number.
     *
     * @param bool input boolean.
     * @param Q Q as in Q-grams.
     * @return array of Q-Grams.
     */
    private static String[] generateQGrams(final Boolean bool, final int Q) {
        if(bool == null) return new String[0];
        String string = bool ? "T":"F";
        string = "_" + string + "_";
        int len = string.length();
        if(Q >= len) return new String[]{string};
        int qGramsCount = len - Q + 1;
        if(qGramsCount < 1) return new String[0];
        String[] qGrams = new String[qGramsCount];
        int i = 0;
        final QGramIterator iterator = new QGramIterator(string,Q);
        while(iterator.hasNext()) qGrams[i++] = iterator.next();
        return qGrams;
    }

    /**
     * Returns the unique Q-grams array of the object. Infers class with the use of type.
     *
     * @param obj input object.
     * @param type object avro type.
     * @param Q Q as in Q-grams.
     * @return array of unique Q-Grams.
     */
    public static String[] generateUniqueQGrams(final Object obj, final Schema.Type type, final int Q) {
        switch(type) {
            case BOOLEAN:
                return generateUniqueQGrams((Boolean) obj, Q);
            case STRING:
                return generateUniqueQGrams(String.valueOf(obj), Q);
            case INT:
            case LONG:
            case DOUBLE:
            case FLOAT:
                return generateUniqueQGrams((Number) obj, Q);
            default:
                return new String[0];
        }
    }

    /**
     * Returns the unique Q-grams array of the string.
     *
     * @param string input string.
     * @param Q Q as in Q-grams.
     * @return array of unique Q-Grams.
     */
    private static String[] generateUniqueQGrams(final String string, final int Q) {
        if(string.isEmpty()) return new String[0];
        final String proper = properString(string);
        final QGramIterator iterator = new QGramIterator(proper,Q);
        Set<String> qGramsSet = new HashSet<String>();
        while(iterator.hasNext()) { qGramsSet.add(iterator.next()); }
        return qGramsSet.toArray(new String[qGramsSet.size()]);
    }

    /**
     * Returns the unique Q-grams array of the number.
     *
     * @param number input number.
     * @param Q Q as in Q-grams.
     * @return array of unique Q-Grams.
     */
    private static String[] generateUniqueQGrams(final Number number, final int Q) {
        String string = String.valueOf(number);
        if(string.isEmpty()) return new String[0];
        string = (string.startsWith(PADDING_STRING)) ? string : PADDING_STRING + string;
        string = (string.endsWith(PADDING_STRING)) ? string : PADDING_STRING + string;
        String proper = string;
        final QGramIterator iterator = new QGramIterator(proper,Q);
        Set<String> qGramsSet = new HashSet<String>();
        while(iterator.hasNext()) { qGramsSet.add(iterator.next()); }
        return qGramsSet.toArray(new String[qGramsSet.size()]);
    }

    /**
     * Returns the unique Q-grams array of the boolean.
     *
     * @param bool input boolean.
     * @param Q Q as in Q-grams.
     * @return array of unique Q-Grams.
     */
    private static String[] generateUniqueQGrams(final Boolean bool, final int Q) {
        return generateQGrams(bool,Q);
    }

    /**
     *  Q Gram iterator of string based on the substring.
     */
    public static class QGramIterator implements Iterator<String> {
        private final String string;
        private final int Q;
        int index;

        public QGramIterator(final String string,int Q) {
            this.index = 0;
            this.string = string;
            this.Q = Q;
        }

        public boolean hasNext() {
            return (index + Q) <= string.length();
        }

        public String next() {
            if((index + Q) > string.length()) return null;
            final String qgram = string.substring(index, index + Q);
            index++;
            return qgram;
        }

        public void remove() {
            throw new UnsupportedOperationException("Unsupported!");
        }
    }
}