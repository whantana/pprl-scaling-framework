package gr.upatras.ceid.pprl.matching;

import gr.upatras.ceid.pprl.combinatorics.CombinatoricsUtil;
import gr.upatras.ceid.pprl.qgram.QGramUtil;
import info.debatty.java.stringsimilarity.Cosine;
import info.debatty.java.stringsimilarity.Jaccard;
import info.debatty.java.stringsimilarity.JaroWinkler;
import info.debatty.java.stringsimilarity.SorensenDice;
import org.apache.avro.generic.GenericRecord;

import java.util.Iterator;

/**
 * Similarity utility class.
 */
public class SimilarityUtil {

    public static final String[] SIMILARITY_METHOD_NAMES = { // Available String similarity methods.
            "jaro_winkler",
            "jaccard_bigrams","jaccard_trigrams","jaccard_quadgrams",
            "cosine_bigrams","cosine_trigrams","cosine_quadgrams",
            "dice_bigrams","dice_trigrams","dice_quadgrams",
            "exact"
    };
    public static final String DEFAULT_SIMILARITY_METHOD_NAME = SIMILARITY_METHOD_NAMES[0]; // Default method.

    /**
     * Returns true if calculated string similarity between two strings s1,s2 is above a certain
     * threshold, false otherwise
     *
     * @param name similarity method name.
     * @param s1 string 1.
     * @param s2 string 2.
     * @return true if strings are similar, false otherwise.
     */
    public static boolean similarity(final String name, final String s1, final String s2) {
        return similarity(name,s1,s2,0.70);
    }

    /**
     * Returns true if calculated string similarity between two strings s1,s2 is above a certain
     * threshold, false otherwise
     *
     * @param name similarity method name.
     * @param s1 string 1.
     * @param s2 string 2.
     * @param threshold threshold value.
     * @return true if strings are similar, false otherwise.
     */
    public static boolean similarity(final String name, final String s1,final String s2, final double threshold) {
        String s11 = s1;
        String s22 = s2;
        if(name.equals("jaro_winkler"))
            return (new JaroWinkler()).similarity(s11,s22) >= threshold;
        if(name.equals("exact"))
            return s11.equals(s22);
        s11 = QGramUtil.properString(s1);
        s22 = QGramUtil.properString(s2);
        if(name.equals("jaccard_bigrams"))
            return (new Jaccard(2)).similarity(s11,s22) >= threshold;
        if(name.equals("jaccard_trigrams"))
            return (new Jaccard(3)).similarity(s11,s22) >= threshold;
        if(name.equals("jaccard_quadgrams"))
            return (new Jaccard(4)).similarity(s11,s22) >= threshold;
        if(name.equals("cosine_bigrams"))
            return (new Cosine(2)).similarity(s11,s22) >= threshold;
        if(name.equals("cosine_trigrams"))
            return (new Cosine(3)).similarity(s11,s22) >= threshold;
        if(name.equals("cosine_quadgrams"))
            return (new Cosine(4)).similarity(s11,s22) >= threshold;
        if(name.equals("dice_bigrams"))
            return (new SorensenDice(2)).similarity(s11,s22) >= threshold;
        if(name.equals("dice_trigrams"))
            return (new SorensenDice(3)).similarity(s11,s2) >= threshold;
        if(name.equals("dice_quadgrams"))
            return (new SorensenDice(4)).similarity(s11,s22) >= threshold;
        throw new UnsupportedOperationException("Unsupported Matching for name " + name + " .");
    }



    /**
     * Create a self-similarity matrix from a given record set.
     *
     * @param records array of generic avro records.
     * @param fieldNames field names.
     * @param similarityMethodName similarity method name.
     * @param threshold similarity threshold
     * @return a <code>SimilarityMatrix</code> instance.
     */
    public static SimilarityMatrix matrix(final GenericRecord[] records,
                                          final String[] fieldNames,
                                          final String similarityMethodName,
                                          final double threshold) {
        final int pairCount = (int) CombinatoricsUtil.twoCombinationsCount(records.length);
        final int fieldCount = fieldNames.length;
        if(Long.compare(pairCount*fieldCount,Integer.MAX_VALUE) > 0)
            throw new UnsupportedOperationException("Cannot create gamma. #N*#F < Integer.MAX");
        final SimilarityMatrix matrix = new SimilarityMatrix(pairCount,fieldCount);
        final Iterator<int[]> pairIter = CombinatoricsUtil.getPairs(records.length);
        do {
            int pair[] = pairIter.next();
            long i = CombinatoricsUtil.rankTwoCombination(pair);
            for(int j=0; j < fieldCount; j++) {
                String s1 = String.valueOf(records[pair[0]].get(fieldNames[j]));
                String s2 = String.valueOf(records[pair[1]].get(fieldNames[j]));
                if(similarity(similarityMethodName, s1, s2,threshold))
                    matrix.set((int)i, j);
            }
        }while(pairIter.hasNext());
        return matrix;
    }

    /**
     * Create a self-similarity matrix from a given record set.
     *
     * @param records array of generic avro records.
     * @param fieldNames field names.
     * @param similarityMethodName similarity method name.
     * @return a <code>SimilarityMatrix</code> instance.
     */
    public static SimilarityMatrix matrix(final GenericRecord[] records,
                                          final String[] fieldNames,
                                          final String similarityMethodName) {
        return matrix(records, fieldNames, similarityMethodName,0.7);
    }

    /**
     * Create a self-similarity matrix from a given record set.
     *
     * @param records array of generic avro records.
     * @param fieldNames field names.
     * @return a <code>SimilarityMatrix</code> instance.
     */
    public static SimilarityMatrix matrix(final GenericRecord[] records,
                                          final String[] fieldNames) {
        return matrix(records, fieldNames, DEFAULT_SIMILARITY_METHOD_NAME);
    }

    /**
     * Create a self-similarity vector frequencies from a given record set.
     *
     * @param records array of generic avro records.
     * @param fieldNames field names.
     * @param similarityMethodName similarity method name.
     * @param threshold similarity threshold.
     * @return a <code>SimilarityVectorFrequencies</code> instance.
     */
    public static SimilarityVectorFrequencies vectorFrequencies(final GenericRecord[] records,
                                                                final String[] fieldNames,
                                                                final String similarityMethodName,
                                                                final double threshold) {
        final int fieldCount = fieldNames.length;
        if ( (1 << fieldCount) < 0 || (1 << fieldCount) >= Integer.MAX_VALUE)
            throw new UnsupportedOperationException("Field count is too high.");
        final SimilarityVectorFrequencies frequencies = new SimilarityVectorFrequencies(fieldCount);
        final Iterator<int[]> pairIter = CombinatoricsUtil.getPairs(records.length);
        do {
            int pair[] = pairIter.next();
            final GenericRecord[] recordPair =
                    new GenericRecord[]{records[pair[0]],records[pair[1]]};
            boolean[] row = recordPairSimilarity(recordPair, fieldNames,
                    similarityMethodName,threshold);
            frequencies.set(row);

        }while(pairIter.hasNext());
        return frequencies;
    }

    /**
     * Create a self-similarity vector frequencies from a given record set.
     *
     * @param records array of generic avro records.
     * @param fieldNames field names.
     * @param similarityMethodName similarity method name.
     * @return a <code>SimilarityVectorFrequencies</code> instance.
     */
    public static SimilarityVectorFrequencies vectorFrequencies(final GenericRecord[] records,
                                                                final String[] fieldNames,
                                                                final String similarityMethodName) {
        return vectorFrequencies(records,fieldNames,similarityMethodName,0.7);
    }

    /**
     * Create a self-similarity vector frequencies from a given record set.
     *
     * @param records array of generic avro records.
     * @param fieldNames field names.
     * @return a <code>SimilarityVectorFrequencies</code> instance.
     */
    public static SimilarityVectorFrequencies vectorFrequencies(final GenericRecord[] records,
                                                                final String[] fieldNames) {
        return vectorFrequencies(records, fieldNames, DEFAULT_SIMILARITY_METHOD_NAME);
    }

    /**
     * Create a self-similarity vector frequencies from a given record set.
     *
     * @param recordsA array of generic avro records.
     * @param recordsB array of generic avro records.
     * @param fieldNamesA field names of A dataset.
     * @param fieldNamesB field names of B dataset.
     * @param similarityMethodName similarity method name.
     * @param threshold similarity threshold.
     * @return a <code>SimilarityVectorFrequencies</code> instance.
     */
    public static SimilarityVectorFrequencies vectorFrequencies(final GenericRecord[] recordsA,
                                                                final GenericRecord[] recordsB,
                                                                final String[] fieldNamesA,
                                                                final String[] fieldNamesB,
                                                                final String similarityMethodName,
                                                                final double threshold) {
        assert fieldNamesA.length == fieldNamesB.length;
        final int fieldCount = fieldNamesA.length;
        if ( (1 << fieldCount) < 0 || (1 << fieldCount) >= Integer.MAX_VALUE)
            throw new UnsupportedOperationException("Field count is too high.");
        final SimilarityVectorFrequencies frequencies = new SimilarityVectorFrequencies(fieldCount);
        for(GenericRecord recordA : recordsA) {
            for(GenericRecord recordB : recordsB) {
                final GenericRecord[] recordPair = new GenericRecord[]{recordA,recordB};
                boolean[] row = recordPairSimilarity(
                        recordPair,
                        new String[][]{fieldNamesA,fieldNamesB},
                        similarityMethodName,threshold);
                frequencies.set(row);
            }
        }

        return frequencies;
    }

    /**
     * Create a self-similarity vector frequencies from a given record set.
     *
     * @param recordsA array of generic avro records.
     * @param recordsB array of generic avro records.
     * @param fieldNamesA field names of A dataset.
     * @param fieldNamesB field names of B dataset.
     * @return a <code>SimilarityVectorFrequencies</code> instance.
     */
    public static SimilarityVectorFrequencies vectorFrequencies(final GenericRecord[] recordsA,
                                                                final GenericRecord[] recordsB,
                                                                final String[] fieldNamesA,
                                                                final String[] fieldNamesB) {
        return vectorFrequencies(recordsA,recordsB,fieldNamesA,fieldNamesB,DEFAULT_SIMILARITY_METHOD_NAME,0.7);
    }




    /**
     * Returns a boolean vector representing the a record pairs similarity among their fields.
     *
     * @param recordPair a generic avro record pair.
     * @param fieldNames field names.
     * @param similarityMethodName similarity method name.
     * @param threshold similarity threshold.
     * @return a similarity boolean vector.
     */
    public static boolean[] recordPairSimilarity(final GenericRecord[] recordPair,
                                                 final String[] fieldNames,
                                                 final String similarityMethodName,
                                                 final double threshold) {
        assert recordPair.length == 2;
        final boolean[] vector = new boolean[fieldNames.length];
        int i = 0;
        for (String fieldName : fieldNames) {
            final String v0 =  String.valueOf(recordPair[0].get(fieldName));
            final String v1 =  String.valueOf(recordPair[1].get(fieldName));
            vector[i++] =  similarity(similarityMethodName, v0, v1,threshold);
        }
        return vector;
    }




    /**
     * Returns a boolean vector representing the a record pairs similarity among their fields.
     *
     * @param recordPair a generic avro record pair.
     * @param fieldNames field names.
     * @return a similarity boolean vector.
     */
    public static boolean[] recordPairSimilarity(final GenericRecord[] recordPair, final String[] fieldNames) {
        return recordPairSimilarity(recordPair,fieldNames, DEFAULT_SIMILARITY_METHOD_NAME,0.7);
    }


    /**
     * Returns a boolean vector representing the a record pairs similarity among their fields.
     *
     * @param recordPair a generic avro record pair.
     * @param fieldNames field names of array of both records.
     * @param similarityMethodName similarity method name.
     * @param threshold similarity threshold.
     * @return a similarity boolean vector.
     */
    public static boolean[] recordPairSimilarity(final GenericRecord[] recordPair,
                                                 final String[][] fieldNames,
                                                 final String similarityMethodName,
                                                 final double threshold) {
        assert recordPair.length == 2;
        assert fieldNames[0].length == fieldNames[1].length;
        final boolean[] vector = new boolean[fieldNames[0].length];
        for (int i = 0; i < fieldNames[0].length; i++ ) {
            final String v0 =  String.valueOf(recordPair[0].get(fieldNames[0][i]));
            final String v1 =  String.valueOf(recordPair[1].get(fieldNames[1][i]));
            vector[i] =  similarity(similarityMethodName, v0, v1,threshold);
        }
        return vector;
    }

    /**
     * Returns a boolean vector representing the a record pairs similarity among their fields.
     *
     * @param recordPair a generic avro record pair.
     * @param fieldNames field names arrays for both records
     */
    public static boolean[] recordPairSimilarity(final GenericRecord[] recordPair,
                                                 final String[][] fieldNames) {
        return recordPairSimilarity(recordPair,fieldNames,DEFAULT_SIMILARITY_METHOD_NAME,0.7);
    }

    /**
     * Returns a boolean vector representing the a record pairs similarity among their fields.
     *
     * @param strings A two dimentional string array. First dimention must be equal to 2 (pair).
     *                Second dimentation must be of the same length
     * @param similarityMethodName similarity method name.
     * @param threshold similarity threshold
     * @return a similarity boolean vector.
     */
    public static boolean[] stringPairSimilarity(final String[][] strings,
                                                 final String similarityMethodName,
                                                 final double threshold) {
        assert strings.length == 2;
        assert strings[0].length == strings[1].length;
        final boolean[] vector = new boolean[strings[0].length];
        for (int i = 0 ; i < strings[0].length ; i++) {
            vector[i] =  similarity(similarityMethodName, strings[0][i], strings[1][i],threshold);
        }
        return vector;
    }

    /**
     * Returns a boolean vector representing the a record pairs similarity among their fields.
     *
     * @param strings A two dimentional string array. First dimention must be equal to 2 (pair).
     *                Second dimentation must be of the same length
     * @return a similarity boolean vector.
     */
    public static boolean[] stringPairSimilarity(final String[][] strings) {
        return stringPairSimilarity(strings,DEFAULT_SIMILARITY_METHOD_NAME,0.7);
    }


    /**
     * Calculate record pair similarity score.
     *
     * @param recordPair a generic avro record pair.
     * @param fieldNames field names arrays for both records
     * @param m FS m probability for fields.
     * @param u FS u probability for fields.
     * @return score
     */
    public static double similarityScore(final GenericRecord[] recordPair,
                                         final String[][] fieldNames,
                                         final double[] m, final double[] u) {
        return similarityScore(recordPair, fieldNames, DEFAULT_SIMILARITY_METHOD_NAME, 0.7, m, u);
    }

    /**
     * Calculate record pair similarity score.
     *
     * @param recordPair a generic avro record pair.
     * @param fieldNames field names arrays for both records
     * @param m FS m probability for fields.
     * @param u FS u probability for fields.
     * @return score
     */
    public static double similarityScore(final GenericRecord[] recordPair,
                                         final String[][] fieldNames,
                                         final String similarityMethodName,
                                         final double threshold,
                                         final double[] m, final double[] u) {
        assert fieldNames[0].length == fieldNames[1].length;

        final boolean[] similarityVector = recordPairSimilarity(
                recordPair, fieldNames,
                similarityMethodName,threshold);

        return similarityScore(similarityVector,m,u);
    }

    /**
     * Calculate record pair similarity score.
     *
     * @param similarityVector a similarity vector.
     * @param m FS m probability for fields.
     * @param u FS u probability for fields.
     * @return score
     */
    public static double similarityScore(final boolean[] similarityVector,
                                         final double[] m, final double[] u) {
        assert similarityVector.length == m.length && u.length == m.length;
        double score = 0.0;
        for (int i = 0; i < similarityVector.length; i++) {
            score += Math.log(similarityVector[i] ? (m[i]/u[i]) : ((1-m[i])/(1-u[i])));
        }
        return score;
    }
}
