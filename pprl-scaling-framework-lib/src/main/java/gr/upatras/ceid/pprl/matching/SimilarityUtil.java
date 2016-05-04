package gr.upatras.ceid.pprl.matching;

import gr.upatras.ceid.pprl.combinatorics.CombinatoricsUtil;
import info.debatty.java.stringsimilarity.Cosine;
import info.debatty.java.stringsimilarity.Jaccard;
import info.debatty.java.stringsimilarity.JaroWinkler;
import org.apache.avro.generic.GenericRecord;

import java.util.Iterator;

/**
 * Similarity utility class.
 */
public class SimilarityUtil {

    public static final String[] SIMILARITY_METHOD_NAMES = { // Available String similarity methods.
            "jaro_winkler",
            "jaccard_bigrams","jaccard_trigrams",
            "cosine_bigrams","cosine_trigrams",
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
        if(name.equals("jaro_winkler"))
            return (new JaroWinkler()).similarity(s1,s2) >= 0.70;
        if(name.equals("jaccard_bigrams"))
            return (new Jaccard(2)).similarity(s1,s2) >= 0.85;
        if(name.equals("jaccard_trigrams"))
            return (new Jaccard(3)).similarity(s1,s2) >= 0.85;
        if(name.equals("cosine_bigrams"))
            return (new Cosine(2)).similarity(s1,s2) >= 0.85;
        if(name.equals("cosine_trigrams"))
            return (new Cosine(3)).similarity(s1,s2) >= 0.85;
        if(name.equals("exact"))
            return s1.equals(s2);
        throw new UnsupportedOperationException("Unsupported Matching for name " + name + " .");
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
                if(similarity(similarityMethodName, s1, s2)) matrix.set((int)i, j);
            }
        }while(pairIter.hasNext());
        return matrix;
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
     * @return a <code>SimilarityVectorFrequencies</code> instance.
     */
    public static SimilarityVectorFrequencies vectorFrequencies(final GenericRecord[] records,
                                                                final String[] fieldNames,
                                                                final String similarityMethodName) {
        final long pairCount = CombinatoricsUtil.twoCombinationsCount(records.length);
        final int fieldCount = fieldNames.length;
        if(Long.compare(pairCount*fieldCount,Integer.MAX_VALUE) > 0)
            throw new UnsupportedOperationException("Cannot create gamma. #N*#F < Integer.MAX");
        final SimilarityVectorFrequencies frequencies = new SimilarityVectorFrequencies(fieldCount);
        final Iterator<int[]> pairIter = CombinatoricsUtil.getPairs(records.length);
        do {
            int pair[] = pairIter.next();
            final GenericRecord[] recordPair =
                    new GenericRecord[]{records[pair[0]],records[pair[1]]};
            boolean[] row =
                    recordPairSimilarity(
                            recordPair, fieldNames, similarityMethodName);
            frequencies.set(row);

        }while(pairIter.hasNext());
        return frequencies;
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
     * Returns a boolean vector representing the a record pairs similarity among their fields.
     *
     * @param recordPair a generic avro record pair.
     * @param fieldNames field names.
     * @return a similarity boolean vector.
     */
    public static boolean[] recordPairSimilarity(final GenericRecord[] recordPair, final String[] fieldNames) {
        return recordPairSimilarity(recordPair,fieldNames, DEFAULT_SIMILARITY_METHOD_NAME);
    }

    /**
     * Returns a boolean vector representing the a record pairs similarity among their fields.
     *
     * @param recordPair a generic avro record pair.
     * @param fieldNames field names.
     * @param similarityMethodName similarity method name.
     * @return a similarity boolean vector.
     */
    public static boolean[] recordPairSimilarity(final GenericRecord[] recordPair, final String[] fieldNames,
                                                 final String similarityMethodName) {
        assert recordPair.length == 2;
        final boolean[] vector = new boolean[fieldNames.length];
        int i = 0;
        for (String fieldName : fieldNames) {
            final String v0 =  String.valueOf(recordPair[0].get(fieldName));
            final String v1 =  String.valueOf(recordPair[1].get(fieldName));
            vector[i++] =  similarity(similarityMethodName, v0, v1);
        }
        return vector;
    }
}
