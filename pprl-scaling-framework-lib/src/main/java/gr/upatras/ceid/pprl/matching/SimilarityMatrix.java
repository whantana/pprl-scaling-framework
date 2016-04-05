package gr.upatras.ceid.pprl.matching;

import info.debatty.java.stringsimilarity.Cosine;
import info.debatty.java.stringsimilarity.Jaccard;
import info.debatty.java.stringsimilarity.JaroWinkler;
import org.apache.avro.generic.GenericRecord;

import java.util.Arrays;
import java.util.Properties;

public class SimilarityMatrix {
    private int fieldCount;
    private long[] vectorCounts;

    public SimilarityMatrix() {}

    public SimilarityMatrix(int fieldCount) {
        this.fieldCount = fieldCount;
        vectorCounts = new long[1 << fieldCount];
    }

    public void set(final boolean[] row) {
        assert row.length == fieldCount;
        vectorCounts[vector2Index(row)]++;
    }

    public static int vector2Index(final boolean[] row) {
        int index = 0;
        for(int i = 0; i < row.length; i ++) if(row[i]) index |= (1 << i);
        return index;
    }

    public static boolean[] index2Vector(final int index,final int fieldCount) {
        assert index < (1 << fieldCount);
        final boolean[] row =  new boolean[fieldCount];
        for (int i = 0; i < fieldCount; i++) {
            row[i] = ((1 << i) & index) > 0;
        }
        return row;
    }

    public static int[] indexesWithJset(final int column, final int fieldCount) {
        assert column < fieldCount;
        final int[] indexes = new int[1 << (fieldCount-1)];
        for (int i = 0; i < (1 << (fieldCount-1)); i++) {
            indexes[i] = (i < (1 << column)) ?
                    (1 << column) | i :
                    (((column==0 ? i : (i >> column)) << (column+1))  |
                            (1 << column) |
                            (column==0 ? 0: (i & (1 << column)-1)));
        }
        return indexes;
    }

    public long[] getVectorCounts() {
        return vectorCounts;
    }

    public String toString() {
        return "Similarity Matrix , vectorCounts=" + Arrays.toString(vectorCounts) + "]";
    }

    public static final String[] SIMILARITY_METHOD_NAMES = {
            "jaro_winkler",
            "jaccard_bigrams","jaccard_trigrams",
            "cosine_bigrams","cosine_trigrams",
            "exact"
    };
    public static final String DEFAULT_SIMILARITY_METHOD_NAME = SIMILARITY_METHOD_NAMES[0];


    public static boolean[] recordPairSimilarity(final GenericRecord[] records, final String[] fieldNames) {
        return recordPairSimilarity(records,fieldNames,DEFAULT_SIMILARITY_METHOD_NAME);
    }


    public static boolean[] recordPairSimilarity(final GenericRecord[] records, final String[] fieldNames,
                                                 final String similarityName) {
        final boolean[] vector = new boolean[fieldNames.length];
        int i = 0;
        for (String fieldName : fieldNames) {
            final String v0 =  (String) records[0].get(fieldName);
            final String v1 =  (String) records[1].get(fieldName);
            vector[i++] =  SimilarityMatrix.similarity(similarityName,v0,v1);
        }
        return vector;
    }

    public static boolean similarity(final String name, final String s1, final String s2) {
        if(name.equals("jaro_winkler"))
            return (new JaroWinkler()).similarity(s1,s2) >= 0.70;
        if(name.equals("jaccard_bigrams"))
            return (new Jaccard(2)).similarity(s1,s2) >= 0.8;
        if(name.equals("jaccard_trigrams"))
            return (new Jaccard(3)).similarity(s1,s2) >= 0.8;
        if(name.equals("cosine_bigrams"))
            return (new Cosine(2)).similarity(s1,s2) >= 0.8;
        if(name.equals("cosine_trigrams"))
            return (new Cosine(3)).similarity(s1,s2) >= 0.8;
        if(name.equals("exact"))
            return s1.equals(s2);
        throw new UnsupportedOperationException("Unsupported Matching for name " + name + " .");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SimilarityMatrix matrix = (SimilarityMatrix) o;

        if (fieldCount != matrix.fieldCount) return false;
        return Arrays.equals(vectorCounts, matrix.vectorCounts);

    }

    @Override
    public int hashCode() {
        int result = fieldCount;
        result = 31 * result + Arrays.hashCode(vectorCounts);
        return result;
    }


    public Properties toProperties() {
        final Properties properties = new Properties();
        properties.setProperty("field.count",String.valueOf(fieldCount));
        for (int i = 0; i < vectorCounts.length; i++)
            properties.setProperty(String.format("vec.%d",i),String.valueOf(vectorCounts[i]));
        return properties;
    }

    public void fromProperties(final Properties properties) {
        if(properties.getProperty("field.count") == null)
            throw new IllegalStateException("Requires field.count");

        fieldCount = Integer.valueOf(properties.getProperty("field.count"));
        vectorCounts = new long[1 << fieldCount];
        for (int i = 0; i < vectorCounts.length; i++)
            vectorCounts[i] = Long.valueOf(properties.getProperty(String.format("vec.%d",i)));
    }
}
