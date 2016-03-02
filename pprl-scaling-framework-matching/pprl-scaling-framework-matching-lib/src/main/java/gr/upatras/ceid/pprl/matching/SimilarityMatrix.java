package gr.upatras.ceid.pprl.matching;

import info.debatty.java.stringsimilarity.Cosine;
import info.debatty.java.stringsimilarity.Jaccard;
import info.debatty.java.stringsimilarity.JaroWinkler;

import java.util.Arrays;

public class SimilarityMatrix {
    private int fieldCount;
    private int[] vectorCounts;

    public SimilarityMatrix(int fieldCount) {
        this.fieldCount = fieldCount;
        vectorCounts = new int[1 << fieldCount];
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

    public int[] getVectorCounts() {
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

    public static boolean similarity(final String name, final String s1, final String s2) {
        if(name.equals("jaro_winkler"))
            return (new JaroWinkler()).similarity(s1,s2) >= 0.70;
        if(name.equals("jaccard_bigrams"))
            return (new Jaccard(2)).similarity(s1,s2) >= 0.75;
        if(name.equals("jaccard_trigrams"))
            return (new Jaccard(3)).similarity(s1,s2) >= 0.75;
        if(name.equals("cosine_bigrams"))
            return (new Cosine(2)).similarity(s1,s2) >= 0.75;
        if(name.equals("cosine_trigrams"))
            return (new Cosine(3)).similarity(s1,s2) >= 0.75;
        if(name.equals("exact"))
            return s1.equals(s2);
        throw new UnsupportedOperationException("Unsupported Matching for name " + name + " .");
    }
}
