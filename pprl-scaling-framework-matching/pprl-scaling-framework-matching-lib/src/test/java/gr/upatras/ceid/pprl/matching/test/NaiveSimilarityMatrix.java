package gr.upatras.ceid.pprl.matching.test;

import gr.upatras.ceid.pprl.base.CombinatoricsUtil;
import info.debatty.java.stringsimilarity.Cosine;
import info.debatty.java.stringsimilarity.Jaccard;
import info.debatty.java.stringsimilarity.JaroWinkler;

import java.util.BitSet;
import java.util.Iterator;

public class NaiveSimilarityMatrix {
    protected final int totalBits;
    protected final int fieldCount;
    protected final int pairCount;
    protected final int minIndex;
    protected final int maxIndex;
    protected int nnzCount;
    protected BitSet bits;

    public NaiveSimilarityMatrix(final int pairCount, final int fieldCount) {
        this.fieldCount = fieldCount;
        this.pairCount = pairCount;
        totalBits = pairCount*fieldCount;
        bits = new BitSet(totalBits);
        nnzCount = 0;
        minIndex = 0;
        maxIndex = pairCount*fieldCount - 1;
    }

    private void set(int rank,int field,boolean value) {
        int index = rank*fieldCount + field;
        assert index >= minIndex && index <= maxIndex;
        bits.set(index,value);
    }

    public void set(int rank,int field) {
        set(rank,field,true);
        nnzCount++;
    }

    public boolean get(int rank,int field) {
        int index = rank*fieldCount + field;
        assert index >= minIndex && index <= maxIndex;
        return bits.get(index);
    }

    public int getFieldCount() {
        return fieldCount;
    }

    public int getPairCount() {
        return pairCount;
    }

    @Override
    public String toString() {
        return String.format("Similarity Matrix [pairs=%d, fields= %d, minIndex = %d, maxIndex = %d, nnz= %d, nz = %d]",
                pairCount,fieldCount,minIndex,maxIndex,nnzCount,totalBits - nnzCount);
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

    public static NaiveSimilarityMatrix createMatrix(final String[][] records,
                                                    final String similarityMethodName) {
        final int pairCount = CombinatoricsUtil.twoCombinationsCount(records.length);
        final int fieldCount = records[0].length;
        if(Long.compare(pairCount*fieldCount,Integer.MAX_VALUE) > 0)
            throw new UnsupportedOperationException("Cannot create gamma. #N*#F < Integer.MAX");
        final NaiveSimilarityMatrix matrix = new NaiveSimilarityMatrix(pairCount,fieldCount);
        final Iterator<int[]> pairIter = CombinatoricsUtil.getPairs(records.length);
        do {
            int pair[] = pairIter.next();
            int i = CombinatoricsUtil.rankTwoCombination(pair);
            for(int j=0; j < records[0].length; j++) {
                String s1 = records[pair[0]][j];
                String s2 = records[pair[1]][j];
                if(NaiveSimilarityMatrix.similarity(similarityMethodName, s1, s2)) matrix.set(i, j);
            }
        }while(pairIter.hasNext());
        return matrix;
    }

    public static NaiveSimilarityMatrix createMatrix(final String[][] records) {
        return createMatrix(records, NaiveSimilarityMatrix.DEFAULT_SIMILARITY_METHOD_NAME);
    }
}
