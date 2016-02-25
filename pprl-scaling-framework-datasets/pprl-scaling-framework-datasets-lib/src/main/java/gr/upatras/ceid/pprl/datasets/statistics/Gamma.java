package gr.upatras.ceid.pprl.datasets.statistics;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.math3.util.Combinations;

import java.util.BitSet;

public class Gamma {
    private final int fieldCount;
    private final int pairCount;
    private BitSet set;

    public Gamma(final int pairCount, final int fieldCount) {
        assert Long.compare(pairCount*fieldCount,Integer.MAX_VALUE) < 0;
        this.fieldCount = fieldCount;
        this.pairCount = pairCount;
        set = new BitSet(pairCount*fieldCount);
    }

    public void set(int rank,int field,boolean value) {
        int index = rank*fieldCount + field;
        assert Long.compare(index,Integer.MAX_VALUE) < 0;
        set.set(index,value);
    }

    public boolean get(int rank,int field) {
        int index = rank*fieldCount + field;
        assert Long.compare(index,Integer.MAX_VALUE) < 0;
        return set.get(index);
    }

    @Override
    public String toString() {
        return String.format("Gamma [pairs=%d , fields= %d]",
                pairCount,fieldCount);
    }



    public static Gamma createGamma(final GenericRecord[] records,
                                    final String[] fieldNames,
                                    final boolean exactMatch) {
        final int pairCount = CombinatoricsUtil.twoCombinationsCount(records.length);
        final int fieldCount = fieldNames.length;
        if(Long.compare(pairCount*fieldCount,Integer.MAX_VALUE) > 0)
            throw new UnsupportedOperationException("Cannot create gamma. #N*#F < Integer.MAX");
        final Gamma gamma = new Gamma(pairCount,fieldCount);
        final Combinations pairs = new Combinations(records.length,2);
        for (int pair[] : pairs) {
            int pairRank = CombinatoricsUtil.rankTwoCombination(pair);
            for(int i=0; i < fieldNames.length; i++) {
                String s1 = String.valueOf(records[pair[0]].get(fieldNames[i]));
                String s2 = String.valueOf(records[pair[1]].get(fieldNames[i]));
                gamma.set(pairRank, i, (exactMatch) ? s1.equals(s2) : (JaroWinkler.compare(s1,s2) > 0.85));
            }
        }
        return gamma;
    }

    public static Gamma createGamma(final GenericRecord[] recordsA,
                                    final String[] fieldNamesA,
                                    final GenericRecord[] recordsB,
                                    final String[] fieldNamesB,
                                    final boolean exactMatch) {
        assert fieldNamesA.length == fieldNamesB.length;
        final int pairCount = recordsA.length*recordsB.length;
        final int fieldCount = fieldNamesA.length;
        if(Long.compare(pairCount*fieldCount,Integer.MAX_VALUE) > 0)
            throw new UnsupportedOperationException("Cannot create gamma. #N*#F < Integer.MAX");
        final Gamma gamma = new Gamma(pairCount,fieldCount);
        int i = 0;
        for (GenericRecord recA : recordsA) {
            for(GenericRecord recB : recordsB) {
                for(int j=0; j < fieldNamesA.length; j++) {
                    String s1 = String.valueOf(recA.get(fieldNamesA[j]));
                    String s2 = String.valueOf(recB.get(fieldNamesB[j]));
                    gamma.set(i,j,(exactMatch) ? s1.equals(s2) : (JaroWinkler.compare(s1,s2) > 0.85));
                }
                i++;
            }
        }
        return gamma;
    }

    private static Gamma createGamma(final String[][] records,
                                     final boolean exactMatch) {
        final int pairCount = CombinatoricsUtil.twoCombinationsCount(records.length);
        final int fieldCount = records[0].length;
        if(Long.compare(pairCount*fieldCount,Integer.MAX_VALUE) > 0)
            throw new UnsupportedOperationException("Cannot create gamma. #N*#F < Integer.MAX");
        final Gamma gamma = new Gamma(pairCount,fieldCount);
        final Combinations pairs = new Combinations(records.length,2);
        for (int pair[] : pairs) {
            int pairRank = CombinatoricsUtil.rankTwoCombination(pair);
            for(int i=0; i < records[0].length; i++) {
                String s1 = records[pair[0]][i];
                String s2 = records[pair[1]][i];
                gamma.set(pairRank,i, (exactMatch) ? s1.equals(s2) : (JaroWinkler.compare(s1,s2) > 0.85));
            }
        }
        return gamma;
    }

    private static Gamma createGamma(final String[][] recordsA,
                                     final String[][] recordsB,
                                     final boolean exactMatch) {
        final int pairCount = recordsA.length*recordsB.length;
        final int fieldCount = recordsA[0].length;
        if(Long.compare(pairCount*fieldCount,Integer.MAX_VALUE) > 0)
            throw new UnsupportedOperationException("Cannot create gamma. #N*#F < Integer.MAX");
        final Gamma gamma = new Gamma(pairCount,fieldCount);
        int i = 0;
        for (String[] recordA : recordsA) {
            for (String[] recordB : recordsB) {
                for (int j = 0; j < recordsA[0].length; j++) {
                    String s1 = recordA[j];
                    String s2 = recordB[j];
                    gamma.set(i,j,(exactMatch) ? s1.equals(s2) : (JaroWinkler.compare(s1,s2) > 0.85));
                }
            }
        }
        return gamma;
    }

    public int getFieldCount() {
        return fieldCount;
    }

    public int getPairCount() {
        return pairCount;
    }
}
