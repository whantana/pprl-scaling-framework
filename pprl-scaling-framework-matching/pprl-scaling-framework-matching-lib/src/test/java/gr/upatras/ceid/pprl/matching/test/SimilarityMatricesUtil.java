package gr.upatras.ceid.pprl.matching.test;

import gr.upatras.ceid.pprl.base.CombinatoricsUtil;
import gr.upatras.ceid.pprl.matching.SimilarityMatrix;
import gr.upatras.ceid.pprl.matching.test.naive.NaiveSimilarityMatrix;
import org.apache.avro.generic.GenericRecord;

import java.util.Iterator;

public class SimilarityMatricesUtil {
    public static SimilarityMatrix similarityMatrix(final GenericRecord[] records,
                                                    final String[] fieldNames,
                                                    final String similarityMethodName) {
        final long pairCount = CombinatoricsUtil.twoCombinationsCount(records.length);
        final int fieldCount = fieldNames.length;
        if(Long.compare(pairCount*fieldCount,Integer.MAX_VALUE) > 0)
            throw new UnsupportedOperationException("Cannot create gamma. #N*#F < Integer.MAX");
        final SimilarityMatrix matrix = new SimilarityMatrix(fieldCount);
        final Iterator<int[]> pairIter = CombinatoricsUtil.getPairs(records.length);
        do {
            int pair[] = pairIter.next();
            boolean[] row = new boolean[fieldCount];
            for(int j = 0 ; j < fieldCount ; j++) {
                String s1 = String.valueOf(records[pair[0]].get(fieldNames[j]));
                String s2 = String.valueOf(records[pair[1]].get(fieldNames[j]));
                if(SimilarityMatrix.similarity(similarityMethodName, s1, s2)) row[j] = true;
            }
            matrix.set(row);

        }while(pairIter.hasNext());
        return matrix;
    }

    public static SimilarityMatrix similarityMatrix(final GenericRecord[] records,
                                                    final String[] fieldNames) {
        return similarityMatrix(records, fieldNames, SimilarityMatrix.DEFAULT_SIMILARITY_METHOD_NAME);
    }

    public static NaiveSimilarityMatrix naiveMatrix(final GenericRecord[] records,
                                                    final String[] fieldNames,
                                                    final String similarityMethodName) {
        final int pairCount = (int)CombinatoricsUtil.twoCombinationsCount(records.length);
        final int fieldCount = fieldNames.length;
        if(Long.compare(pairCount*fieldCount,Integer.MAX_VALUE) > 0)
            throw new UnsupportedOperationException("Cannot create gamma. #N*#F < Integer.MAX");
        final NaiveSimilarityMatrix matrix = new NaiveSimilarityMatrix(pairCount,fieldCount);
        final Iterator<int[]> pairIter = CombinatoricsUtil.getPairs(records.length);
        do {
            int pair[] = pairIter.next();
            long i = CombinatoricsUtil.rankTwoCombination(pair);
            for(int j=0; j < fieldCount; j++) {
                String s1 = String.valueOf(records[pair[0]].get(fieldNames[j]));
                String s2 = String.valueOf(records[pair[1]].get(fieldNames[j]));
                if(NaiveSimilarityMatrix.similarity(similarityMethodName, s1, s2)) matrix.set((int)i, j);
            }
        }while(pairIter.hasNext());
        return matrix;
    }

    public static NaiveSimilarityMatrix naiveMatrix(final GenericRecord[] records,final String[] fieldNames) {
        return naiveMatrix(records,fieldNames, NaiveSimilarityMatrix.DEFAULT_SIMILARITY_METHOD_NAME);
    }
}
