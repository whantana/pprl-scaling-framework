package gr.upatras.ceid.pprl.service.matching;

import gr.upatras.ceid.pprl.combinatorics.CombinatoricsUtil;
import gr.upatras.ceid.pprl.matching.ExpectationMaximization;
import gr.upatras.ceid.pprl.matching.SimilarityMatrix;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.Iterator;

@Service
public class LocalMatchingService implements InitializingBean {

    private static final Logger LOG = LoggerFactory.getLogger(LocalMatchingService.class);

    public void afterPropertiesSet() {
        LOG.info("Local Matching service initialized.");
    }

    public SimilarityMatrix createMatrix(final GenericRecord[] records,
                                         final String[] fieldNames,
                                         final String similarityMethodName)
            throws Exception {
        try {
            final long pairCount = CombinatoricsUtil.twoCombinationsCount(records.length);
            final int fieldCount = fieldNames.length;
            if(Long.compare(pairCount*fieldCount,Integer.MAX_VALUE) > 0)
                throw new UnsupportedOperationException("Cannot create similarity matrix. #N*#F < Integer.MAX");
            LOG.info("Creating similarity matrix");
            final SimilarityMatrix matrix = new SimilarityMatrix(fieldCount);
            int pairsDone = 0;
            final Iterator<int[]> pairIter = CombinatoricsUtil.getPairs(records.length);
            do {
                int pair[] = pairIter.next();
                boolean[] row = new boolean[fieldNames.length];
                for(int j=0; j < fieldNames.length; j++) {
                    String s1 = String.valueOf(records[pair[0]].get(fieldNames[j]));
                    String s2 = String.valueOf(records[pair[1]].get(fieldNames[j]));
                    if(SimilarityMatrix.similarity(similarityMethodName, s1, s2)) row[j] = true;
                }
                matrix.set(row);
                pairsDone++;
                LOG.info("Pairs done : {}.",pairsDone );
            }while(pairIter.hasNext());
            LOG.info("{}",matrix);
            return matrix;
        } catch(Exception e) {
            LOG.error(e.getMessage());
            throw e;
        }
    }

    public SimilarityMatrix createMatrix(final GenericRecord[] records,
                                         final String[] fieldNames)
            throws Exception {
        return createMatrix(records,fieldNames, SimilarityMatrix.DEFAULT_SIMILARITY_METHOD_NAME);
    }

    public SimilarityMatrix createMatrix(final GenericRecord[] recordsA,
                                         final String[] fieldNamesA,
                                         final GenericRecord[] recordsB,
                                         final String[] fieldNamesB,
                                         final String similarityMethodName) throws Exception {
        try{
            assert fieldNamesA.length == fieldNamesB.length;
            final int pairCount = recordsA.length*recordsB.length;
            final int fieldCount = fieldNamesA.length;
            if(Long.compare(pairCount*fieldCount,Integer.MAX_VALUE) > 0)
                throw new UnsupportedOperationException("Cannot create gamma. #N*#F < Integer.MAX");
            LOG.info("Creating similarity matrix");
            final SimilarityMatrix matrix = new SimilarityMatrix(fieldCount);
            int pairsDone = 0;
            for (GenericRecord recA : recordsA) {
                for(GenericRecord recB : recordsB) {
                    boolean[] row = new boolean[fieldNamesA.length];
                    for(int j=0; j < fieldNamesA.length; j++) {
                        String s1 = String.valueOf(recA.get(fieldNamesA[j]));
                        String s2 = String.valueOf(recB.get(fieldNamesB[j]));
                        if(SimilarityMatrix.similarity(similarityMethodName, s1, s2)) row[j] = true;
                    }
                    matrix.set(row);
                    pairsDone++;
                    LOG.info("Pairs done : {}.",pairsDone );
                }
            }
            LOG.info("{}",matrix);
            return matrix;
        } catch(Exception e) {
            LOG.error(e.getMessage());
            throw e;
        }
    }

    public SimilarityMatrix createMatrix(final GenericRecord[] recordsA,
                                         final String[] fieldNamesA,
                                         final GenericRecord[] recordsB,
                                         final String[] fieldNamesB)
            throws Exception {
        return createMatrix(recordsA,fieldNamesA,recordsB,fieldNamesB, SimilarityMatrix.DEFAULT_SIMILARITY_METHOD_NAME);
    }

    public ExpectationMaximization newEMInstance(final String[] fieldNames, final double[] m, final double[] u, double p) {
        LOG.info(String.format("New EM Instance [fieldNames=%s,m=%s,u=%s,p=%f].",
                Arrays.toString(fieldNames),Arrays.toString(m),Arrays.toString(u),p));
        return new ExpectationMaximization(fieldNames,m,u,p);
    }

    public ExpectationMaximization newEMInstance(final String[] fieldNames, final double m, final double u, double p) {
        LOG.info(String.format("New EM Instance [fieldNames=%s,m=%f,u=%f,p=%f].",
                Arrays.toString(fieldNames),m,u,p));
        return new ExpectationMaximization(fieldNames,m,u,p);
    }

    public ExpectationMaximization newEMInstance(final String[] fieldNames) {
        return newEMInstance(fieldNames,0.9,0.9,0.001);
    }
}
