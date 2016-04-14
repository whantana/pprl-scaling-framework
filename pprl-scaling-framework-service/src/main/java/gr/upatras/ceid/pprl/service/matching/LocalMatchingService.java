package gr.upatras.ceid.pprl.service.matching;

import gr.upatras.ceid.pprl.combinatorics.CombinatoricsUtil;
import gr.upatras.ceid.pprl.matching.ExpectationMaximization;
import gr.upatras.ceid.pprl.matching.SimilarityUtil;
import gr.upatras.ceid.pprl.matching.SimilarityVectorFrequencies;
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

    /**
     * Returns Similarity vector frequencies.
     *
     * @param records generic avro records array (self-similarity).
     * @param fieldNames field names to check for similarity.
     * @param similarityMethodName similarity method name.
     * @return an array of similarity vector freqs.
     */
    public SimilarityVectorFrequencies vectorFrequencies(final GenericRecord[] records,
                                                         final String[] fieldNames,
                                                         final String similarityMethodName) {
        final long pairCount = CombinatoricsUtil.twoCombinationsCount(records.length);
        final int fieldCount = fieldNames.length;
        if(Long.compare(pairCount*fieldCount,Integer.MAX_VALUE) > 0)
            throw new UnsupportedOperationException("Cannot create similarity frequencies. #N*#F < Integer.MAX");
        LOG.info("Creating similarity frequencies");
        final SimilarityVectorFrequencies frequencies = new SimilarityVectorFrequencies(fieldCount);
        int pairsDone = 0;
        final Iterator<int[]> pairIter = CombinatoricsUtil.getPairs(records.length);
        do {
            int pair[] = pairIter.next();
            boolean[] row = new boolean[fieldNames.length];
            for(int j=0; j < fieldNames.length; j++) {
                String s1 = String.valueOf(records[pair[0]].get(fieldNames[j]));
                String s2 = String.valueOf(records[pair[1]].get(fieldNames[j]));

                if(SimilarityUtil.similarity(similarityMethodName, s1, s2)) row[j] = true;
            }
            frequencies.set(row);
            pairsDone++;
        }while(pairIter.hasNext());
        LOG.info("Pairs done : {}.",pairsDone );
        LOG.info("{}",frequencies);
        return frequencies;
    }

    /**
     * Returns Similarity vector frequencies.
     *
     * @param records generic avro records array (self-similarity).
     * @param fieldNames field names to check for similarity.
     * @return an array of similarity vector freqs.
     * @throws Exception
     */
    public SimilarityVectorFrequencies vectorFrequencies(final GenericRecord[] records,
                                                         final String[] fieldNames) {
        return vectorFrequencies(records, fieldNames, SimilarityUtil.DEFAULT_SIMILARITY_METHOD_NAME);
    }

    /**
     * Returns Similarity vector frequencies.
     *
     * @param recordsA generic avro records array (left dataset).
     * @param fieldNamesA field names to check for similarity (left dataset).
     * @param recordsB generic avro records array (right dataset).
     * @param fieldNamesB field names to check for similarity (right dataset).
     * @param similarityMethodName similarity method name.
     * @return an array of similarity vector freqs.
     * @throws Exception
     */
    public SimilarityVectorFrequencies vectorFrequencies(final GenericRecord[] recordsA,
                                                         final String[] fieldNamesA,
                                                         final GenericRecord[] recordsB,
                                                         final String[] fieldNamesB,
                                                         final String similarityMethodName) {
        assert fieldNamesA.length == fieldNamesB.length;
        final int pairCount = recordsA.length*recordsB.length;
        final int fieldCount = fieldNamesA.length;
        if(Long.compare(pairCount*fieldCount,Integer.MAX_VALUE) > 0)
            throw new UnsupportedOperationException("Cannot create gamma. #N*#F < Integer.MAX");
        LOG.info("Creating similarity matrix");
        final SimilarityVectorFrequencies matrix = new SimilarityVectorFrequencies(fieldCount);
        int pairsDone = 0;
        for (GenericRecord recA : recordsA) {
            for(GenericRecord recB : recordsB) {
                boolean[] row = new boolean[fieldNamesA.length];
                for(int j=0; j < fieldNamesA.length; j++) {
                    String s1 = String.valueOf(recA.get(fieldNamesA[j]));
                    String s2 = String.valueOf(recB.get(fieldNamesB[j]));
                    if(SimilarityUtil.similarity(similarityMethodName, s1, s2)) row[j] = true;
                }
                matrix.set(row);
                pairsDone++;
                LOG.info("Pairs done : {}.",pairsDone );
            }
        }
        LOG.info("{}",matrix);
        return matrix;
    }

    /**
     * Returns Similarity vector frequencies.
     *
     * @param recordsA generic avro records array (left dataset).
     * @param fieldNamesA field names to check for similarity (left dataset).
     * @param recordsB generic avro records array (right dataset).
     * @param fieldNamesB field names to check for similarity (right dataset).
     * @return an array of similarity vector freqs.
     */
    public SimilarityVectorFrequencies vectorFrequencies(final GenericRecord[] recordsA,
                                                         final String[] fieldNamesA,
                                                         final GenericRecord[] recordsB,
                                                         final String[] fieldNamesB) {
        return vectorFrequencies(recordsA, fieldNamesA, recordsB, fieldNamesB, SimilarityUtil.DEFAULT_SIMILARITY_METHOD_NAME);
    }

    /**
     * A new instance of Expecation Maximization estimator.
     *
     * @param fieldNames field names.
     * @param m initial m values.
     * @param u initial u values.
     * @param p initial p value.
     * @return a new EM estimator instance.
     */
    public ExpectationMaximization newEMInstance(final String[] fieldNames, final double[] m, final double[] u, double p) {
        LOG.info(String.format("New EM Instance [fieldNames=%s,m=%s,u=%s,p=%f].",
                Arrays.toString(fieldNames),Arrays.toString(m),Arrays.toString(u),p));
        return new ExpectationMaximization(fieldNames,m,u,p);
    }

    /**
     * A new instance of Expecation Maximization estimator.
     *
     * @param fieldNames field names.
     * @param m initial m value (for all fields).
     * @param u initial u value (for all fields).
     * @param p initial p value.
     * @return a new EM estimator instance.
     */
    public ExpectationMaximization newEMInstance(final String[] fieldNames, final double m, final double u, double p) {
        LOG.info(String.format("New EM Instance [fieldNames=%s,m=%f,u=%f,p=%f].",
                Arrays.toString(fieldNames),m,u,p));
        return new ExpectationMaximization(fieldNames,m,u,p);
    }

    /**
     * A new instance of Expecation Maximization estimator.
     *
     * @param fieldNames field names.
     * @return a new EM estimator instance.
     */
    public ExpectationMaximization newEMInstance(final String[] fieldNames) {
        return newEMInstance(fieldNames,0.9,0.9,0.001);
    }
}
