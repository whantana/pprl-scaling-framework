package gr.upatras.ceid.pprl.service.blocking;

import gr.upatras.ceid.pprl.blocking.BlockingException;
import gr.upatras.ceid.pprl.blocking.HammingLSHBlocking;
import gr.upatras.ceid.pprl.blocking.HammingLSHBlockingResult;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncodingException;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncodingUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.ByteBuffer;

@Service
public class LocalBlockingService implements InitializingBean {

    private static final Logger LOG = LoggerFactory.getLogger(LocalBlockingService.class);

    @Autowired
    private FileSystem localFs;

    public FileSystem getLocalFs() {
        return localFs;
    }

    public void setLocalFS(FileSystem localFs) {
        this.localFs = localFs;
    }

    public void afterPropertiesSet() {
        LOG.info("Local Blocking service initialized.");
    }

    /**
     * Return new instance of the <code>HammingLSHBlocking </code>.
     *
     * @param L Number of blocking groups.
     * @param K Number of hash values.
     * @param aliceSchema alice encoding schema.
     * @param bobSchema bob encoding schema.
     * @param seed seed for hlsh hashing, wont be used if negative
     * @return new instance of the <code>HammingLSHBlocking </code>.
     * @throws BloomFilterEncodingException
     * @throws BlockingException
     */
    public HammingLSHBlocking newHammingLSHBlockingInstance(final int L, final int K,
                                                            final Schema aliceSchema,
                                                            final Schema bobSchema, final int seed)
            throws BloomFilterEncodingException, BlockingException {

        try {
            if(seed < 0)
                return new HammingLSHBlocking(L, K,
                    BloomFilterEncodingUtil.setupNewInstance(aliceSchema),
                    BloomFilterEncodingUtil.setupNewInstance(bobSchema));
            else
                return new HammingLSHBlocking(L, K, seed,
                    BloomFilterEncodingUtil.setupNewInstance(aliceSchema),
                    BloomFilterEncodingUtil.setupNewInstance(bobSchema));
        } catch (BloomFilterEncodingException | BlockingException e) {
            LOG.error(e.getMessage(),e);
            throw e;
        }
    }

    /**
     * Run the FPS algorithm on Hamming blocking.
     *
     * @param blocking a <code>HammingLSHBlocking </code> instance.
     * @param aliceRecords alice encoded records.
     * @param aliceUidFieldName alice UID field name.
     * @param bobRecords bob encoded records.
     * @param bobUidFieldName bob UID field name.
     * @param C collision limit.
     * @param hammingThreshold similarity threshold.
     * @throws BlockingException
     */
    public void runFPSonHammingBlocking(HammingLSHBlocking blocking,
                                        final GenericRecord[] aliceRecords, final String aliceUidFieldName,
                                        final GenericRecord[] bobRecords, final String bobUidFieldName,
                                        final short C,
                                        final int hammingThreshold) throws BlockingException {
        try {
            blocking.runHLSH(
                    bobRecords, bobUidFieldName);
            blocking.runFPS(
                    aliceRecords, aliceUidFieldName, C, hammingThreshold);
        } catch (BlockingException e) {
            LOG.error(e.getMessage(),e);
            throw e;
        }
    }

    public void saveResult(HammingLSHBlockingResult result, Path blockingOutputPath)
            throws IOException {
        try {
            HammingLSHBlockingResult.saveBlockingResult(localFs, blockingOutputPath, result);
        } catch (IOException e) {
            LOG.error(e.getMessage(),e);
            throw e;
        }
    }
}
