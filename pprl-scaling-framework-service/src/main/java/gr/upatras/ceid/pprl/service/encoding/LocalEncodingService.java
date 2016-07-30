package gr.upatras.ceid.pprl.service.encoding;


import gr.upatras.ceid.pprl.encoding.BloomFilterEncoding;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncodingException;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Service;


@Service
public class LocalEncodingService implements InitializingBean {

    private static final Logger LOG = LoggerFactory.getLogger(LocalEncodingService.class);

    public void afterPropertiesSet() throws Exception {
        LOG.info("Local Encoding service initialized.");
    }

    /**
     * Encode records.
     *
     * @param records generic avro records array.
     * @param encoding encoding instance.
     * @return encoded generic avro records array.
     * @throws BloomFilterEncodingException
     */
    public GenericRecord[] encodeRecords(final GenericRecord[] records,
                                         final BloomFilterEncoding encoding)
            throws BloomFilterEncodingException {

        try {
            encoding.initialize();
            LOG.info("Encoding scheme \"{}\" ready to encode.",encoding);
            LOG.info("Record count : {}",records.length);
            final GenericRecord[] encodedRecords = new GenericRecord[records.length];

            for (int i = 0; i < records.length; i++)
                encodedRecords[i] = encoding.encodeRecord(records[i]);

            return encodedRecords;
        } catch (BloomFilterEncodingException e) {
            LOG.error(e.getMessage(),e);
            throw e;
        }
    }
}
