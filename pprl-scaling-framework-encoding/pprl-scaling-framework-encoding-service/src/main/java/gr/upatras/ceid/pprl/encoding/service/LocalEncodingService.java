package gr.upatras.ceid.pprl.encoding.service;


import gr.upatras.ceid.pprl.encoding.BloomFilterEncoding;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncodingException;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Service;

@Service
public class LocalEncodingService implements InitializingBean {

    private static final Logger LOG = LoggerFactory.getLogger(LocalEncodingService.class);

    public static final FsPermission ONLY_OWNER_PERMISSION
            = new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE, false);


    public void afterPropertiesSet() throws Exception {
        LOG.info("Local Encoding service initialized.");
    }

    public GenericRecord[] encodeRecords(final GenericRecord[] records,
                                         final BloomFilterEncoding encoding)
            throws BloomFilterEncodingException {

        try {
            encoding.initialize();
            final GenericRecord[] encodedRecords = new GenericRecord[records.length];
            for (int i = 0; i < records.length; i++)
                encodedRecords[i] = encoding.encodeRecord(records[i]);
            return encodedRecords;
        } catch (BloomFilterEncodingException e) {
            LOG.error(e.getMessage());
            throw e;
        }
    }
}
