package gr.upatras.ceid.pprl.encoding.service;

import gr.upatras.ceid.pprl.encoding.MultiBloomFilterEncoding;
import gr.upatras.ceid.pprl.encoding.RowBloomFilterEncoding;
import gr.upatras.ceid.pprl.encoding.SimpleBloomFilterEncoding;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Service
public class EncodingService implements InitializingBean {

    private static final Logger LOG = LoggerFactory.getLogger(EncodingService.class);

    public static final Map<String,Class> AVAILABLE_METHODS;
    static {
        AVAILABLE_METHODS = new HashMap<String,Class>();
        AVAILABLE_METHODS.put("SIMPLE", SimpleBloomFilterEncoding.class);
        AVAILABLE_METHODS.put("MULTI", MultiBloomFilterEncoding.class);
        AVAILABLE_METHODS.put("ROW", RowBloomFilterEncoding.class);
    }

    private static final String ENCODINGS_FILE = ".pprl_encodings";

    private static final FsPermission ONLY_OWNER_PERMISSION
            = new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE, false);


    @Autowired
    private FileSystem pprlClusterHdfs;

//    @Autowired
//    private DatasetService datasetService;

    public void afterPropertiesSet() throws Exception {
        // TODO search for existing datasets and retrieve their encodings in each ENCODINGS_FILE
        //LOG.info("Service is now initialized. Found {} datasets on the PPRL site.", datasets.size());
    }
}
