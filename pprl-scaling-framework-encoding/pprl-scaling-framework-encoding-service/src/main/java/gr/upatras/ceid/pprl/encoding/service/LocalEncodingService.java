package gr.upatras.ceid.pprl.encoding.service;


import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class LocalEncodingService implements InitializingBean {

    private static final Logger LOG = LoggerFactory.getLogger(LocalEncodingService.class);

    public void afterPropertiesSet() throws Exception {
        LOG.info("Local Encoding service initialized.");
    }

    @Autowired
    private FileSystem localFS;

    public FileSystem getLocalFileSystem() {
        return localFS;
    }

    public void setLocalFileSystem(FileSystem localFileSystem) {
        this.localFS = localFileSystem;
    }
}
