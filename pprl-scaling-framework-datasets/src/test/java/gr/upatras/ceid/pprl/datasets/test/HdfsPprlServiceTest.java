package gr.upatras.ceid.pprl.datasets.test;

import gr.upatras.ceid.pprl.datasets.config.PprlConfig;
import gr.upatras.ceid.pprl.datasets.service.HdfsPprlService;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.hadoop.test.context.HadoopDelegatingSmartContextLoader;
import org.springframework.data.hadoop.test.context.MiniHadoopCluster;
import org.springframework.data.hadoop.test.junit.AbstractHadoopClusterTests;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.ContextHierarchy;

import java.io.IOException;
import java.util.Map;

import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertNull;


@ContextConfiguration(classes=PprlConfig.class, loader=HadoopDelegatingSmartContextLoader.class)
@MiniHadoopCluster (nodes=1, id="PPRL setup tests")
public class HdfsPprlServiceTest extends AbstractHadoopClusterTests {

    private static final Logger LOG = LoggerFactory.getLogger(HdfsPprlServiceTest.class);


    private HdfsPprlService service;

    @Before
    public void setup() {

        service = getApplicationContext().getBean("hdfsPprlService", HdfsPprlService.class);
        service.setConfiguration(getHadoopCluster().getConfiguration());
        LOG.info("HdfsPprlService.setup()");
    }

    @Test
    public void testServiceSetup() throws IOException, InterruptedException {

        service.setUp();
        for (Map.Entry<String, Path> entry : service.getPprlSetup().entrySet()) {
            assertNotNull(entry.getValue());
            FileSystem fs = getHadoopCluster().getFileSystem();
            assertTrue(fs.exists(entry.getValue()));
        }
        LOG.info("HdfsPprlService.testServiceSetup()");
    }

    @Test
    public void testServiceTearDown() throws IOException, InterruptedException {
        service.tearDown();
        for (Map.Entry<String, Path> entry : service.getPprlSetup().entrySet()) {
            assertNull(entry.getValue());
            FileSystem fs = getHadoopCluster().getFileSystem();
            assertTrue(!fs.exists(new Path("/user/" + entry.getKey())));
        }
        LOG.info("HdfsPprlService.testServiceTearDown()");
    }
}


