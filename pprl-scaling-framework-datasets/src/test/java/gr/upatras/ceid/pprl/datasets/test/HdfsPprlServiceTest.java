package gr.upatras.ceid.pprl.datasets.test;

import gr.upatras.ceid.pprl.datasets.service.HdfsPprlService;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.hadoop.test.context.HadoopDelegatingSmartContextLoader;
import org.springframework.data.hadoop.test.context.MiniHadoopCluster;
import org.springframework.data.hadoop.test.junit.AbstractHadoopClusterTests;
import org.springframework.test.context.ContextConfiguration;

import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


@ContextConfiguration(locations = "classpath:META-INF/spring/datasets-context.xml", loader = HadoopDelegatingSmartContextLoader.class)
@MiniHadoopCluster(nodes = 1, id = "PPRL-Build-Tests")
public class HdfsPprlServiceTest extends AbstractHadoopClusterTests {

    private static final Logger LOG = LoggerFactory.getLogger(HdfsPprlServiceTest.class);

    private HdfsPprlService service;
    private FsPermission perms;

    @Before
    public void setup() throws IOException {
        service = getApplicationContext().getBean("hdfsPprlService", HdfsPprlService.class);
        perms = service.isUserRestricted() ?
                HdfsPprlService.PPRL_USER_GROUP_PERMISSIONS : HdfsPprlService.PPRL_NO_PERMISSIONS;
    }

    @Test
    public void test1() throws IOException {
        service.openHdfs();
        service.makePprlPartyPaths("pprl", new String[]{"pprlA", "pprlB", "pprlC", "pprlD"});
        assertTrue(service.pprlBasePathExists());
        final String lp = service.getLinkingParty();
        assertTrue(lp.equals("pprl"));
        java.util.List<String> dps = Arrays.asList(service.getDataParties());
        assertFalse(dps.contains("pprl"));
        for (String s : new String[]{"pprlA", "pprlB", "pprlC", "pprlD"}) {
            assertTrue(dps.contains(s));
        }
        service.closeHdfs();
    }

    @Test
    public void test2() throws IOException { // make directories
        service.openHdfs();
        service.deletePprlBasePath();
        assertFalse(service.pprlBasePathExists());
        service.closeHdfs();
    }

    @Test
    public void test3() throws IOException {
        service.openHdfs();
        String username = System.getProperty("user.name");
        service.makePprlPartyPaths(username, new String[]{"pprlA", "pprlB", "pprlC", "pprlD"});
        assertTrue(service.pprlBasePathExists());
        final String lp = service.getLinkingParty();
        assertTrue(lp.equals(username));
        java.util.List<String> dps = Arrays.asList(service.getDataParties());
        assertFalse(dps.contains(username));
        for (String s : new String[]{"pprlA", "pprlB", "pprlC", "pprlD"}) {
            assertTrue(dps.contains(s));
        }
        String fileName = "/Users/whantana/whantana.workspaces/intellij-idea/" +
                "pprl-scaling-framework/pprl-scaling-framework-datasets/src/test/resources/sample.xml";
        service.uploadFile(username, fileName);
        final String files[] = service.listUploadedFiles(username);
        for (String f : files) {
            LOG.info(f);
        }
        service.closeHdfs();
    }


    @Test
    public void test4() throws IOException {
        service.openHdfs();
        String username = System.getProperty("user.name");
        service.deleteUploadedFile(username, "sample.xml");
        final String files[] = service.listUploadedFiles(username);
        assertTrue(files.length == 0);
    }
}


