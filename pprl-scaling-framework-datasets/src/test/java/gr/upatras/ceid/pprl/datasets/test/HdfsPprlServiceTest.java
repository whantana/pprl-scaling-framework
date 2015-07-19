package gr.upatras.ceid.pprl.datasets.test;

import gr.upatras.ceid.pprl.datasets.service.HdfsPprlService;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import java.util.Map;

import static org.junit.Assert.*;


@ContextConfiguration(locations = "classpath:datasets-context.xml",loader=HadoopDelegatingSmartContextLoader.class)
@MiniHadoopCluster (nodes=1, id="PPRL-Build-Tests")
public class HdfsPprlServiceTest extends AbstractHadoopClusterTests {

    private static final Logger LOG = LoggerFactory.getLogger(HdfsPprlServiceTest.class);

    private HdfsPprlService service;
    private FileSystem mfs;
    private FsPermission perms;

    @Before
    public void setup() throws IOException {
        service = getApplicationContext().getBean("hdfsPprlService", HdfsPprlService.class);
        mfs = getFileSystem();
        perms = service.isUserRestricted() ?
                HdfsPprlService.PPRL_USER_PERMISSIONS:HdfsPprlService.PPRL_NO_PERMISSSIONS;
    }

    @Test
    public void test1() throws IOException {
        try {
            LOG.info("Fresh format and save");
            beforeFormat();
            formatAndSave();
            afterFormat();
        } catch (Exception e ) {
            LOG.error(e.getMessage());
        } finally {
            service.closeHdfs();
        }
    }


    @Test
    public void test2() throws IOException {
        try {
            LOG.info("Resetting service and loading format from HDFS");
            assertTrue(mfs.exists(service.getPprlFormatPath()));
            reset();
            service.loadFormatFromHdfs();
            afterFormat();
        } catch (Exception e ) {
            LOG.error(e.getMessage());
        } finally {
            service.closeHdfs();
        }
    }

    @Test
    public void test3() throws IOException {
        try {
            LOG.info("Undo format");
            service.undoFormat();
            service.deleteFormatFromHdfs();
            assertFalse(service.validateFormat());
            for(Boolean b : service.getAvailableFormat().values()) {
                assertFalse(b);
            }
            assertFalse(mfs.exists(service.getPprlFormatPath()));
        } catch (Exception e ) {
            LOG.error(e.getMessage());
        } finally {
            service.closeHdfs();
        }
    }

    public void beforeFormat() throws IOException {
        assertNotNull(service.getFormat());
        assertEquals(service.getFormat().keySet().size(), 5);
        assertEquals(service.getLinkingParty(), "pprl");
        assertTrue(service.getFormat().containsKey("pprl"));
        assertTrue(service.getFormat().containsKey("pprlA"));
        assertTrue(service.getFormat().containsKey("pprlB"));
        assertTrue(service.getFormat().containsKey("pprlC"));
        assertTrue(service.getFormat().containsKey("pprlD"));
        for(Path p : service.getFormat().values()) {
            assertNull(p);
        }
        assertFalse(service.validateFormat());
        for(Boolean b : service.getAvailableFormat().values()) {
            assertFalse(b);
        }
        assertFalse(mfs.exists(service.getPprlFormatPath()));
    }

    public void formatAndSave() throws IOException, InterruptedException {
        service.doFormat();
        service.saveFormatOnHdfs();
    }

    public void afterFormat() throws IOException {
        assertNotNull(service.getFormat());
        assertEquals(service.getFormat().keySet().size(), 5);
        assertEquals(service.getLinkingParty(), "pprl");
        assertTrue(service.getFormat().containsKey("pprl"));
        assertTrue(service.getFormat().containsKey("pprlA"));
        assertTrue(service.getFormat().containsKey("pprlB"));
        assertTrue(service.getFormat().containsKey("pprlC"));
        assertTrue(service.getFormat().containsKey("pprlD"));
        assertTrue(service.validateFormat());
        for(Map.Entry<String,Path> entry : service.getFormat().entrySet()) {
            assertNotNull(entry.getValue());
            checkFormatEntry(entry.getKey(),entry.getValue());
        }
        for(Boolean b : service.getAvailableFormat().values()) {
            assertTrue(b);
        }
        assertTrue(mfs.exists(service.getPprlFormatPath()));
        assertEquals(mfs.getFileStatus(service.getPprlFormatPath()).getOwner(), service.getLinkingParty());
        assertEquals(mfs.getFileStatus(service.getPprlFormatPath()).getPermission(),perms);

    }

    private void checkFormatEntry(final String name , final Path p) throws IOException {
        assertEquals(mfs.getFileStatus(p).getOwner(), name);
        assertEquals(mfs.getFileStatus(p).getPermission(),perms);
    }


    private void reset() throws IOException {
        service.closeHdfs();
        service = getApplicationContext().getBean("hdfsPprlService", HdfsPprlService.class);
        perms = service.isUserRestricted() ?
                HdfsPprlService.PPRL_USER_PERMISSIONS:HdfsPprlService.PPRL_NO_PERMISSSIONS;

    }
}


