package gr.upatras.ceid.pprl.datasets.test;


import gr.upatras.ceid.pprl.datasets.DatasetException;
import gr.upatras.ceid.pprl.datasets.DatasetStatistics;
import gr.upatras.ceid.pprl.datasets.service.LocalDatasetsService;
import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = "classpath:local-datasets-test-context.xml")
public class LocalDatasetsServiceTest {

    private static Logger LOG = LoggerFactory.getLogger(LocalDatasetsServiceTest.class);

    @Autowired
    private LocalDatasetsService localDatasetsService;

    private File[] DBLP_FILES;
    private File DBLP_SCHEMA_FILE;

    @Before
    public void setUp() throws IOException {
        assertNotNull(localDatasetsService);
        assertNotNull(localDatasetsService.getLocalFileSystem());
        if(localDatasetsService.getLocalFileSystem().getConf() == null) {
            localDatasetsService.getLocalFileSystem().initialize(URI.create("file:///"),new Configuration());
        }

        try {
            DBLP_FILES = retrieveFiles("dblp/avro");
            DBLP_SCHEMA_FILE = new File("dblp/schema/dblp_sample.avsc");
        } catch (IOException e) {
            LOG.error(e.getMessage());
        }
    }

    @Test
    public void test0() throws IOException, DatasetException {
        Map<String,String> description = localDatasetsService.describeLocalDataset(DBLP_SCHEMA_FILE);
        LOG.info(description.toString());
        assertTrue(description.keySet().size() > 0);
        assertTrue(description.values().size() > 0);
    }

    @Test
    public void test1() throws IOException, DatasetException {
        List<String> sample =  localDatasetsService.sampleOfLocalDataset(DBLP_FILES,DBLP_SCHEMA_FILE,3);
        LOG.info(sample.toString());
        assertTrue(sample.size() <= 3);
    }

    @Test
    public void test2() throws IOException, DatasetException {
        Map<String,double[]> stats = localDatasetsService.calculateStatisticsLocalDataset(DBLP_FILES,DBLP_SCHEMA_FILE,new String[]{"author","year"});
        for(String fieldName : stats.keySet()) {
            LOG.info("Field Name {} : ",fieldName);
            LOG.info(DatasetStatistics.prettyStats(stats.get(fieldName)));
        }
        assertTrue(stats.keySet().size() == 2);

    }

    private static File[] retrieveFiles(final String avroPaths) throws IOException {
        File[] avroFiles;
        if(!avroPaths.contains(",")) {
            final File avroFile = new File(avroPaths);
            if (!avroFile.exists())
                throw new IOException("Path \"" + avroPaths + "\" does not exist.");
            if(avroFile.isDirectory()) {
                LOG.info("Avro file input is the directory {}",avroFile);
                avroFiles = avroFile.listFiles(new FilenameFilter() {
                    public boolean accept(File dir, String name) {
                        return name.endsWith(".avro");
                    }
                });
                LOG.info("Found {} avro files in directory {}",avroFiles.length,avroFile);
            } else {
                avroFiles = new File[1];
                avroFiles[0] = avroFile;
            }
        } else {
            String[] paths = avroPaths.split(",");
            avroFiles = new File[paths.length];
            for (int j = 0; j < avroFiles.length; j++) {
                avroFiles[j] = new File(paths[j]);
                if (!avroFiles[j].exists())
                    throw new IOException("Path \"" + avroPaths + "\" does not exist.");
            }
        }
        return avroFiles;
    }
}
