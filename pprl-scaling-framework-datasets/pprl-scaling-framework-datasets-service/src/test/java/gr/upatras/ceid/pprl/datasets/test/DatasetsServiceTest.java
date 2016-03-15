package gr.upatras.ceid.pprl.datasets.test;

import gr.upatras.ceid.pprl.datasets.DatasetException;
import gr.upatras.ceid.pprl.datasets.DatasetStatistics;
import gr.upatras.ceid.pprl.datasets.service.DatasetsService;
import org.apache.avro.Schema;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.hadoop.test.context.HadoopDelegatingSmartContextLoader;
import org.springframework.data.hadoop.test.context.MiniHadoopCluster;
import org.springframework.data.hadoop.test.junit.AbstractMapReduceTests;
import org.springframework.test.context.ContextConfiguration;

import java.io.IOException;
import java.net.URI;
import java.util.Properties;

import static org.junit.Assert.assertNotNull;


@ContextConfiguration(locations = "classpath:datasets-test-context.xml", loader=HadoopDelegatingSmartContextLoader.class)
@MiniHadoopCluster(nodes = 1, id = "datasets_service_test")
public class DatasetsServiceTest extends AbstractMapReduceTests {
    private static Logger LOG = LoggerFactory.getLogger(DatasetsServiceTest.class);

    @Autowired
    private DatasetsService ds;

    @Value("${build.test.dir}")
    private String cwd;

    private Path[] AVRO_PATHS;
    private Path SCHEMA_PATH;
    private String[] FIELDS =  new String[]{"name", "surname"};


    @Before
    public void setUp() throws IOException {
        LOG.info("Working directory : {}",cwd);
        assertNotNull(ds);
        assertNotNull(ds.getLocalFs());
        if (ds.getLocalFs().getConf() == null) {
            ds.getLocalFs().initialize(URI.create("file:///"), getConfiguration());
        }
        AVRO_PATHS = new Path[]{new Path("person_small/avro")};
        SCHEMA_PATH = new Path("person_small/schema/person_small.avsc");
    }

    @Test
    public void test0() throws IOException, DatasetException {
        final Path uploadedPath = ds.uploadFiles(AVRO_PATHS, SCHEMA_PATH, "person_small");
        LOG.info("Uploaded to path : {} ",uploadedPath);
        final Path uploadedSchemaPath = new Path(uploadedPath,"schema/person_small.avsc");
        final Schema uploadedSchema = ds.loadSchema(uploadedSchemaPath);
        LOG.info("Uploaded Schema : {} ",uploadedSchema.toString(true));
    }

    @Test
    public void test1() throws IOException, DatasetException {
        final Path downloadedPath = ds.downloadFiles("person_small","person_small_0");
        LOG.info("Downloaded to path : {} ",downloadedPath);
    }


    @Test
    public void test2() throws Exception {
        final Path xmlPath = new Path("dblp/xml/dblp_sample.xml");
        final Path schemaPath = new Path("dblp/schema/dblp_sample.avsc");
        final Path uploadedPath = ds.importDblpXmlDataset(xmlPath,schemaPath,"small_dblp");
        LOG.info("Uploaded to path : {} ",uploadedPath);
        final Path uploadedSchemaPath = new Path(uploadedPath,"schema/dblp_sample.avsc");
        final Schema uploadedSchema = ds.loadSchema(uploadedSchemaPath);
        LOG.info("Uploaded Schema : {} ",uploadedSchema.toString(true));
    }

    @Test
    public void test3() throws Exception {
        final Path inputPath = new Path("person_small/avro");
        final Path schemaPath = new Path("person_small/schema/person_small.avsc");
        final Path basePath = new Path("person_small/stats");
        final String[] fieldNames= {"name","surname","location"};
        final Path propertiesPath = ds.countQGrams(inputPath,schemaPath,basePath,fieldNames);
        DatasetStatistics ds = new DatasetStatistics();
        Properties p = new Properties();
        p.load(getFileSystem().open(propertiesPath));
        ds.fromProperties(p);
        LOG.info(ds.toString());
    }

    // TODO test the add ulid
}