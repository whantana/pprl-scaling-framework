package gr.upatras.ceid.pprl.test;

import gr.upatras.ceid.pprl.service.datasets.DatasetsService;

import gr.upatras.ceid.pprl.service.encoding.EncodingService;
import gr.upatras.ceid.pprl.service.matching.MatchingService;
import org.junit.Ignore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.hadoop.test.context.HadoopDelegatingSmartContextLoader;
import org.springframework.data.hadoop.test.context.MiniHadoopCluster;
import org.springframework.data.hadoop.test.junit.AbstractMapReduceTests;
import org.springframework.test.context.ContextConfiguration;

@Ignore
@ContextConfiguration(locations = "classpath:services-test-context.xml", loader=HadoopDelegatingSmartContextLoader.class)
@MiniHadoopCluster(nodes = 1, id = "datasets_service_test")
public class ServicesTest extends AbstractMapReduceTests {
    private static Logger LOG = LoggerFactory.getLogger(ServicesTest.class);

    @Autowired
    private DatasetsService ds;

    @Autowired
    private EncodingService es;

    @Autowired
    private MatchingService ms;

//    @Value("${build.test.dir}")
//    private String cwd;
//
//    private Path[] AVRO_PATHS;
//    private Path SCHEMA_PATH;
//    private String[] FIELDS =  new String[]{"name", "surname"};
//
//
//    @Before
//    public void setUp() throws IOException {
//        LOG.info("Working directory : {}",cwd);
//        assertNotNull(ds);
//        assertNotNull(ds.getLocalFs());
//        AVRO_PATHS = new Path[]{new Path("data/person_small/avro")};
//        LOG.debug("Found avro path : {} ",ds.getLocalFs().exists(AVRO_PATHS[0]));
//        SCHEMA_PATH = new Path("data/person_small/schema/person_small.avsc");
//        LOG.debug("Found schema path : {} ",ds.getLocalFs().exists(SCHEMA_PATH));
//    }
//
//    @Test
//    public void test0() throws IOException, DatasetException {
//        final Path uploadedPath = ds.uploadFiles(AVRO_PATHS, SCHEMA_PATH, "person_small");
//        LOG.info("Uploaded to path : {} ",uploadedPath);
//        final Path uploadedSchemaPath = new Path(uploadedPath,"schema/person_small.avsc");
//        final Schema uploadedSchema = ds.loadSchema(uploadedSchemaPath);
//        LOG.info("Uploaded Schema : {} ",uploadedSchema.toString(true));
//    }
//
//    @Test
//    public void test1() throws IOException, DatasetException {
//        final Path downloadedPath = ds.downloadFiles("person_small","person_small_0",new Path("data"));
//        LOG.info("Downloaded to path : {} ",downloadedPath);
//    }
//
//
//    @Test
//    public void test2() throws Exception {
//        final Path xmlPath = new Path("data/dblp/xml/dblp.xml");
//        final Path uploadedPath = ds.importDblpXmlDataset(xmlPath,"small_dblp");
//        LOG.info("Uploaded to path : {} ",uploadedPath);
//        final Path uploadedSchemaPath = new Path(uploadedPath,"schema/small_dblp.avsc");
//        final Schema uploadedSchema = ds.loadSchema(uploadedSchemaPath);
//        LOG.info("Uploaded Schema : {} ",uploadedSchema.toString(true));
//    }
//
//    @Test
//    public void test3() throws Exception {
//        final Path inputPath = new Path("person_small/avro");
//        final Path schemaPath = new Path("person_small/schema/person_small.avsc");
//        final Path basePath = new Path("person_small/stats");
//        final Path propertiesPath = ds.countAvgQgrams(inputPath, schemaPath, basePath, "my_stats", FIELDS);
//        Path p = ds.downloadFiles("person_small","person_small_dl",new Path(cwd));
//        LOG.info("Downloaded at " + p);
//    }

    // TODO test the add ulid
    // TODO BIG TODO MOVE ALL TESTING IN ONE MODULE
}