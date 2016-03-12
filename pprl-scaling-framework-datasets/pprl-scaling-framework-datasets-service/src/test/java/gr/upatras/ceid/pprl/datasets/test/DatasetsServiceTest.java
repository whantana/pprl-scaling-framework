package gr.upatras.ceid.pprl.datasets.test;

import gr.upatras.ceid.pprl.datasets.DatasetException;
import gr.upatras.ceid.pprl.datasets.service.DatasetsService;
import org.apache.avro.Schema;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
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

import static org.junit.Assert.assertNotNull;


@ContextConfiguration(locations = "classpath:datasets-test-context.xml", loader=HadoopDelegatingSmartContextLoader.class)
@MiniHadoopCluster(nodes = 1, id = "datasets_service_test")
public class DatasetsServiceTest extends AbstractMapReduceTests {
    private static Logger LOG = LoggerFactory.getLogger(DatasetsServiceTest.class);

    @Autowired
    private DatasetsService es;

    @Value("${build.test.dir}")
    private String cwd;

    private Path[] AVRO_PATHS;
    private Path SCHEMA_PATH;
    private String[] FIELDS =  new String[]{"name", "surname"};


    @Before
    public void setUp() throws IOException {
        LOG.info("Working directory : {}",cwd);
        assertNotNull(es);
        assertNotNull(es.getLocalFs());
        if (es.getLocalFs().getConf() == null) {
            es.getLocalFs().initialize(URI.create("file:///"), getConfiguration());
        }
        AVRO_PATHS = new Path[]{new Path("person_small/avro")};
        SCHEMA_PATH = new Path("person_small/schema/person_small.avsc");
    }

    @Test
    public void test0() throws IOException, DatasetException {
        final Path uploadedPath = es.uploadFiles(AVRO_PATHS, SCHEMA_PATH, "person_small_0");
        LOG.info("Uploaded to path : {} ",uploadedPath);
        final Path uploadedSchemaPath = new Path(uploadedPath,"schema/person_small.avsc");
        final Schema uploadedSchema = es.loadSchema(uploadedSchemaPath);
        LOG.info("Uploaded Schema : {} ",uploadedSchema.toString(true));
    }

    @Test
    public void test1() throws IOException, DatasetException {
        final Path downloadedPath = es.downloadFiles("person_small_0","downloaded_person_small_0");
        LOG.info("Downloaded to path : {} ",downloadedPath);
    }


    @Test
    public void test2() throws Exception {
        final Path xmlPath = new Path("dblp/xml/dblp_sample.xml");
        final Path schemaPath = new Path("dblp/schema/dblp_sample.avsc");
        final Path uploadedPath = es.importDblpXmlDataset(xmlPath,schemaPath,"small_dblp");
        LOG.info("Uploaded to path : {} ",uploadedPath);
        final Path uploadedSchemaPath = new Path(uploadedPath,"schema/dblp_sample.avsc");
        final Schema uploadedSchema = es.loadSchema(uploadedSchemaPath);
        LOG.info("Uploaded Schema : {} ",uploadedSchema.toString(true));
    }

}


//

//
//    @Autowired
//    private LocalDatasetsService localDatasetsService;
//
//
//    private Path datasetsFile;
//
//    private int datasetsCount = 0;
//
//    @Test
//    public void test1() throws IOException, DatasetException, URISyntaxException {
//        checkForDatasetsFile();
//        importLocalAvro();
//        dropDataset();
//        importAgain();
//        dropAgainDataset();
//        doubleImport();
//    }
//
//    @Test
//    public void test2() throws DatasetException, IOException {
//        checkForDatasetsFile();
//        list();
//        getSample();
//        describe();
//        getStats();
//    }
//
//    @Test
//    public void test3() throws Exception {
//        checkForDatasetsFile();
//        multiFileImport();
//        getSampleFromMulti();
//        saveSamples();
//        calcStats();
//    }
//
////    TODO : Look into mini yarn cluster tests for running the dblp import mr tool
////    @Test
////    public void test4() throws Exception {
////        checkForDatasetsFile();
////        dblpImport();
////        list();
////        dblpSample();
////        dblpDescirbe();
////        dblpGetStats();
////    }
//
//    private void describe() throws DatasetException, IOException {
//
//        Map<String,String> schema = service.describeDataset("random1");
//        for(Map.Entry<String,String> field : schema.entrySet())
//            LOG.info("\t {} {}",field.getKey(),field.getValue());
//    }
//
//    private void getSample() throws DatasetException, IOException {
//        LOG.info("Sample(5) of random1:");
//        for(String sample : service.sampleOfDataset("random1",5)) LOG.info("\t{}", sample);
//        LOG.info("Sample(5) of random2:");
//        for(String sample : service.sampleOfDataset("random2",5)) LOG.info("\t{}",sample);
//    }
//
//    private void list() {
//        int i = 1;
//        for(String s : service.listDatasets(false))
//            LOG.info("{}.Dataset : {} ",i++,s);
//    }
//
//    private void getStats() {
//    }
//
//    private void checkForDatasetsFile() throws IOException {
//        if(datasetsFile == null ) {
//            datasetsFile = service.getUserDatasetsFile();
//            LOG.info("Setting datasets file : {}",datasetsFile);
//        }
//        assertTrue(getFileSystem().exists(datasetsFile));
//        long len = getFileSystem().getFileStatus(datasetsFile).getLen();
//        LOG.info("Checking datasets file size : {} bytes",len);
//        datasetsCount = service.listDatasets(true).size();
//        LOG.info("Found {} datasets.",datasetsCount);
//
//    }
//
//    private void importLocalAvro() throws IOException, DatasetException, URISyntaxException {
//        File localAvroFile = new File(getClass().getResource("/random/avro/random.avro").toURI());
//        File localAvroSchemaFile = new File(getClass().getResource("/random/schema/random.avsc").toURI());
//        service.importDataset("random",localAvroSchemaFile,localAvroFile);
//        datasetsCount++;
//        assertEquals(datasetsCount,service.listDatasets(true).size());
//        long len = getFileSystem().getFileStatus(datasetsFile).getLen();
//        LOG.info("After import datasets file size : {} bytes",len);
//    }
//
//    private void dropDataset() throws IOException, DatasetException {
//        service.dropDataset("random", false);
//        datasetsCount--;
//        assertEquals(datasetsCount, service.listDatasets(true).size());
//        long len = getFileSystem().getFileStatus(datasetsFile).getLen();
//        LOG.info("After drop datasets file size : {} bytes",len);
//        assertTrue(getFileSystem().exists(new Path(getFileSystem().getHomeDirectory() + "/random")));
//
//    }
//
//    private void importAgain() throws IOException, DatasetException, URISyntaxException {
//        importLocalAvro();
//    }
//
//    private void dropAgainDataset() throws IOException, DatasetException {
//        dropDataset();
//    }
//
//    private void doubleImport() throws IOException, DatasetException, URISyntaxException {
//        File localAvroFile = new File(getClass().getResource("/random/avro/random.avro").toURI());
//        File localAvroSchemaFile = new File(getClass().getResource("/random/schema/random.avsc").toURI());
//        service.importDataset("random1",localAvroSchemaFile,localAvroFile);
//        service.importDataset("random2",localAvroSchemaFile,localAvroFile);
//        datasetsCount += 2;
//        assertEquals(datasetsCount, service.listDatasets(true).size());
//        long len = getFileSystem().getFileStatus(datasetsFile).getLen();
//        LOG.info("After double import datasets file size : {} bytes",len);
//    }
//
//    private void multiFileImport() throws URISyntaxException, IOException, DatasetException {
//        File localAvroSchemaFile = new File(getClass().getResource("/da_int/schema/da_int.avsc").toURI());
//        File[] localAvroFiles = new File[4];
//        for (int i = 1; i <= 4; i++) {
//            localAvroFiles[i-1] = new File(getClass().getResource(String.format("/da_int/avro/da_int_%d.avro", i)).toURI());
//        }
//        service.importDataset("da_int",localAvroSchemaFile,localAvroFiles);
//        datasetsCount++;
//        assertEquals(datasetsCount, service.listDatasets(true).size());
//        long len = getFileSystem().getFileStatus(datasetsFile).getLen();
//        LOG.info("After import datasets file size : {} bytes",len);
//    }
//
//    private void getSampleFromMulti() throws DatasetException, IOException {
//        LOG.info("Sample(13) of da_int:");
//        for(String sample : service.sampleOfDataset("da_int",13)) LOG.info("\t{}", sample);
//    }
//
//    private void saveSamples() throws DatasetException, IOException, URISyntaxException {
//        service.saveSampleOfDataset("random1", 5, "random1_sample");
//        service.saveSampleOfDataset("random2", 5, "random2_sample");
//        service.saveSampleOfDataset("da_int", 5, "da_int_sample");
//    }
//
//    private void calcStats() throws IOException {
//        File sample1 = new File("random1_sample.avro");
//        File schema1 = new File("random1_sample.avsc");
//        Set<File> files = new HashSet<File>();
//        files.add(sample1);
//        LOG.info("Stats Q=1");
//        final Map<String,double[]> stats1 =
//                localDatasetsService.calculateStatistics(files, schema1, new String[]{"random_int", "random_string"}, 1);
//        for(Map.Entry<String,double[]> entry: stats1.entrySet()) LOG.info("\t{} -> {}",entry.getKey(),entry.getValue());
//        LOG.info("Stats Q=2");
//        final Map<String,double[]> stats2 =
//                localDatasetsService.calculateStatistics(files, schema1, new String[]{"random_int", "random_string"}, 2);
//        for(Map.Entry<String,double[]> entry: stats2.entrySet()) LOG.info("\t{} -> {}",entry.getKey(),entry.getValue());
//        LOG.info("Stats Q=3");
//        final Map<String,double[]> stats3 =
//                localDatasetsService.calculateStatistics(files, schema1, new String[]{"random_int", "random_string"}, 2);
//        for(Map.Entry<String,double[]> entry: stats3.entrySet()) LOG.info("\t{} -> {}",entry.getKey(),entry.getValue());
//    }
