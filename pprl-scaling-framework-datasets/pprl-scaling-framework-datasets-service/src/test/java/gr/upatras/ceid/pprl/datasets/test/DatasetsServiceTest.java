package gr.upatras.ceid.pprl.datasets.test;

import gr.upatras.ceid.pprl.datasets.DatasetException;
import gr.upatras.ceid.pprl.datasets.service.DatasetsService;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.hadoop.test.context.HadoopDelegatingSmartContextLoader;
import org.springframework.data.hadoop.test.context.MiniHadoopCluster;
import org.springframework.data.hadoop.test.junit.AbstractMapReduceTests;
import org.springframework.test.context.ContextConfiguration;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@ContextConfiguration(locations = "classpath:datasets-test-context.xml",loader=HadoopDelegatingSmartContextLoader.class)
@MiniHadoopCluster(nodes = 1, id = "datasets_service_test")
public class DatasetsServiceTest extends AbstractMapReduceTests{

    private static Logger LOG = LoggerFactory.getLogger(DatasetsServiceTest.class);

    @Autowired
    private DatasetsService service;

    private Path datasetsFile;

    @Test
    public void test1() throws IOException, DatasetException, URISyntaxException {
        checkForDatasetsFile();
        importLocalAvro();
        dropDataset();
        importAgain();
        dropAgainDataset();
        doubleImport();
    }

    @Test
    public void test2() throws DatasetException, IOException {
        list();
        getSample();
        describe();
        getStats();
    }

//    @Test
//    public void test3() throws Exception {
//        checkForDatasetsFile();
//        dblpImport();
//        list();
//        dblpSample();
//        dblpDescirbe();
//        dblpGetStats();
//    }

    private void describe() throws DatasetException, IOException {

        Map<String,String> schema = service.describeDataset("random1");
        for(Map.Entry<String,String> field : schema.entrySet())
            LOG.info("\t {} {}",field.getKey(),field.getValue());
    }

    private void getSample() throws DatasetException, IOException {
        LOG.info("Sample of random1:");
        for(String sample : service.sampleOfDataset("random1",5)) LOG.info("\t{}", sample);
        LOG.info("Sample of random2:");
        for(String sample : service.sampleOfDataset("random2",5)) LOG.info("\t{}",sample);

    }

    private void list() {
        int i = 1;
        for(String s : service.listDatasets())
            LOG.info("{}.Dataset : {} ",i++,s);
    }

    private void getStats() {
        // TODO implement me
    }


    private void dblpImport() throws Exception {
        File localDblpXml = new File(getClass().getResource("/dblp_sample.xml").toURI());
        File localAvroSchemaFile = new File(getClass().getResource("/dblp_sample.avsc").toURI());
        service.importDblpXmlDataset(localDblpXml,localAvroSchemaFile,"dblp");
        assertTrue(service.listDatasets().size() == 3);

    }

    private void dblpDescirbe() throws DatasetException, IOException {
        Map<String,String> schema = service.describeDataset("dblp");
        for(Map.Entry<String,String> field : schema.entrySet())
            LOG.info("\t {} {}",field.getKey(),field.getValue());
    }

    private void dblpSample() throws DatasetException, IOException {

        LOG.info("Sample of dblp:");
        for(String sample : service.sampleOfDataset("dblp",5)) LOG.info("\t{}", sample);
    }

    private void dblpGetStats() {
        // TODO implement me
    }

    private void checkForDatasetsFile() throws IOException {
        datasetsFile = new Path(getFileSystem().getHomeDirectory() + "/" + ".pprl_datasets");
        assertTrue(getFileSystem().exists(datasetsFile));
        long len = getFileSystem().getFileStatus(datasetsFile).getLen();
        LOG.info("Datasets file exists. Size : {} bytes",len);
    }

    private void importLocalAvro() throws IOException, DatasetException, URISyntaxException {
        File localAvroFile = new File(getClass().getResource("/random.avro").toURI());
        File localAvroSchemaFile = new File(getClass().getResource("/random.avsc").toURI());
        service.importDataset(localAvroFile,localAvroSchemaFile,"random");
        assertTrue(service.listDatasets().size() == 1);
        long len = getFileSystem().getFileStatus(datasetsFile).getLen();
        LOG.info("Datasets file size : {} bytes",len);

    }

    private void dropDataset() throws IOException, DatasetException {
        service.dropDataset("random", false);
        assertTrue(service.listDatasets().size() == 0);
        long len = getFileSystem().getFileStatus(datasetsFile).getLen();
        LOG.info("Datasets file size : {} bytes",len);
        assertTrue(getFileSystem().exists(new Path(getFileSystem().getHomeDirectory() + "/random")));

    }

    private void importAgain() throws IOException, DatasetException, URISyntaxException {
        importLocalAvro();
    }

    private void dropAgainDataset() throws IOException, DatasetException {
        service.dropDataset("random", true);
        assertTrue(service.listDatasets().size() == 0);
        long len = getFileSystem().getFileStatus(datasetsFile).getLen();
        LOG.info("Datasets file size : {} bytes",len);
        assertFalse(getFileSystem().exists(new Path(getFileSystem().getHomeDirectory() + "/random")));

    }

    private void doubleImport() throws IOException, DatasetException, URISyntaxException {
        File localAvroFile = new File(getClass().getResource("/random.avro").toURI());
        File localAvroSchemaFile = new File(getClass().getResource("/random.avsc").toURI());
        service.importDataset(localAvroFile,localAvroSchemaFile,"random1");
        service.importDataset(localAvroFile,localAvroSchemaFile,"random2");
        assertTrue(service.listDatasets().size() == 2);

    }
}
