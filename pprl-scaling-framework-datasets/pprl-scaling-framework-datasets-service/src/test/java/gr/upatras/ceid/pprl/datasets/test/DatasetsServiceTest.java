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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@ContextConfiguration(locations = "classpath:datasets-test-context.xml",loader=HadoopDelegatingSmartContextLoader.class)
@MiniHadoopCluster(nodes = 1, id = "datasets_service_test")
public class DatasetsServiceTest extends AbstractMapReduceTests{

    private static Logger LOG = LoggerFactory.getLogger(DatasetsServiceTest.class);

    @Autowired
    private DatasetsService service;

    private Path datasetsFile;

    private int datasetsCount = 0;

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
        checkForDatasetsFile();
        list();
        getSample();
        describe();
        getStats();
    }

    @Test
    public void test3() throws Exception {
        checkForDatasetsFile();
        multiFileImport();
        getSampleFromMulti();
    }
//
//    @Test
//    public void test4() throws Exception {
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
        LOG.info("Sample(5) of random1:");
        for(String sample : service.sampleOfDataset("random1",5)) LOG.info("\t{}", sample);
        LOG.info("Sample(5) of random2:");
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

    private void checkForDatasetsFile() throws IOException {
        if(datasetsFile == null ) {
            datasetsFile = new Path(getFileSystem().getHomeDirectory() + "/" + ".pprl_datasets");
            LOG.info("Setting datasets file : {}",datasetsFile);
        }
        assertTrue(getFileSystem().exists(datasetsFile));
        long len = getFileSystem().getFileStatus(datasetsFile).getLen();
        LOG.info("Checking datasets file size : {} bytes",len);
        datasetsCount = service.listDatasets().size();
        LOG.info("Found {} datasets.",datasetsCount);

    }

    private void importLocalAvro() throws IOException, DatasetException, URISyntaxException {
        File localAvroFile = new File(getClass().getResource("/random.avro").toURI());
        File localAvroSchemaFile = new File(getClass().getResource("/random.avsc").toURI());
        service.importDataset("random",localAvroSchemaFile,localAvroFile);
        datasetsCount++;
        assertEquals(datasetsCount,service.listDatasets().size());
        long len = getFileSystem().getFileStatus(datasetsFile).getLen();
        LOG.info("After import datasets file size : {} bytes",len);

    }

    private void dropDataset() throws IOException, DatasetException {
        service.dropDataset("random", false);
        datasetsCount--;
        assertEquals(datasetsCount, service.listDatasets().size());
        long len = getFileSystem().getFileStatus(datasetsFile).getLen();
        LOG.info("After drop datasets file size : {} bytes",len);
        assertTrue(getFileSystem().exists(new Path(getFileSystem().getHomeDirectory() + "/random")));

    }

    private void importAgain() throws IOException, DatasetException, URISyntaxException {
        importLocalAvro();
    }

    private void dropAgainDataset() throws IOException, DatasetException {
        dropDataset();
    }

    private void doubleImport() throws IOException, DatasetException, URISyntaxException {
        File localAvroFile = new File(getClass().getResource("/random.avro").toURI());
        File localAvroSchemaFile = new File(getClass().getResource("/random.avsc").toURI());
        service.importDataset("random1",localAvroSchemaFile,localAvroFile);
        service.importDataset("random2",localAvroSchemaFile,localAvroFile);
        datasetsCount += 2;
        assertEquals(datasetsCount, service.listDatasets().size());
        long len = getFileSystem().getFileStatus(datasetsFile).getLen();
        LOG.info("After double import datasets file size : {} bytes",len);
    }

    private void multiFileImport() throws URISyntaxException, IOException, DatasetException {
        File localAvroSchemaFile = new File(getClass().getResource("/da_int.avsc").toURI());
        File[] localAvroFiles = new File[4];
        for (int i = 1; i <= 4; i++) {
            localAvroFiles[i-1] = new File(getClass().getResource(String.format("/da_int_%d.avro", i)).toURI());
        }
        service.importDataset("da_int",localAvroSchemaFile,localAvroFiles);
        datasetsCount++;
        assertEquals(datasetsCount, service.listDatasets().size());
        long len = getFileSystem().getFileStatus(datasetsFile).getLen();
        LOG.info("After import datasets file size : {} bytes",len);
    }

    private void getSampleFromMulti() throws DatasetException, IOException {
        LOG.info("Sample(13) of da_int:");
        for(String sample : service.sampleOfDataset("da_int",13)) LOG.info("\t{}", sample);
    }

    private void dblpImport() throws Exception {
        File localDblpXml = new File(getClass().getResource("/dblp_sample.xml").toURI());
        File localAvroSchemaFile = new File(getClass().getResource("/dblp_sample.avsc").toURI());
        service.importDblpXmlDataset("dblp",localAvroSchemaFile,localDblpXml);
        datasetsCount++;
        assertEquals(datasetsCount, service.listDatasets().size());
    }

    private void dblpDescirbe() throws DatasetException, IOException {
        Map<String,String> schema = service.describeDataset("dblp");
        for(Map.Entry<String,String> field : schema.entrySet())
            LOG.info("\t {} {}",field.getKey(),field.getValue());
    }

    private void dblpSample() throws DatasetException, IOException {
        LOG.info("Sample(5) of dblp:");
        for(String sample : service.sampleOfDataset("dblp",5)) LOG.info("\t{}", sample);
    }

    private void dblpGetStats() {
        // TODO implement me
    }
}
