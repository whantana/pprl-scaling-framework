package gr.upatras.ceid.pprl.encoding.test;

import gr.upatras.ceid.pprl.datasets.DatasetException;
import gr.upatras.ceid.pprl.datasets.service.DatasetsService;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncodingException;
import gr.upatras.ceid.pprl.encoding.EncodedDatasetException;
import gr.upatras.ceid.pprl.encoding.service.EncodingService;
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
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@ContextConfiguration(locations = "classpath:encoding-test-context.xml",loader=HadoopDelegatingSmartContextLoader.class)
@MiniHadoopCluster(nodes = 1, id = "encoding_service_test")
public class EncodingServiceTest  extends AbstractMapReduceTests {

    private static Logger LOG = LoggerFactory.getLogger(EncodingServiceTest.class);

    @Autowired
    private DatasetsService datasetsService;

    @Autowired
    private EncodingService encodingService;

    private Path datasetsFile;
    private Path encodingsFile;

    private int datasetsCount = 0;
    private int encodingsCount = 0;

    @Test
    public void test1() throws IOException, DatasetException, URISyntaxException, BloomFilterEncodingException {
        checkForSavingFiles();

        importLocalDatasets("dblp",new String[]{"/dblp/avro/part-m-00000.avro","/dblp/schema/dblp_sample.avsc"});
        importLocalDatasets("random",new String[]{"/random/avro/random.avro","/random/schema/random.avsc"});
        for(String s : datasetsService.listDatasets(false)) {
            LOG.info("\t {}",s);
        }


        importOrphanEncodedDataset("enc_orphan","SIMPLE",1024,30,2,
                new String[]{
                        "/enc_SIMPLE_1024_30_2_author_title/avro/part-m-00000.avro",
                        "/enc_SIMPLE_1024_30_2_author_title/schema/enc_SIMPLE_1024_30_2_author_title.avsc"
                });
        importNotOrphanEncodedDataset(
                "enc_SIMPLE_1024_30_2_author_title","dblp",
                new String[]{"author","title"},"SIMPLE",1024,30,2,
                new String[]{
                        "/enc_SIMPLE_1024_30_2_author_title/avro/part-m-00000.avro",
                        "/enc_SIMPLE_1024_30_2_author_title/schema/enc_SIMPLE_1024_30_2_author_title.avsc"
                }
        );
        for(String s : encodingService.listDatasets(false)) {
            LOG.info("\t {}",s);
        }

        describe("enc_orphan");
        getSample("enc_orphan",5);

        describe("enc_SIMPLE_1024_30_2_author_title");
        getSample("enc_SIMPLE_1024_30_2_author_title",5);
    }

    @Test
    public void test2() throws IOException, DatasetException, BloomFilterEncodingException {
        checkForSavingFiles();
        dropEncoding("enc_orphan");
        for(String s : encodingService.listDatasets(false)) {
            LOG.info("\t {}",s);
        }
    }


    @Test
    public void test3() throws URISyntaxException, BloomFilterEncodingException, DatasetException, IOException {
        saveSamples();
    }

    private void getSample(String name,int size) throws DatasetException, IOException {
        LOG.info("Sample({}) of {}",size,name);
        for(String sample : encodingService.sampleOfDataset(name,size)) LOG.info("\t{}", sample);
    }

    private void describe(String name) throws DatasetException, IOException {
        Map<String,String> schema = encodingService.describeDataset(name);
        for(Map.Entry<String,String> field : schema.entrySet())
            LOG.info("\t {} {}",field.getKey(),field.getValue());
    }

    private void checkForSavingFiles() throws IOException, EncodedDatasetException, BloomFilterEncodingException {
        if(datasetsFile == null ) {
            datasetsFile = datasetsService.getUserDatasetsFile();
            LOG.info("Setting datasets file : {}",datasetsFile);
        }
        if(encodingsFile == null) {
            encodingsFile = encodingService.getUserDatasetsFile();
            LOG.info("Setting encodings file : {}",encodingsFile);
        }

        assertTrue(getFileSystem().exists(datasetsFile));
        assertTrue(getFileSystem().exists(encodingsFile));
        long dlen = getFileSystem().getFileStatus(datasetsFile).getLen();
        LOG.info("Checking datasets file size : {} bytes",dlen);
        long elen = getFileSystem().getFileStatus(encodingsFile).getLen();
        LOG.info("Checking encodings file size : {} bytes",elen);

        datasetsCount = datasetsService.listDatasets(true).size();
        encodingsCount = encodingService.listDatasets(true).size();
        LOG.info("Found {} datasets.",datasetsCount);
        LOG.info("Found {} encodings.",encodingsCount);
    }

    private void importLocalDatasets(final String name, final String[] paths)
            throws URISyntaxException, IOException, DatasetException {
        File localAvroFile = new File(getClass().getResource(paths[0]).toURI());
        File localAvroSchemaFile = new File(getClass().getResource(paths[1]).toURI());
        datasetsService.importDataset(name,localAvroSchemaFile,localAvroFile);
        datasetsCount++;
        assertEquals(datasetsCount,datasetsService.listDatasets(true).size());
        long len = getFileSystem().getFileStatus(datasetsFile).getLen();
        LOG.info("After import datasets file size : {} bytes",len);
    }

    private void importOrphanEncodedDataset(final String name, final String methodName,
                                            final int N, final int K, final int Q ,
                                            final String[] paths)
            throws DatasetException, BloomFilterEncodingException, IOException, URISyntaxException {
        File localAvroFile = new File(getClass().getResource(paths[0]).toURI());
        File localAvroSchemaFile = new File(getClass().getResource(paths[1]).toURI());
        encodingService.importEncodedDatasets(name,methodName,N,K,Q,localAvroSchemaFile,localAvroFile);
        encodingsCount++;
        assertEquals(encodingsCount,encodingService.listDatasets(true).size());
        long len = getFileSystem().getFileStatus(encodingsFile).getLen();
        LOG.info("After import encoded datasets file size : {} bytes",len);
    }

    private void importNotOrphanEncodedDataset(final String name, final String datasetName,
                                               final String[] columns,
                                               final String methodName,
                                               final int N, final int K, final int Q ,
                                               final String[] paths)
            throws URISyntaxException, DatasetException, BloomFilterEncodingException, IOException {
        File localAvroFile = new File(getClass().getResource(paths[0]).toURI());
        File localAvroSchemaFile = new File(getClass().getResource(paths[1]).toURI());
        encodingService.importEncodedDatasets(name, datasetName, Arrays.asList(columns),
                methodName,N,K,Q,localAvroSchemaFile,localAvroFile);
        encodingsCount++;
        assertEquals(encodingsCount,encodingService.listDatasets(true).size());
        long len = getFileSystem().getFileStatus(encodingsFile).getLen();
        LOG.info("After import encoded datasets file size : {} bytes",len);
    }

    private void dropEncoding(String name)
            throws IOException, DatasetException {
        encodingService.dropDataset(name,true);
        encodingsCount--;
        assertEquals(encodingsCount,encodingService.listDatasets(true).size());
        long len = getFileSystem().getFileStatus(encodingsFile).getLen();
        LOG.info("After drop encoded datasets file size : {} bytes",len);
    }

    private void saveSamples() throws DatasetException, IOException, URISyntaxException, BloomFilterEncodingException {
        String parent = new File(getClass().getResource("/dblp/avro").toURI()).getParent();
        File[] dblp = {
                new File(parent,"sample.avsc"),
                new File(parent,"sample.avro")
        };
        dblp[0].createNewFile();
        dblp[1].createNewFile();
        datasetsService.saveSampleOfDataset("dblp", 5, dblp[0], dblp[1]);
        File[] enc_1 = {
            new File(parent,"enc_1.avsc"),
            new File(parent,"enc_1.avro")
        };
        File[] enc_2 = {
                    new File(parent,"enc_2.avsc"),
                    new File(parent,"enc_2.avro")
                };
        File[] enc_3 = {
                    new File(parent,"enc_3.avsc"),
                    new File(parent,"enc_3.avro")
                };
        Set<File> files = new TreeSet<File>();
        files.add(dblp[1]);

        encodingService.encodeLocalFile(Arrays.asList("author"),
                "SIMPLE", 1024, 30, 2, files, dblp[0], enc_1[1], enc_1[0]);
        encodingService.encodeLocalFile(Arrays.asList("author"),
                "MULTI", 1024, 30, 2, files, dblp[0], enc_2[1], enc_2[0]);
        encodingService.encodeLocalFile(Arrays.asList("author"),
                "ROW", 1024, 30, 2, files, dblp[0], enc_3[1], enc_3[0]);
    }

}
