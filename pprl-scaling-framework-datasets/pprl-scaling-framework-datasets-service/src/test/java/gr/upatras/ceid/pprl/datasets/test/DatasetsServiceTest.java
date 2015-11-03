package gr.upatras.ceid.pprl.datasets.test;

import gr.upatras.ceid.pprl.datasets.service.DatasetsService;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.hadoop.test.context.HadoopDelegatingSmartContextLoader;
import org.springframework.data.hadoop.test.context.MiniHadoopCluster;
import org.springframework.data.hadoop.test.junit.AbstractMapReduceTests;
import org.springframework.test.context.ContextConfiguration;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;

import static org.junit.Assert.*;

// TODO MiniCluster needs a miniHive server for testing
// TODO upload file with permissions service.uploadFileToHdfs(localFile,destination,Permissions);
@Ignore
@ContextConfiguration(locations = "classpath:META-INF/spring/datasets-context.xml", loader = HadoopDelegatingSmartContextLoader.class)
@MiniHadoopCluster(nodes = 1, id = "PPRL-Build")
public class DatasetsServiceTest extends AbstractMapReduceTests {

//    @Autowired
//    private DatasetsService service;

//    @Test
//    public void mkdirsTest() throws IOException {
//        FileSystem hdfs = getFileSystem();
//        final Path home = hdfs.getHomeDirectory();
//        if(!hdfs.exists(home))
//            hdfs.mkdirs(home);
//        service.makeDatasetDirectory("foo",false);
//        service.makeDatasetDirectory("bar",true);
//        assertTrue(hdfs.exists(new Path(home + "/foo")));
//        assertEquals(0, hdfs.listStatus(new Path(home + "/foo")).length);
//        assertTrue(hdfs.exists(new Path(home + "/bar")));
//        assertTrue(hdfs.exists(new Path(home + "/bar/avro")));
//        assertTrue(hdfs.exists(new Path(home + "/bar/schema")));
//    }
//
//    @Test
//    public void uploadTest() throws IOException, URISyntaxException {
//        FileSystem hdfs = getFileSystem();
//        final Path home = hdfs.getHomeDirectory();
//        File localFile = new File(getClass().getResource("/sample.avro").toURI());
//        assertNotNull("Test file missing",localFile);
//        service.makeDatasetDirectory("bar",true);
//        assertTrue(hdfs.exists(new Path(home + "/bar")));
//        assertTrue(hdfs.exists(new Path(home + "/bar/avro")));
//        service.uploadFileToHdfs(localFile,new Path(home + "/bar/avro"));
//        assertTrue(hdfs.exists(new Path(home + "/bar/avro/sample.avro")));
//        localFile = new File(getClass().getResource("sample.avsc").toURI());
//        assertNotNull("Test file missing",localFile);
//        assertTrue(hdfs.exists(new Path(home + "/bar")));
//        assertTrue(hdfs.exists(new Path(home + "/bar/schema")));
//        service.uploadFileToHdfs(localFile,new Path(home + "/bar/schema"));
//        assertTrue(hdfs.exists(new Path(home + "/bar/schema/sample.avsc")));
//    }

//    @Test
//    public void uploadWithPermTest() {
//
//    }
//
//    @Test
//    public void importTest() {
//
//    }

//    @Test
//    public void importDblpTest() throws Exception {
//        FileSystem hdfs = getFileSystem();
//        final Path home = hdfs.getHomeDirectory();
//        service.makeDatasetDirectory("dblp_sample",false);
//
//        File localFile = new File(getClass().getResource("/dblp_sample.xml").toURI());
//        service.uploadFileToHdfs(localFile,new Path(home + "/dblp_sample/xml"));
//        File localSchemaFile = new File(getClass().getResource("/dblp_sample.avsc").toURI());
//        service.uploadFileToHdfs(localSchemaFile,new Path(home + "/dblp_sample/schema"));
//        final Path input = new Path(home + "/dblp_sample/xml");
//        final Path output = new Path(home + "/dblp_sample/avro");
//        service.runDblpXmlToAvroTool(input, output);
//
//        assertTrue(hdfs.exists(output));
//        assertTrue(hdfs.listStatus(output).length == 2);
//    }
}
