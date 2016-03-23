package gr.upatras.ceid.pprl.encoding.test;

import org.junit.Ignore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.hadoop.test.context.HadoopDelegatingSmartContextLoader;
import org.springframework.data.hadoop.test.context.MiniHadoopCluster;
import org.springframework.data.hadoop.test.junit.AbstractMapReduceTests;
import org.springframework.test.context.ContextConfiguration;

@Ignore
@ContextConfiguration(locations = "classpath:encoding-test-context.xml",loader=HadoopDelegatingSmartContextLoader.class)
//@MiniHadoopCluster(nodes = 1, id = "encoding_service_test")
public class EncodingServiceTest extends AbstractMapReduceTests {}
