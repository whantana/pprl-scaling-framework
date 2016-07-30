package gr.upatras.ceid.pprl.config;

import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.hadoop.config.annotation.EnableHadoop;
import org.springframework.data.hadoop.config.annotation.SpringHadoopConfigurerAdapter;
import org.springframework.data.hadoop.config.annotation.builders.HadoopConfigConfigurer;

import java.io.IOException;

@Configuration
@EnableHadoop
public class HadoopYarnConfig extends SpringHadoopConfigurerAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(HadoopYarnConfig.class);

    @Value("${hadoop.namenode}")
    private String hadoopNamenode;

    @Value("${yarn.resourcemanager}")
    private String yarnResourceManager;

    @Value("${yarn.mapreduce.framework}")
    private String yarnMapReduceFramework;

    @Value("${yarn.application.classpath}")
    private String yarnApplicationClasspath;

    @Override
    public void configure(HadoopConfigConfigurer config) throws Exception {
        LOG.info("HDFS Namenode : " +
                (isDefined(hadoopNamenode) ? hadoopNamenode : "Not provided"));
        config.fileSystemUri(String.format("hdfs://%s:8020", hadoopNamenode));
        LOG.info("YARN ResourceManager : " +
                (isDefined(yarnResourceManager) ? yarnResourceManager : "Not provided"));
        config.resourceManagerAddress(String.format("%s:8032", yarnResourceManager));
        config
                .withProperties()
                .property("mapreduce.framework.fieldName", yarnMapReduceFramework)
                .property("yarn.nodemanager.aux-services", "mapreduce_shuffle")
//                .property("dfs.client.use.datanode.hostname", "true")
                .property("yarn.application.classpath", yarnApplicationClasspath);
		
		// TODO review spring capabilities for different hadoop vendors 
    }

    @Bean(name = "hdfs")
    @Conditional(PPRLCLusterCondition.class)
    @Autowired
    public FileSystem fileSystem(final org.apache.hadoop.conf.Configuration conf) throws IOException {
        return FileSystem.get(conf);
    }

    @Bean(name = "localFs")
    @Autowired
    public FileSystem localfileSystem(final org.apache.hadoop.conf.Configuration conf) throws IOException {
        return FileSystem.getLocal(conf);
    }

    private static boolean isDefined ( final String str){
        return str != null && !str.isEmpty();
    }
}
