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

    @Value("${mapred.jobhistory}")
    private String mapredJobHistory;

    @Value("${yarn.mapreduce.framework}")
    private String yarnMapReduceFramework;

    @Value("${local.resources}")
    private String localResources;

    @Override
    public void configure(HadoopConfigConfigurer config) throws Exception {
        LOG.info("HDFS Namenode host : " +
                (isDefined(hadoopNamenode) ? hadoopNamenode : "Not provided"));
        LOG.info("YARN ResourceManager host : " +
                (isDefined(yarnResourceManager) ? yarnResourceManager : "Not provided"));
        LOG.info("YARN MapReduce Framework : " +
                (isDefined(yarnMapReduceFramework) ? yarnMapReduceFramework : "Not provided"));
        LOG.info("MapReduce Job History host : " +
                (isDefined(mapredJobHistory) ? mapredJobHistory : "Not provided"));

        config.fileSystemUri(String.format("hdfs://%s:8020", hadoopNamenode));
        config.resourceManagerAddress(String.format("%s:8032", yarnResourceManager));
        config.jobHistoryAddress(String.format("%s:19888",mapredJobHistory));
        config.withProperties().property("mapreduce.framework.name", yarnMapReduceFramework);
        if(isDefined(localResources)) {
            LOG.info("Local Resources : " + localResources);
            if (localResources.contains(",")) {
                for (String s : localResources.split(","))
                config.withResources().resource(s);
            } else config.withResources().resource(localResources);
        }
    }

    @Bean(name = "hdfs")
    @Conditional(PPRLCLusterCondition.class)
    @Autowired
    public FileSystem fileSystem(final org.apache.hadoop.conf.Configuration conf) throws IOException {
        LOG.info("-START-HADOOP-CONFIGURATION---\n");
        LOG.info(conf.toString());
        LOG.info("-END-HADOOP-CONFIGURATION---\n");
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
