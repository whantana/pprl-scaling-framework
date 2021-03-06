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
import java.util.Arrays;
import java.util.Map;

@Configuration
@EnableHadoop
public class HadoopYarnConfig extends SpringHadoopConfigurerAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(HadoopYarnConfig.class);

    @Value("${local.resources}")
    private String localResources;

    @Value("${namenode.host}")
    private String namenodeHost;

    @Value("${resourcemanager.host}")
    private String resourcemanagerHost;

    @Value("${jobhistory.host}")
    private String jobhistoryHost;

    @Override
    public void configure(HadoopConfigConfigurer config) throws Exception {
        if(isDefined(localResources)) {
            LOG.info("Local Resources : " + localResources);
            if (localResources.contains(",")) {
                config.withResources().resources(
                    Arrays.asList(localResources.split(","))
                );
            } else {
                config.withResources().resource(localResources);
            }
        }

        config.withProperties()
                .property("mapreduce.job.user.classpath.first", "true")
                .property("mapreduce.map.java.opts", "-javaagent:./classmexer-0.0.3.jar")
                .property("mapreduce.map.maxattempts", "1")
                .property("mapreduce.reduce.maxattempts", "1")
                .property("mapreduce.reduce.java.opts", "-javaagent:./classmexer-0.0.3.jar");


        if(isDefined(namenodeHost)) {
            final String fsUri = String.format("hdfs://%s:8020", namenodeHost);
            LOG.info("FsUri : " + fsUri);
            config.fileSystemUri(fsUri);

        }

        if(isDefined(resourcemanagerHost)) {
            final String rmAddress = String.format("%s:8032", resourcemanagerHost);
            LOG.info("Resource Manager Address : " + rmAddress);
            config.resourceManagerAddress(rmAddress);
        }

        if(isDefined(jobhistoryHost)) {
            final String jobHistoryAddress = String.format("%s:10020", jobhistoryHost);
            LOG.info("MapReduce Job History Address : " + jobHistoryAddress);
            config.jobHistoryAddress(jobHistoryAddress);
        }
    }

    @Bean(name = "hdfs")
    @Conditional(PPRLCLusterCondition.class)
    @Autowired
    public FileSystem fileSystem(final org.apache.hadoop.conf.Configuration conf) throws IOException {
        for(Map.Entry<String,String> c : conf)
            LOG.info(c.getKey() + " : " + c.getValue());
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
