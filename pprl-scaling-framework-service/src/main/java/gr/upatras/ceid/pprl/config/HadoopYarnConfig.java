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
import java.util.Map;

@Configuration
@EnableHadoop
public class HadoopYarnConfig extends SpringHadoopConfigurerAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(HadoopYarnConfig.class);

    @Value("${local.resources}")
    private String localResources;

    @Override
    public void configure(HadoopConfigConfigurer config) throws Exception {
        if(isDefined(localResources)) {
            LOG.info("Local Resources : " + localResources);
            if (localResources.contains(",")) {
                for (String s : localResources.split(",")) {
                    LOG.info("Loading file://" + s);
                    config.withResources().resource("file://"+s);
                }
            } else config.withResources().resource("file://"+localResources);
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
