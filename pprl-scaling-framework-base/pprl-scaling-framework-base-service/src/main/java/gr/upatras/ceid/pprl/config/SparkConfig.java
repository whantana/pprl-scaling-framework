package gr.upatras.ceid.pprl.config;

import org.apache.spark.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SparkConfig {

    private static final Logger LOG = LoggerFactory.getLogger(SparkConfig.class);

    @Value("${spark.master}")
    private String sparkMaster;

    @Value("${spark.cores.max}")
    private String sparkCoresMax;

    @Value("${spark.default.parallelism}")
    private String sparkDefaultParallelism;

    @Value("${spark.executor.memory}")
    private String sparkExecutorMemory;

    @Bean
    @Conditional(PPRLCLusterCondition.class)
    public SparkConf sparkConf() {
        LOG.info("Spark-Master : " +
                (isDefined(sparkMaster) ? sparkMaster : "Not provided"));
        return new SparkConf()
                .setMaster(sparkMaster)
                .set("spark.executor.memory", sparkExecutorMemory)
                .set("spark.cores.max", sparkCoresMax)
                .set("spark.default.parallelism", sparkDefaultParallelism);
    }

    private static boolean isDefined(final String str) {
        return str != null && !str.isEmpty();
    }
}
