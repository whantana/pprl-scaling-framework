package gr.upatras.ceid.pprl.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.core.type.AnnotatedTypeMetadata;

@Configuration
public class PPRLCLusterCondition implements Condition {

    private static final Logger LOG = LoggerFactory.getLogger(PPRLCLusterCondition.class);


    @Autowired
    @Bean(name="isClusterReady")
    public Boolean isClusterReady(final Environment env) {
        boolean defined = areAllDefined(
//                env.getProperty("database.host"),
//                env.getProperty("database.user"),
//                env.getProperty("database.password"),
//                env.getProperty("spark.master"),
                env.getProperty("hadoop.namenode"),
                env.getProperty("yarn.resourcemanager")
        );
        LOG.info(String.format("Application %s cluster ready.",defined ? "is" :"is not"));
//        boolean persistence = areAllDefined(
//                env.getProperty("database.host"),
//                env.getProperty("database.user"),
//                env.getProperty("database.password"));
        boolean hdfs = isDefined(env.getProperty("hadoop.namenode"));
        boolean yarn = isDefined(env.getProperty("yarn.resourcemanager"));
//        boolean spark = isDefined(env.getProperty("spark.master"));

        LOG.info(String.format("Details : [" +
//                        " Persistence : %s," +
//                        " Spark : %s," +
                        " HDFS : %s," +
                        " YARN : %s]",
//                persistence ? "set" :"not set",
//                spark ? "set" :"not set",
                hdfs ? "set" :"not set",
                yarn ? "set" :"not set")
        );
        return defined;
    }

    public boolean matches(ConditionContext ctx, AnnotatedTypeMetadata meta) {
        final Environment env = ctx.getEnvironment();
        return areAllDefined(
//                env.getProperty("database.host"),
//                env.getProperty("database.user"),
//                env.getProperty("database.password"),
//                env.getProperty("spark.master"),
                env.getProperty("hadoop.namenode"),
                env.getProperty("yarn.resourcemanager")
        );
    }

    private static boolean isDefined ( final String str){
        return str != null && !str.isEmpty();
    }

    private static boolean areAllDefined(final String...strs) {
        boolean def = true;
        for(String s : strs) {
            def &= isDefined(s);
        }
        return def;
    }
}

