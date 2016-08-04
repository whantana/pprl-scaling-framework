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
    @Autowired
    @Bean(name="isClusterReady")
    public Boolean isClusterReady(final Environment env) {
        return areAllDefined(
                env.getProperty("local.resources"),
                env.getProperty("namenode.host"),
                env.getProperty("resourcemanager.host"),
                env.getProperty("jobhistory.host")
        );
    }

    public boolean matches(ConditionContext ctx, AnnotatedTypeMetadata meta) {
        final Environment env = ctx.getEnvironment();
        return areAllDefined(
                env.getProperty("local.resources"),
                env.getProperty("namenode.host"),
                env.getProperty("resourcemanager.host"),
                env.getProperty("jobhistory.host")
        );
    }

    private static boolean isDefined (final String str){
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

