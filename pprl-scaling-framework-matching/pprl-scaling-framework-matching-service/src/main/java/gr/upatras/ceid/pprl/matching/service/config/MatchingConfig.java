package gr.upatras.ceid.pprl.matching.service.config;

import gr.upatras.ceid.pprl.config.PPRLCLusterCondition;
import gr.upatras.ceid.pprl.matching.service.LocalMatchingService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MatchingConfig {

    @Bean
    @Conditional(PPRLCLusterCondition.class)
    public MatchingConfig matchingService() {
        return new MatchingConfig();
    }

    @Bean
    public LocalMatchingService localMatchingService() {
        return new LocalMatchingService ();
    }
}
