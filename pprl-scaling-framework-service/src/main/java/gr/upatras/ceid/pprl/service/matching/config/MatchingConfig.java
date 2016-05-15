package gr.upatras.ceid.pprl.service.matching.config;

import gr.upatras.ceid.pprl.config.PPRLCLusterCondition;
import gr.upatras.ceid.pprl.service.matching.LocalMatchingService;
import gr.upatras.ceid.pprl.service.matching.MatchingService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MatchingConfig {

    @Bean
    @Conditional(PPRLCLusterCondition.class)
    public MatchingService matchingService() {
        return new MatchingService();
    }

    @Bean
    public LocalMatchingService localMatchingService() {
        return new LocalMatchingService();
    }
}
