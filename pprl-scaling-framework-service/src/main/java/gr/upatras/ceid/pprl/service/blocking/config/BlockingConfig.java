package gr.upatras.ceid.pprl.service.blocking.config;

import gr.upatras.ceid.pprl.config.PPRLCLusterCondition;
import gr.upatras.ceid.pprl.service.blocking.BlockingService;
import gr.upatras.ceid.pprl.service.blocking.LocalBlockingService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

@Configuration
public class BlockingConfig {
    @Bean
    @Conditional(PPRLCLusterCondition.class)
    public BlockingService blockingService() {
        return new BlockingService();
    }

    @Bean
    public LocalBlockingService localBlockingService() {
        return new LocalBlockingService();
    }
}
