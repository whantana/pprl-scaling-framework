package gr.upatras.ceid.pprl.encoding.service.config;

import gr.upatras.ceid.pprl.config.PPRLCLusterCondition;
import gr.upatras.ceid.pprl.encoding.service.EncodingService;
import gr.upatras.ceid.pprl.encoding.service.LocalEncodingService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

@Configuration
public class EncodingConfig {

    @Bean
    @Conditional(PPRLCLusterCondition.class)
    public EncodingService encodingService() {
        return new EncodingService();
    }

    @Bean
    public LocalEncodingService localEncodingService() {
        return new LocalEncodingService();
    }
}