package gr.upatras.ceid.pprl.service.encoding.config;

import gr.upatras.ceid.pprl.config.PPRLCLusterCondition;
import gr.upatras.ceid.pprl.service.encoding.EncodingService;
import gr.upatras.ceid.pprl.service.encoding.LocalEncodingService;
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