package gr.upatras.ceid.pprl.service.datasets.config;


import gr.upatras.ceid.pprl.config.PPRLCLusterCondition;
import gr.upatras.ceid.pprl.service.datasets.DatasetsService;
import gr.upatras.ceid.pprl.service.datasets.LocalDatasetsService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

@Configuration
public class DatasetsConfig {
    @Bean
    @Conditional(PPRLCLusterCondition.class)
    public DatasetsService datasetsService() {
        return new DatasetsService();
    }

    @Bean
    public LocalDatasetsService localDatasetsService() {
        return new LocalDatasetsService();
    }
}
