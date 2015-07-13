package gr.upatras.ceid.pprl.datasets.config;

import gr.upatras.ceid.pprl.datasets.service.HdfsPprlService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;

import javax.annotation.PostConstruct;
import java.util.List;

@Configuration
@PropertySource({
        "classpath:pprl.properties",
        "classpath:hadoop.properties"
})
@ImportResource("hadoop-context.xml")
public class PprlConfig {

    private static final Logger LOG = LoggerFactory.getLogger(PprlConfig.class);

    @Value("#{'${pprl.data.parties}'.split(',')}")
    private List<String> dataParties;

    @Value("${pprl.with.permissions:true}")
    private Boolean withPermissions;

    @Value("${pprl.linking.party:pprl}")
    private String linkingParty;

    @Autowired
    private org.apache.hadoop.conf.Configuration hadoopConfiguration;

    @PostConstruct
    public void init() {
        LOG.info(toString());
    }

    @Bean(name="hdfsPprlService")
    public HdfsPprlService getHdfsPprlService() {
        HdfsPprlService service = new HdfsPprlService(linkingParty,dataParties,withPermissions);
        service.setConfiguration(hadoopConfiguration);
        return service;
    }

	@Bean
	public static PropertySourcesPlaceholderConfigurer propertyConfigInDev() {
		return new PropertySourcesPlaceholderConfigurer();
	}

    @Override
    public String toString() {
        return "PprlConfig{" +
                "dataParties=" + dataParties +
                ", withPermissions=" + withPermissions +
                ", linkingParty='" + linkingParty + '\'' +
                ", configuration='" + hadoopConfiguration + '\'' +
                '}';
    }
}
