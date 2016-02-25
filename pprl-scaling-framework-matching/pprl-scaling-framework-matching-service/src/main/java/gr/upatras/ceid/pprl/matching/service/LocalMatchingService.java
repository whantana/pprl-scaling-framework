package gr.upatras.ceid.pprl.matching.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Service;

@Service
public class LocalMatchingService implements InitializingBean {

    private static final Logger LOG = LoggerFactory.getLogger(LocalMatchingService.class);

    public void afterPropertiesSet() {
        LOG.info("Local Matching service initialized.");
    }
}
