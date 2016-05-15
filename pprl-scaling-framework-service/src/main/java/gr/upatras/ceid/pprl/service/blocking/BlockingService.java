package gr.upatras.ceid.pprl.service.blocking;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Service;

@Service
public class BlockingService implements InitializingBean {

    private static final Logger LOG = LoggerFactory.getLogger(LocalBlockingService.class);

    public void afterPropertiesSet() {
        LOG.info("Blocking service initialized.");
    }
}
