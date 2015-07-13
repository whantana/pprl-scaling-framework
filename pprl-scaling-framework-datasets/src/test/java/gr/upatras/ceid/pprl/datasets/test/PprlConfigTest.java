package gr.upatras.ceid.pprl.datasets.test;


import gr.upatras.ceid.pprl.datasets.config.PprlConfig;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.context.annotation.ImportResource;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = PprlConfig.class)
public class PprlConfigTest {

    @Test
    public void pprlConfiguration() {}
}
