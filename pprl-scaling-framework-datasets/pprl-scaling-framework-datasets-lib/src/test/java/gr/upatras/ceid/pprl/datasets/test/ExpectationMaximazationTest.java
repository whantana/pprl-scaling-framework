package gr.upatras.ceid.pprl.datasets.test;

import gr.upatras.ceid.pprl.datasets.DatasetException;
import gr.upatras.ceid.pprl.datasets.DatasetsUtil;
import gr.upatras.ceid.pprl.datasets.statistics.ExpectationMaximization;
import gr.upatras.ceid.pprl.datasets.statistics.Gamma;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang.time.StopWatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class ExpectationMaximazationTest {

    private static Logger LOG = LoggerFactory.getLogger(QGramTest.class);
    private static String[] names = {"person_small","person_medium"};
    private static String[][] fieldNames = {
            {"name","surname","location"},
            {"name","surname","age"},
    };



    private GenericRecord[][] allRecords;

    @Before
    public void setup() throws DatasetException, IOException {
        final FileSystem fs = FileSystem.getLocal(new Configuration());
        allRecords = new GenericRecord[names.length][];
        for (int i = 0; i < names.length ; i++) {
            final Schema schema = DatasetsUtil.loadSchemaFromFSPath(
                    fs, new Path(
                        String.format("%s/%s/schema/%s.avsc",fs.getWorkingDirectory(),names[i],names[i])
                    ));
            DatasetsUtil.DatasetRecordReader reader = new DatasetsUtil.DatasetRecordReader(
                    fs,schema, new Path(
                        String.format("%s/%s/avro",fs.getWorkingDirectory(),names[i])
                    ));
            List<GenericRecord> recList = new ArrayList<GenericRecord>();
            while (reader.hasNext()) recList.add(reader.next());
            reader.close();
            allRecords[i] = recList.toArray(new GenericRecord[recList.size()]);
        }
    }

//    @Test
//    public void test0() {
//        LOG.info("value[00] = {}",Gamma.toIndex(new boolean[]{false,false}));
//        LOG.info("value[01] = {}",Gamma.toIndex(new boolean[]{true,false}));
//        LOG.info("value[10] = {}",Gamma.toIndex(new boolean[]{false,true}));
//        LOG.info("value[11] = {}",Gamma.toIndex(new boolean[]{true,true}));
//
//        LOG.info("value[000] = {}",Gamma.toIndex(new boolean[]{false,false,false}));
//        LOG.info("value[001] = {}",Gamma.toIndex(new boolean[]{true,false,false}));
//        LOG.info("value[010] = {}",Gamma.toIndex(new boolean[]{false,true,false}));
//        LOG.info("value[011] = {}",Gamma.toIndex(new boolean[]{true,true,false}));
//        LOG.info("value[100] = {}",Gamma.toIndex(new boolean[]{false,false,true}));
//        LOG.info("value[101] = {}",Gamma.toIndex(new boolean[]{true,false,true}));
//        LOG.info("value[110] = {}",Gamma.toIndex(new boolean[]{false,true,true}));
//        LOG.info("value[111] = {}",Gamma.toIndex(new boolean[]{true,true,true}));
//    }

    @Test
    public void test1() {
        final Gamma[] gammas = new Gamma[names.length];
        final StopWatch sw = new StopWatch();
        for (int i = 0; i < gammas.length ; i++) {
            sw.start();
            gammas[i] = Gamma.createGamma(allRecords[i], fieldNames[i], false);
            sw.stop();
            long time = sw.getTime();
            sw.reset();
            LOG.info(i + " {}",gammas[i]);
            LOG.info(i + ". Took {} ms",time);
        }
    }

    @Test
    public void test2() {
        final Gamma[] gammas = new Gamma[names.length];
        for (int i = 0; i < gammas.length ; i++) {
            gammas[i] = Gamma.createGamma(allRecords[i], fieldNames[i], false);
            ExpectationMaximization estimator = new ExpectationMaximization(fieldNames[i].length,0.9,0.1,0.01);
            estimator.runAlgorithm(gammas[i]);
            LOG.info(estimator.toString());
        }
    }
}
