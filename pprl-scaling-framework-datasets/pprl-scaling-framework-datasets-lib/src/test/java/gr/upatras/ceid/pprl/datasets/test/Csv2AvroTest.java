package gr.upatras.ceid.pprl.datasets.test;


import gr.upatras.ceid.pprl.datasets.DatasetException;
import gr.upatras.ceid.pprl.datasets.DatasetsUtil;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class Csv2AvroTest {

    private static final Logger LOG = LoggerFactory.getLogger(UpdateWithULIDTest.class);


    private static final String[] SMALL_HEADER = {"id","name","surname","location"};
    private static final String[] MED_HEADER  =  {"id","name","surname","age"};
    private static final String[] BIG_HEADER  =  {"id","name","surname","gender"};

    private static Schema.Type[] SMALL_TYPES = {
            Schema.Type.LONG,Schema.Type.STRING,
            Schema.Type.STRING,Schema.Type.STRING
    };
    private static String[] SMALL_DOCS = {
            "unique id","First name","Last name","State"
    };

    private static Schema.Type[] MED_TYPES = {
            Schema.Type.LONG,Schema.Type.STRING,
            Schema.Type.STRING,Schema.Type.INT
    };
    private static String[] MED_DOCS = {
            "unique id","First name","Last name","Age"
    };


    private static Schema.Type[] BIG_TYPES = {
            Schema.Type.LONG,Schema.Type.STRING,
            Schema.Type.STRING,Schema.Type.STRING
    };
    private static String[] BIG_DOCS = {
            "unique id","First name","Last name","Gender"
    };

    @Test
    public void test1() throws IOException, DatasetException {
        Schema schema = DatasetsUtil.avroSchema("person_small","People","pprl.datasets",SMALL_HEADER,SMALL_TYPES,SMALL_DOCS);
        final FileSystem fs = FileSystem.getLocal(new Configuration());
        final Path p = DatasetsUtil.csv2avro(fs,schema,new Path(fs.getWorkingDirectory(),"data"),
                new Path(fs.getWorkingDirectory(), "data/person_small/csv/person_small.csv"));
        LOG.info("Saved at path {} ", p);
    }

    @Test
    public void test2() throws IOException, DatasetException {
        Schema schema = DatasetsUtil.avroSchema("person_medium","People","pprl.datasets",MED_HEADER,MED_TYPES,MED_DOCS);
        final FileSystem fs = FileSystem.getLocal(new Configuration());
        final Path p = DatasetsUtil.csv2avro(fs,schema,new Path(fs.getWorkingDirectory(),"data"),
                new Path(fs.getWorkingDirectory(), "data/person_medium/csv/person_medium.csv"));
        LOG.info("Saved at path {} ", p);
    }

    @Test
    public void test3() throws IOException, DatasetException {
        Schema schema = DatasetsUtil.avroSchema("person_big","People","pprl.datasets",BIG_HEADER,BIG_TYPES,BIG_DOCS);
        final FileSystem fs = FileSystem.getLocal(new Configuration());
        final Path p = DatasetsUtil.csv2avro(fs,schema,new Path(fs.getWorkingDirectory(),"data"),
                new Path(fs.getWorkingDirectory(), "data/person_big/csv/person_big.csv"));
        LOG.info("Saved at path {} ", p);
    }
}
