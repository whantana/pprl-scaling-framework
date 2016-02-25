package gr.upatras.ceid.pprl.datasets.test;


import gr.upatras.ceid.pprl.datasets.DatasetException;
import gr.upatras.ceid.pprl.datasets.DatasetsUtil;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;

public class Csv2AvroTest {
    private static final String[] SMALL_HEADER = {"id","name","surname","location"};
    private static final String[] MED_HEADER = {"id","name","surname","age"};
    private static final String[] BIG_HEADER = {"id","name","surname","gender"};

    private static Schema.Type[] SMALL_TYPES = {
            Schema.Type.STRING,Schema.Type.STRING,
            Schema.Type.STRING,Schema.Type.STRING
    };

    private static Schema.Type[] MED_TYPES = {
            Schema.Type.STRING,Schema.Type.STRING,
            Schema.Type.STRING,Schema.Type.INT
    };

    private static Schema.Type[] BIG_TYPES = {
            Schema.Type.STRING,Schema.Type.STRING,
            Schema.Type.STRING,Schema.Type.STRING
    };

    @Test
    public void test1() throws IOException, DatasetException {
        Schema schema = DatasetsUtil.avroSchema("person_small",SMALL_HEADER,SMALL_TYPES);
        final FileSystem fs = FileSystem.getLocal(new Configuration());
        DatasetsUtil.csv2avro(fs,schema,new Path(fs.getWorkingDirectory() + "/data_small.csv"));
    }

    @Test
    public void test2() throws IOException, DatasetException {
        Schema schema = DatasetsUtil.avroSchema("person_medium",MED_HEADER,MED_TYPES);
        final FileSystem fs = FileSystem.getLocal(new Configuration());
        DatasetsUtil.csv2avro(fs,schema,new Path(fs.getWorkingDirectory() + "/data_medium.csv"));
    }

    @Test
    public void test3() throws IOException, DatasetException {
        Schema schema = DatasetsUtil.avroSchema("person_big",BIG_HEADER,BIG_TYPES);
        final FileSystem fs = FileSystem.getLocal(new Configuration());
        DatasetsUtil.csv2avro(fs,schema,new Path(fs.getWorkingDirectory() + "/data_big.csv"));
    }
}
