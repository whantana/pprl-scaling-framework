package gr.upatras.ceid.pprl.test;

import gr.upatras.ceid.pprl.datasets.DatasetException;
import gr.upatras.ceid.pprl.datasets.DatasetsUtil;
import gr.upatras.ceid.pprl.matching.SimilarityMatrix;
import gr.upatras.ceid.pprl.matching.SimilarityUtil;
import gr.upatras.ceid.pprl.matching.SimilarityVectorFrequencies;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class SimilarityMatrixTest {

    private static Logger LOG = LoggerFactory.getLogger(SimilarityMatrixTest.class);
    public String[] fieldNames = {"name","surname","location"};
    private GenericRecord[] records;


    @Before
    public void setup() throws IOException, DatasetException {
        FileSystem fs = FileSystem.getLocal(new Configuration());
        Schema schema = DatasetsUtil.loadSchemaFromFSPath(fs, new Path("data/person_small/schema/person_small.avsc"));
        records = DatasetsUtil.loadAvroRecordsFromFSPaths(fs, schema, new Path("data/person_small/avro/person_small.avro"));
    }

    @Test
    public void test0() {

        boolean[][] rows = new boolean[][]{
                {false}, // 0
                {true},  // 1
        };
        assertEquals(rows.length , (1 << 1));
        int rowPos = 0;
        for(boolean[] row : rows) {
            int index = SimilarityVectorFrequencies.vector2Index(row);
            LOG.info("Row : {}",rowPos);
            LOG.info("vector {} -index-> {}", Arrays.toString(row),index);
            assertEquals(rowPos, index);
            rowPos++;
        }

        rows = new boolean[][]{
                {false,false}, // 0
                {true,false},  // 1
                {false,true},  // 2
                {true,true},   // 3
        };
        assertEquals(rows.length , (1 << 2));
        rowPos = 0;
        for(boolean[] row : rows) {
            int index = SimilarityVectorFrequencies.vector2Index(row);
            LOG.info("Row : {}",rowPos);
            LOG.info("vector {} -index-> {}", Arrays.toString(row),index);
            assertEquals(rowPos, index);
            rowPos++;
        }

        rows = new boolean[][]{
                {false,false,false}, // 0
                {true,false,false},  // 1
                {false,true,false},  // 2
                {true,true,false},   // 3
                {false,false,true},  // 4
                {true,false,true},   // 5
                {false,true,true},   // 6
                {true,true,true}     // 7
        };
        assertEquals(rows.length , (1 << 3));
        rowPos = 0;
        for(boolean[] row : rows) {
            int index = SimilarityVectorFrequencies.vector2Index(row);
            LOG.info("Row : {}",rowPos);
            LOG.info("vector {} -index-> {}", Arrays.toString(row),index);
            assertEquals(rowPos, index);
            rowPos++;
        }

        rows = new boolean[][]{
                {false,false,false,false}, // 0
                {true,false,false,false},  // 1
                {false,true,false,false},  // 2
                {true,true,false,false},   // 3
                {false,false,true,false},  // 4
                {true,false,true,false},   // 5
                {false,true,true,false},   // 6
                {true,true,true,false},    // 7
                {false,false,false,true},  // 8
                {true,false,false,true},   // 9
                {false,true,false,true},   // 10
                {true,true,false,true},    // 11
                {false,false,true,true},   // 12
                {true,false,true,true},    // 13
                {false,true,true,true},    // 14
                {true,true,true,true}      // 15
        };
        assertEquals(rows.length , (1 << 4));
        rowPos = 0;
        for(boolean[] row : rows) {
            int index = SimilarityVectorFrequencies.vector2Index(row);
            LOG.info("Row : {}",rowPos);
            LOG.info("vector {} -index-> {}", Arrays.toString(row),index);
            assertEquals(rowPos, index);
            rowPos++;
        }
    }


    @Test
    public void test1() {
        DescriptiveStatistics stats = new DescriptiveStatistics();
        SimilarityMatrix matrix = null;
        for (int i = 0; i < 5; i++) {
            long start = System.currentTimeMillis();
            matrix = SimilarityUtil.matrix(records, fieldNames);
            long end = System.currentTimeMillis();
            long time = end - start;
            stats.addValue(time);
        }
        LOG.info("{} took {} ms.",matrix,stats.getPercentile(50));
    }

    @Test
    public void test2() {
        DescriptiveStatistics stats = new DescriptiveStatistics();
        SimilarityVectorFrequencies frequencies = null;
        for (int i = 0; i < 5; i++) {
            long start = System.currentTimeMillis();
            frequencies = SimilarityUtil.vectorFrequencies(records, fieldNames);
            long end = System.currentTimeMillis();
            long time = end - start;
            stats.addValue(time);
        }
        LOG.info("{} took {} ms.",frequencies,stats.getPercentile(50));
    }

    @Test
    public void test3() {
        for (int fieldCount= 1; fieldCount < 6; fieldCount++) {
            for (int i = 0; i < (1 << fieldCount); i++) {
                LOG.info("{} , {}",i, SimilarityVectorFrequencies.index2Vector(i, fieldCount));
            }
            LOG.info("\n\n");
        }
    }

    @Test
    public void test4() {
        for(int f=1 ; f <= 5; f++ ) {
            for(int j=0; j < f ; j++) {
                int[] indexes = SimilarityVectorFrequencies.indexesWithJset(j, f);
                LOG.info("j=" + j +", f=" + f + " -indexes(" + indexes.length +")->" + Arrays.toString(indexes));
            }
            LOG.info("\n");
        }
    }

    @Test
    public void test5() throws IOException {
        SimilarityVectorFrequencies frequencies = SimilarityUtil.vectorFrequencies(records, fieldNames);
        Properties properties = frequencies.toProperties();
        properties.store(new FileOutputStream(new File("data/sim_freqs.properties")),"Similarity Vector Frequencies");

        Properties properties1 = new Properties();
        properties1.load(new FileInputStream(new File("data/sim_freqs.properties")));
        SimilarityVectorFrequencies frequencies1 = new SimilarityVectorFrequencies();
        frequencies1.fromProperties(properties1);
        assertEquals(frequencies,frequencies1);
    }
}
