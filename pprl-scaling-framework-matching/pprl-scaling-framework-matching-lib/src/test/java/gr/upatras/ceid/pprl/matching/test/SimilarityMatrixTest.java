package gr.upatras.ceid.pprl.matching.test;

import gr.upatras.ceid.pprl.matching.SimilarityMatrix;
import gr.upatras.ceid.pprl.matching.test.naive.NaiveSimilarityMatrix;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class SimilarityMatrixTest {

    private static Logger LOG = LoggerFactory.getLogger(SimilarityMatrixTest.class);
    public String[] fieldNames = {"name","surname","location"};
    private GenericRecord[] records;
    private Schema schema;

    @Before
    public void setup() throws IOException {
        schema = loadAvroSchemaFromFile(
                new File("data/person_small/schema/person_small.avsc"));
        records = loadAvroRecordsFromFiles(schema, new File[]{
                new File("data/person_small/avro/person_small.avro")});
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
            int index = SimilarityMatrix.vector2Index(row);
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
            int index = SimilarityMatrix.vector2Index(row);
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
            int index = SimilarityMatrix.vector2Index(row);
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
            int index = SimilarityMatrix.vector2Index(row);
            LOG.info("Row : {}",rowPos);
            LOG.info("vector {} -index-> {}", Arrays.toString(row),index);
            assertEquals(rowPos, index);
            rowPos++;
        }
    }


    @Test
    public void test1() {
        DescriptiveStatistics stats = new DescriptiveStatistics();
        NaiveSimilarityMatrix matrix = null;
        for (int i = 0; i < 5; i++) {
            long start = System.currentTimeMillis();
            matrix = SimilarityMatricesUtil.naiveMatrix(records,fieldNames);
            long end = System.currentTimeMillis();
            long time = end - start;
            stats.addValue(time);
        }
        LOG.info("{} took {} ms.",matrix,stats.getPercentile(50));
    }

    @Test
    public void test2() {
        DescriptiveStatistics stats = new DescriptiveStatistics();
        SimilarityMatrix matrix = null;
        for (int i = 0; i < 5; i++) {
            long start = System.currentTimeMillis();
            matrix = SimilarityMatricesUtil.similarityMatrix(records,fieldNames);
            long end = System.currentTimeMillis();
            long time = end - start;
            stats.addValue(time);
        }
        LOG.info("{} took {} ms.",matrix,stats.getPercentile(50));
    }

    @Test
    public void test3() {
        for (int fieldCount= 1; fieldCount < 6; fieldCount++) {
            for (int i = 0; i < (1 << fieldCount); i++) {
                LOG.info("{} , {}",i,SimilarityMatrix.index2Vector(i,fieldCount));
            }
            LOG.info("\n\n");
        }
    }

    @Test
    public void test4() {
        for(int f=1 ; f <= 5; f++ ) {
            for(int j=0; j < f ; j++) {
                int[] indexes = SimilarityMatrix.indexesWithJset(j, f);
                LOG.info("j=" + j +", f=" + f + " -indexes(" + indexes.length +")->" + Arrays.toString(indexes));
            }
            LOG.info("\n");
        }
    }


    private static Schema loadAvroSchemaFromFile(final File schemaFile) throws IOException {
        FileInputStream fis = new FileInputStream(schemaFile);
        Schema schema = (new Schema.Parser()).parse(fis);
        fis.close();
        return schema;
    }

    private static GenericRecord[] loadAvroRecordsFromFiles(final Schema schema,final File[] avroFiles) throws IOException {
        final List<GenericRecord> recordList =  new ArrayList<GenericRecord>();
        int i = 0;
        for (File avroFile : avroFiles) {
            final DataFileReader<GenericRecord> reader =
                    new DataFileReader<GenericRecord>(avroFile,
                            new GenericDatumReader<GenericRecord>(schema));
            for (GenericRecord record : reader) recordList.add(i++,record);
            reader.close();
        }
        return recordList.toArray(new GenericRecord[recordList.size()]);
    }
}
