package gr.upatras.ceid.pprl.test;


import gr.upatras.ceid.pprl.blocking.BlockingException;
import gr.upatras.ceid.pprl.blocking.HammingLSHBlocking;
import gr.upatras.ceid.pprl.blocking.RecordIdPair;
import gr.upatras.ceid.pprl.datasets.DatasetException;
import gr.upatras.ceid.pprl.datasets.DatasetsUtil;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncoding;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncodingException;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncodingUtil;
import gr.upatras.ceid.pprl.encoding.CLKEncoding;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.AvroFSInput;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertTrue;

public class HammingLSHBlockingTest {
    private static Logger LOG = LoggerFactory.getLogger(HammingLSHBlockingTest.class);

    private static final String[] VOTER_HEADER = {"id","surname","name","address","city"};

    private static Schema.Type[] VOTER_TYPES = {
            Schema.Type.STRING,Schema.Type.STRING,
            Schema.Type.STRING,Schema.Type.STRING,
            Schema.Type.STRING
    };

    private static String[] VOTER_DOCS = {
            "Voter ID","Surname","Name","Address","City"
    };

    @Test
    public void test1() throws IOException, DatasetException {
        Schema schemaVoters = DatasetsUtil.avroSchema(
                "voters", "Voters Registration", "pprl.datasets",
                VOTER_HEADER,VOTER_TYPES,VOTER_DOCS);
        final FileSystem fs = FileSystem.getLocal(new Configuration());
        final Path pA = DatasetsUtil.csv2avro(fs,schemaVoters,"voters_a",
                new Path(fs.getWorkingDirectory(),"data"),
                new Path(fs.getWorkingDirectory(), "data/voters_a/csv/voters_a.csv"));
        LOG.info("Saved at path {} ", pA);
        final Path pB = DatasetsUtil.csv2avro(fs,schemaVoters,"voters_b",
                new Path(fs.getWorkingDirectory(),"data"),
                new Path(fs.getWorkingDirectory(), "data/voters_b/csv/voters_b.csv"));
        LOG.info("Saved at path {} ", pB);
    }

    @Test
    public void test2() throws BloomFilterEncodingException, IOException {
        final FileSystem fs = FileSystem.getLocal(new Configuration());
        Schema schemaVoters = DatasetsUtil.avroSchema(
                "voters", "Voters Registration", "pprl.datasets",
                VOTER_HEADER,VOTER_TYPES,VOTER_DOCS);
        final String[] SELECTED_FIELDS = {"surname","name","address","city"};
        final String[] REST_FIELDS = {"id"};
        CLKEncoding encoding = new CLKEncoding(1024,10,2);
        encoding.makeFromSchema(schemaVoters, SELECTED_FIELDS, REST_FIELDS);
        assertTrue(encoding.isEncodingOfSchema(schemaVoters));
        encodeLocalFile(fs,"data/voters_a/clk", Collections.singleton(new Path("data/voters_a/avro/voters_a.avro")),
                schemaVoters, encoding);
        encodeLocalFile(fs,"data/voters_b/clk", Collections.singleton(new Path("data/voters_b/avro/voters_b.avro")),
                schemaVoters, encoding);
    }

    @Test
    public void test3()
            throws IOException, DatasetException,
            BloomFilterEncodingException, BlockingException {
        final FileSystem fs = FileSystem.getLocal(new Configuration());
        BloomFilterEncoding encodingA = BloomFilterEncodingUtil.newInstance("CLK");
        BloomFilterEncoding encodingB = BloomFilterEncodingUtil.newInstance("CLK");


        encodingA.setupFromSchema(
                DatasetsUtil.loadSchemaFromFSPath(fs, new Path("data/voters_a/clk.avsc")));
        encodingB.setupFromSchema(
                DatasetsUtil.loadSchemaFromFSPath(fs, new Path("data/voters_b/clk.avsc")));

        final GenericRecord[] recordsA = DatasetsUtil.loadAvroRecordsFromFSPaths(fs, encodingA.getEncodingSchema(),
                new Path("data/voters_a/clk.avro"));
        final GenericRecord[] recordsB = DatasetsUtil.loadAvroRecordsFromFSPaths(fs,encodingB.getEncodingSchema(),
                new Path("data/voters_b/clk.avro"));

        final int LC = 26;
        final int K = 10;
        final int hammingThreshold = 40;
        final int C = 2;
        HammingLSHBlocking blocking = new HammingLSHBlocking(LC,K,encodingA,encodingB);
        blocking.initializeBlockingBuckets();
        LOG.info("Blocking Records...");
        blocking.blockRecords(recordsA, recordsB);
        LOG.info("Blocking Records...DONE");
        LOG.info("Count colisions...");
        blocking.countPairColisions();
        LOG.info("Count colisions...DONE");
        LOG.info("Finding frequent record pairs...");
        final List<RecordIdPair> frequentPairs = blocking.retrieveFrequentPairs(C);
        LOG.info("Frequent pairs list size : {}", frequentPairs.size());
        LOG.info("Do matching in frequent pairs");
        final List<RecordIdPair> matchedPairs = blocking.matchFrequentPairs(
            recordsA,recordsB,frequentPairs,hammingThreshold
        );
        LOG.info("Matched pairs list size : {}", matchedPairs.size());
        int i = 0;
        for(RecordIdPair pair : matchedPairs)
            LOG.debug("{}. {}",++i,pair);

    }

    private static String[] encodeLocalFile(final FileSystem fs ,
                                            final String name, final Set<Path> avroFiles, final Schema schema,
                                            final BloomFilterEncoding encoding)
            throws IOException, BloomFilterEncodingException {
        final Schema encodingSchema = encoding.getEncodingSchema();
        encoding.initialize();

        final File encodedSchemaFile = new File(name + ".avsc");
        encodedSchemaFile.createNewFile();
        final PrintWriter schemaWriter = new PrintWriter(encodedSchemaFile);
        schemaWriter.print(encodingSchema .toString(true));
        schemaWriter.close();

        final File encodedFile = new File(name + ".avro");
        encodedFile.createNewFile();
        final DataFileWriter<GenericRecord> writer =
                new DataFileWriter<GenericRecord>(
                        new GenericDatumWriter<GenericRecord>(encodingSchema));
        writer.create(encodingSchema, encodedFile);
        for (Path p : avroFiles) {
            final long len = fs.getFileStatus(p).getLen();
            final DataFileReader<GenericRecord> reader =
                    new DataFileReader<GenericRecord>(new AvroFSInput(fs.open(p),len),
                            new GenericDatumReader<GenericRecord>(schema));
            for (GenericRecord record : reader) writer.append(encoding.encodeRecord(record));
            reader.close();
        }
        writer.close();

        return new String[]{
                encodedFile.getAbsolutePath(),
                encodedSchemaFile.getAbsolutePath()
        };
    }
}
