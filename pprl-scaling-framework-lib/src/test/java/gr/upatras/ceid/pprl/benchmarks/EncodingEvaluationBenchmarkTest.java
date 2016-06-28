package gr.upatras.ceid.pprl.benchmarks;


import gr.upatras.ceid.pprl.datasets.DatasetException;
import gr.upatras.ceid.pprl.datasets.DatasetsUtil;
import gr.upatras.ceid.pprl.encoding.BloomFilter;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncodingException;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncodingUtil;
import gr.upatras.ceid.pprl.encoding.CLKEncoding;
import gr.upatras.ceid.pprl.matching.PrivateSimilarityUtil;
import gr.upatras.ceid.pprl.matching.SimilarityUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class EncodingEvaluationBenchmarkTest {

    private static final Logger LOG = LoggerFactory.getLogger(EncodingEvaluationBenchmarkTest.class);

    private static final String[] HEADER = {"id","name","surname","city"};
    private static Schema.Type[] TYPES = {
            Schema.Type.STRING,Schema.Type.STRING,
            Schema.Type.STRING,Schema.Type.STRING
    };
    private static String[] DOCS = {
            "unique id","First name","Last name","City"
    };

    private int[] S = new int[]{512,1024,2048};
    private int[] Q = new int[]{2,3,4};
    private int[] K = new int[]{10,15,30};

	private final int LIMIT = 100;

    @Test
    public void test0() throws IOException, DatasetException {
        final FileSystem fs = FileSystem.getLocal(new Configuration());
        Schema schemaA = DatasetsUtil.avroSchema("person_a", "People", "pprl.datasets", HEADER, TYPES, DOCS);
        Schema schemaB = DatasetsUtil.avroSchema("person_b", "People", "pprl.datasets", HEADER, TYPES, DOCS);
        final Path p = DatasetsUtil.csv2avro(fs,schemaA,"dataA",new Path(fs.getWorkingDirectory(),"data/benchmarks"),
                new Path(fs.getWorkingDirectory(), "data/benchmarks/dataA.csv"));
        LOG.info("Saved at path {} ", p);
        final Path p1 = DatasetsUtil.csv2avro(fs,schemaB,"dataB",new Path(fs.getWorkingDirectory(),"data/benchmarks"),
                new Path(fs.getWorkingDirectory(), "data/benchmarks/dataB.csv"));
        LOG.info("Saved at path {} ", p1);
    }

    @Test
    public void test1() throws DatasetException, IOException, BloomFilterEncodingException {
        final FileSystem fs = FileSystem.getLocal(new Configuration());
        final Schema schemaA =
                DatasetsUtil.loadSchemaFromFSPath(fs,new Path("data/benchmarks/dataA/schema/dataA.avsc"));
        final GenericRecord[] recordsA =
                DatasetsUtil.loadAvroRecordsFromFSPaths(fs,LIMIT,schemaA,new Path("data/benchmarks/dataA/avro"));
        final Schema schemaB =
                DatasetsUtil.loadSchemaFromFSPath(fs,new Path("data/benchmarks/dataB/schema/dataB.avsc"));
        final GenericRecord[] recordsB =
                DatasetsUtil.loadAvroRecordsFromFSPaths(fs,LIMIT,schemaB,new Path("data/benchmarks/dataB/avro"));

        for (int s : S) {
            for (int q : Q) {
                for (int k : K) {
                    CLKEncoding encodingA = new CLKEncoding(s, k, q);
                    encodingA.makeFromSchema(schemaA, new String[]{"name"}, new String[]{"id"});
                    encodingA.initialize();
                    final GenericRecord[] encodedRecordsA = new GenericRecord[recordsA.length];
                    for (int r = 0; r < recordsA.length; r++)
                        encodedRecordsA[r] = encodingA.encodeRecord(recordsA[r]);


                    CLKEncoding encodingB = new CLKEncoding(s, k, q);
                    encodingB.makeFromSchema(schemaA, new String[]{"name"}, new String[]{"id"});
                    encodingB.initialize();
                    final GenericRecord[] encodedRecordsB = new GenericRecord[recordsB.length];
                    for (int r = 0; r < recordsB.length; r++)
                        encodedRecordsB[r] = encodingB.encodeRecord(recordsB[r]);

                    int TM = 0;
                    int FM = 0;
                    int TN = 0;
                    int FN = 0;
					int i = 0;
                    for (GenericRecord erA : encodedRecordsA) {
						int j = 0;
                        for (GenericRecord erB : encodedRecordsB) {
                            final BloomFilter bfA = encodingA.retrieveBloomFilter(erA);
                            final BloomFilter bfB = encodingB.retrieveBloomFilter(erB);
                            String idA = erA.get("id").toString();
                            String idB = erB.get("id").toString();
                            boolean trueSimilar = idA.substring(4, 7).equals(idB.substring(4, 7));
							// boolean trueSimilar = SimilarityUtil.similarity("jaro_winkler",
							// 	recordsA[i].get("name").toString(),recordsB[j].get("name").toString());
                            boolean similar = PrivateSimilarityUtil.similarity("dice", bfA, bfB, 0.5);
                            if (similar && trueSimilar) TM++;
                            else if (similar && !trueSimilar) FM++;
                            else if (!similar && trueSimilar) FN++;
                            else TN++;
							j++;
                        }
						i++;
                    }

                    LOG.info("Encoding settings : {}", String.format("(S=%d,K=%d,Q=%d)", s, k, q));
                    LOG.info("Linkage results : {}", String.format("TM=%d,FM=%d,TN=%d,FN=%d", TM, FM, TN, FN));
                    double recall = (double) TM / (double) (TM + FN);
                    double precision = (double) TM / (double) (TM + FM);
                    LOG.info("Recall : {} Precision : {}", recall, precision);
                }
            }
        }
        LOG.info("DONE");
    }

}
