package gr.upatras.ceid.pprl.service.blocking;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.hadoop.mapreduce.ToolRunner;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Service
public class BlockingService implements InitializingBean {

    private static final Logger LOG = LoggerFactory.getLogger(BlockingService.class);

    public void afterPropertiesSet() {
        if(basePath == null)
            basePath = new Path(hdfs.getHomeDirectory(),"pprl");
        LOG.info(String.format("Blocking service initialized [" +
                "basePath = %s , " +
                "Tool#1 = %s, Tool#2 = %s].",
                basePath,
                (hammingLshBlockingToolRunner!=null),
                (hammingLshFpsBlockingToolRunner!=null)));
    }

    private Path basePath; // PPRL Base Path on the HDFS (pprl-site).

    @Autowired
    private FileSystem hdfs;

    @Autowired
    private ToolRunner hammingLshBlockingToolRunner; // Runner of Hamming LSH BLocking Tool

    @Autowired
    private ToolRunner hammingLshFpsBlockingToolRunner; // Runner of Hamming LSH FPS Blocking Tool


    /**
     * Run Hamming LSH Blocking Tool.
     *
     * @param aliceAvroPath Alice encoded avro data path.
     * @param aliceSchemaPath Alice encoding schema path.
     * @param aliceUidFieldName Alice uid field name.
     * @param bobAvroPath Bob avro path.
     * @param bobSchemaPath Bob encoding schema path.
     * @param bobUidFieldName Bob uid field name.
     * @param blockingName Name of this blacking (as base path).
     * @param L Blocking Groups Count.
     * @param K Hash values Count.
     * @param C Collision Frequency limit.
     * @param R1 Number of reducers for first job.
     * @param R2 Number of reducers for second job.
     * @param R3 Number of reducers for third job.
     * @throws Exception
     */
    public void runHammingLSHBlockingToolRunner(final Path aliceAvroPath, final Path aliceSchemaPath,
                                                final String aliceUidFieldName,
                                                final Path bobAvroPath, final Path bobSchemaPath,
                                                final String bobUidFieldName,
                                                final String blockingName,
                                                final int L, final int K, final short C,
                                                final int R1, final int R2, final int R3)
            throws Exception {
        try {
            final Path blockingPath = new Path(basePath,blockingName);
            hdfs.mkdirs(blockingPath);
            final Path allPairsPath = new Path(blockingPath,"all_pairs");
            final Path frequentPairsPath = new Path(blockingPath,"frequent_pairs");
            final Path matchedPairsPath = new Path(blockingPath,"matched_pairs");
            final Path statsPath = new Path(blockingPath,"stats");
            List<String> argsList = new ArrayList<String>();
            argsList.add(aliceAvroPath.toString());
            argsList.add(aliceSchemaPath.toString());
            argsList.add(aliceUidFieldName);
            argsList.add(bobAvroPath.toString());
            argsList.add(bobSchemaPath.toString());
            argsList.add(bobUidFieldName);
            argsList.add(allPairsPath.toString());
            argsList.add(frequentPairsPath.toString());
            argsList.add(matchedPairsPath.toString());
            argsList.add(statsPath.toString());
            argsList.add(String.valueOf(L));
            argsList.add(String.valueOf(K));
            argsList.add(String.valueOf(C));
            argsList.add(String.valueOf(R1));
            argsList.add(String.valueOf(R2));
            argsList.add(String.valueOf(R3));
            String[] args = new String[argsList.size()];
            args = argsList.toArray(args);
            hammingLshBlockingToolRunner.setArguments(args);
            hammingLshBlockingToolRunner.call();
        } catch (IOException e) {
            LOG.error(e.getMessage());
            throw e;
        } catch (Exception e) {
            LOG.error(e.getMessage());
            throw e;
        }
    }

    /**
     * Run Hamming LSH-FPS Blocking Tool.
     *
     * @param aliceAvroPath
     * @param aliceSchemaPath
     * @param aliceUidFieldName
     * @param bobAvroPath
     * @param bobSchemaPath
     * @param bobUidFieldName
     * @param blockingName
     * @param L
     * @param K
     * @param C
     * @param R1
     * @param R2
     * @param R3
     * @throws Exception
     */
    public void runHammingLSHFPSBlockingToolRuner(final Path aliceAvroPath, final Path aliceSchemaPath,
                                                  final String aliceUidFieldName,
                                                  final Path bobAvroPath, final Path bobSchemaPath,
                                                  final String bobUidFieldName,
                                                  final String blockingName,
                                                  final int L, final int K, final short C,
                                                  final int R1, final int R2, final int R3)
            throws Exception {
        try {
            final Path blockingPath = new Path(basePath,blockingName);
            hdfs.mkdirs(blockingPath);
            final Path bucketsPath = new Path(blockingPath,"buckets");
            final Path frequentPairsPath = new Path(blockingPath,"frequent_pairs");
            final Path matchedPairsPath = new Path(blockingPath,"matched_pairs");
            final Path statsPath = new Path(blockingPath,"stats");
            List<String> argsList = new ArrayList<String>();
            argsList.add(aliceAvroPath.toString());
            argsList.add(aliceSchemaPath.toString());
            argsList.add(aliceUidFieldName);
            argsList.add(bobAvroPath.toString());
            argsList.add(bobSchemaPath.toString());
            argsList.add(bobUidFieldName);
            argsList.add(bucketsPath.toString());
            argsList.add(frequentPairsPath.toString());
            argsList.add(matchedPairsPath.toString());
            argsList.add(statsPath.toString());
            argsList.add(String.valueOf(L));
            argsList.add(String.valueOf(K));
            argsList.add(String.valueOf(C));
            argsList.add(String.valueOf(R1));
            argsList.add(String.valueOf(R2));
            argsList.add(String.valueOf(R3));
            String[] args = new String[argsList.size()];
            args = argsList.toArray(args);
            hammingLshFpsBlockingToolRunner.setArguments(args);
            hammingLshFpsBlockingToolRunner.call();
        } catch (IOException e) {
            LOG.error(e.getMessage());
            throw e;
        } catch (Exception e) {
            LOG.error(e.getMessage());
            throw e;
        }
    }
}
