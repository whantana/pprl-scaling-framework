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
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

@Service
public class BlockingService implements InitializingBean {

    private static final Logger LOG = LoggerFactory.getLogger(BlockingService.class);

    public void afterPropertiesSet() {
        if(basePath == null)
            basePath = new Path(hdfs.getHomeDirectory(),"pprl");
        LOG.info(String.format("Blocking service initialized [" +
                        "basePath = %s , " +
                        "Tool#1 = %s, Tool#2 = %s, Tool#3 = %s, Tool#4 = %s].",
                basePath,
                (hammingLshFpsBlockingV0ToolRunner !=null),
                (hammingLshFpsBlockingV1ToolRunner !=null),
                (hammingLshFpsBlockingV2ToolRunner !=null),
                (hammingLshFpsBlockingV3ToolRunner !=null)
        ));
    }

    private Path basePath; // PPRL Base Path on the HDFS (pprl-site).

    @Autowired
    private FileSystem hdfs;

    @Autowired
    private ToolRunner hammingLshFpsBlockingV0ToolRunner; // Runner of Hamming LSH/FPS BLocking Tool v0
    @Autowired
    private ToolRunner hammingLshFpsBlockingV1ToolRunner; // Runner of Hamming LSH/FPS BLocking Tool v1
    @Autowired
    private ToolRunner hammingLshFpsBlockingV2ToolRunner; // Runner of Hamming LSH/FPS BLocking Tool v2
    @Autowired
    private ToolRunner hammingLshFpsBlockingV3ToolRunner; // Runner of Hamming LSH/FPS BLocking Tool v2

    public void runHammingLSHFPSBlockingToolRunner(final String blockingSchemeName,
                                                   final String aliceName,
                                                   final Path aliceAvroPath, final Path aliceSchemaPath,
                                                   final String aliceUidFieldName,
                                                   final String bobName,
                                                   final Path bobAvroPath, final Path bobSchemaPath,
                                                   final String bobUidFieldName,
                                                   final int hammingThreshold,
                                                   final short C, final int L, final int K,
                                                   final int R1, final int R2, final int R3,
                                                   final String jobProfile1,final String jobProfile2,final String jobProfile3,
                                                   final int seed
                                                   ) throws Exception {
        final String blockingName = String.format("blocking.%s.%s.%s.%s.%s",
                blockingSchemeName.toLowerCase(),
                String.format("C=%d_L=%d_K=%d",C,L,K),
                aliceName,bobName,
                (new SimpleDateFormat("yyyy.MM.dd.hh.mm")).format(new Date())
        );
        final Path blockingPath = new Path(basePath,blockingName);
        hdfs.mkdirs(blockingPath);
        final Path allPairsPath = new Path(blockingPath,"all_pairs");
        final Path frequentPairsPath = new Path(blockingPath,"frequent_pairs");
        final Path bucketsPath = new Path(blockingPath,"buckets");
        final Path matchedPairsPath = new Path(blockingPath,"matched_pairs");
        final Path statsPath = new Path(blockingPath,"stats");
        final List<String> argsList = new ArrayList<String>();
        argsList.add(aliceAvroPath.toString());
        argsList.add(aliceSchemaPath.toString());
        argsList.add(aliceUidFieldName);
        argsList.add(bobAvroPath.toString());
        argsList.add(bobSchemaPath.toString());
        argsList.add(bobUidFieldName);
        switch(blockingSchemeName) {
            case "HLSH_FPS_MR_v0":
                argsList.add(allPairsPath.toString());
                argsList.add(frequentPairsPath.toString());
                break;
            case "HLSH_FPS_MR_v1":
                argsList.add(bucketsPath.toString());
                argsList.add(frequentPairsPath.toString());
                break;
            case "HLSH_FPS_MR_v2":
                argsList.add(bucketsPath.toString());
                break;
            case "HLSH_FPS_MR_v3":
                argsList.add(bucketsPath.toString());
                break;
            default:
                throw new Exception("Unsuppored blocking name scheme : " + blockingSchemeName);
        }
        argsList.add(matchedPairsPath.toString());
        argsList.add(statsPath.toString());
        argsList.add(String.valueOf(L));
        argsList.add(String.valueOf(K));
        argsList.add(String.valueOf(C));
        switch(blockingSchemeName) {
            case "HLSH_FPS_MR_v0":
                argsList.add(String.valueOf(R1));
                argsList.add(String.valueOf(R2));
                argsList.add(String.valueOf(R3));
                argsList.add(jobProfile1);
                argsList.add(jobProfile2);
                argsList.add(jobProfile3);
                break;
            case "HLSH_FPS_MR_v1":
                argsList.add(String.valueOf(R1));
                argsList.add(String.valueOf(R2));
                argsList.add(jobProfile1);
                argsList.add(jobProfile2);
                argsList.add(jobProfile3);
                break;
            case "HLSH_FPS_MR_v2":
                argsList.add(String.valueOf(R1));
                argsList.add(String.valueOf(R2));
                argsList.add(jobProfile1);
                argsList.add(jobProfile2);
                break;
            case "HLSH_FPS_MR_v3":
                argsList.add(String.valueOf(R1));
                argsList.add(jobProfile1);
                argsList.add(jobProfile2);
                break;
            default:
                throw new Exception("Unsuppored blocking name scheme : " + blockingSchemeName);
        }
        argsList.add(String.valueOf(hammingThreshold));
        argsList.add(String.valueOf(seed));
        String[] args = new String[argsList.size()];
        args = argsList.toArray(args);
        System.out.println(Arrays.toString(args));
//        switch(blockingSchemeName) {
//            case "HLSH_FPS_MR_v0":
//                hammingLshFpsBlockingV0ToolRunner.setArguments(args);
//                hammingLshFpsBlockingV0ToolRunner.call();
//                break;
//            case "HLSH_FPS_MR_v1":
//                hammingLshFpsBlockingV1ToolRunner.setArguments(args);
//                hammingLshFpsBlockingV1ToolRunner.call();
//                break;
//            case "HLSH_FPS_MR_v2":
//                hammingLshFpsBlockingV2ToolRunner.setArguments(args);
//                hammingLshFpsBlockingV2ToolRunner.call();
//                break;
//            case "HLSH_FPS_MR_v3":
//                hammingLshFpsBlockingV3ToolRunner.setArguments(args);
//                hammingLshFpsBlockingV3ToolRunner.call();
//                break;
//            default:
//                throw new Exception("Unsuppored blocking name scheme : " + blockingSchemeName);
//        }
    }
}
