package gr.upatras.ceid.pprl.service.blocking;

import gr.upatras.ceid.pprl.mapreduce.HammingLSHFPSStatistics;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.hadoop.mapreduce.ToolRunner;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

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

    public String retrieveBenchmarkStats(final String aliceName,
                                         final String bobName) throws IOException {
        try {
            final SortedSet<Path> blockingStatsPaths = new TreeSet<Path>();
            RemoteIterator<LocatedFileStatus> iterator = hdfs.listLocatedStatus(basePath);
            while (iterator.hasNext()) {
                LocatedFileStatus lfs = iterator.next();
                if (lfs.isDirectory() && lfs.getPath().getName().startsWith(
                        String.format("blocking.%s.%s", aliceName, bobName))) {
                    blockingStatsPaths.add(lfs.getPath());
                }
            }

            if (blockingStatsPaths.isEmpty())
                return String.format("No blocking benchmarks found for datasets [%s,%s]", aliceName, bobName);


            StringBuilder sb = new StringBuilder();
            final String header = "C,L,K,version,time_1,time_2,time_3,hbw_1,hbw_2,hbw_3,mf_1,mf_2,mf_3,tpc,fpc,mpc";
            sb.append("--Source paths found-----------------\n");
            int i = 1;
            for (Path path : blockingStatsPaths) {
                sb.append(i).append(". ").append(path.toUri()).append("\n");
                i++;
            }
            sb.append("--Stats (csv)------------------------\n");
            sb.append(header).append("\n");
            System.out.println(sb.toString());

            for (Path path : blockingStatsPaths) {
                StringBuilder prefixBuilder = new StringBuilder();
                final String pathName = path.getName();
                final String[] pathNameParts = pathName.split(".");
                assert pathNameParts[4].split("_").length == 6;
                prefixBuilder
                        .append(pathNameParts[4].split("_")[1]).append(",")
                        .append(pathNameParts[4].split("_")[3]).append(",")
                        .append(pathNameParts[4].split("_")[5]).append(",");
                assert pathNameParts[3].split("_").length == 4;
                prefixBuilder
                        .append(pathNameParts[3].split("_")[3]).append(",");
                final Path statsPath = new Path(path, "stats");
                if (!hdfs.exists(statsPath) || !hdfs.isFile(statsPath)) continue;
                final String benchmarkString = HammingLSHFPSStatistics.loadAsBenchmarkAsStringCSV(hdfs, statsPath);
                sb.append(prefixBuilder.toString()).append(benchmarkString).append("\n");
            }
            return sb.toString();
        } catch(IOException e) {
            LOG.error(e.getMessage(),e);
            throw e;
        }
    }

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
                aliceName,bobName,
                blockingSchemeName.toLowerCase(),
                String.format("C_%d_L_%d_K_%d",C,L,K),
                (new SimpleDateFormat("yyyy.MM.dd.hh.mm")).format(new Date())
        );
        try {
            final Path blockingPath = new Path(basePath, blockingName);
            hdfs.mkdirs(blockingPath);
            final Path allPairsPath = new Path(blockingPath, "all_pairs");
            final Path frequentPairsPath = new Path(blockingPath, "frequent_pairs");
            final Path bucketsPath = new Path(blockingPath, "buckets");
            final Path matchedPairsPath = new Path(blockingPath, "matched_pairs");
            final Path statsPath = new Path(blockingPath, "stats");
            final List<String> argsList = new ArrayList<String>();
            argsList.add(aliceAvroPath.toString());
            argsList.add(aliceSchemaPath.toString());
            argsList.add(aliceUidFieldName);
            argsList.add(bobAvroPath.toString());
            argsList.add(bobSchemaPath.toString());
            argsList.add(bobUidFieldName);
            switch (blockingSchemeName) {
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
            switch (blockingSchemeName) {
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
            switch (blockingSchemeName) {
                case "HLSH_FPS_MR_v0":
                    hammingLshFpsBlockingV0ToolRunner.setArguments(args);
                    hammingLshFpsBlockingV0ToolRunner.call();
                    break;
                case "HLSH_FPS_MR_v1":
                    hammingLshFpsBlockingV1ToolRunner.setArguments(args);
                    hammingLshFpsBlockingV1ToolRunner.call();
                    break;
                case "HLSH_FPS_MR_v2":
                    hammingLshFpsBlockingV2ToolRunner.setArguments(args);
                    hammingLshFpsBlockingV2ToolRunner.call();
                    break;
                case "HLSH_FPS_MR_v3":
                    hammingLshFpsBlockingV3ToolRunner.setArguments(args);
                    hammingLshFpsBlockingV3ToolRunner.call();
                    break;
                default:
                    throw new Exception("Unsuppored blocking name scheme : " + blockingSchemeName);
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(),e);
            throw e;
        }
    }
}
