package gr.upatras.ceid.pprl.shell.command;

import gr.upatras.ceid.pprl.blocking.HammingLSHBlockingUtil;
import gr.upatras.ceid.pprl.blocking.HammingLSHBlocking;
import gr.upatras.ceid.pprl.service.blocking.BlockingService;
import gr.upatras.ceid.pprl.service.blocking.LocalBlockingService;
import gr.upatras.ceid.pprl.service.datasets.DatasetsService;
import gr.upatras.ceid.pprl.service.datasets.LocalDatasetsService;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliAvailabilityIndicator;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

@Component
public class BlockingCommands implements CommandMarker {

    private static Logger LOG = LoggerFactory.getLogger(BlockingCommands.class);

    @Autowired(required = false)
    @Qualifier("datasetsService")
    private DatasetsService ds;

    @Autowired(required = false)
    @Qualifier("localDatasetsService")
    private LocalDatasetsService lds;

    @Autowired(required = false)
    @Qualifier("blockingService")
    private BlockingService bs;

    @Autowired(required = false)
    @Qualifier("localBlockingService")
    private LocalBlockingService lbs;


    @CliAvailabilityIndicator(value = {"list_supported_blocking_schemes","get_optimal_hlsh_fps_params"})
    public boolean availability0() { return true; }
    @CliAvailabilityIndicator(value = {"block_encoded_local_data"})
    public boolean availability1() { return lbs != null && lds != null; }
    @CliAvailabilityIndicator(value = {"block_encoded_data"})
    public boolean availability3() { return ds != null && bs != null; }

    /**
     *  COMMON BLOCKING COMMANDS
     */

    @CliCommand(value = "list_supported_blocking_schemes", help = "List system's supported blocking schemes.")
    public String command0() {
        LOG.info("Supported blocking schemes : ");
        int i = 1;
        for(String methodName: HammingLSHBlockingUtil.SCHEME_NAMES) {
            LOG.info("\t{}. {}",i,methodName);
            i++;
        }
        return "DONE";
    }

    @CliCommand(value = "get_optimal_hlsh_fps_params", help = "Return optimal HLSH/FPS Parameters for future blocking.")
    public String command1(
        @CliOption(key = {"theta"}, mandatory = true, help = "Hamming distance threshold.")
        final String thetaStr,
        @CliOption(key = {"delta"}, mandatory = true, help = "Hamming distance threshold.")
        final String deltaStr,
        @CliOption(key = {"size"}, mandatory = true, help = "Hamming distance threshold.")
        final String sizeStr,
        @CliOption(key = {"K"}, mandatory = true, help = "Hamming distance threshold.")
        final String Kstr
    ) {
        final int theta = CommandUtil.retrieveInt(thetaStr,0);
        final double delta = CommandUtil.retrieveDouble(deltaStr, 0);
        final int S = CommandUtil.retrieveInt(sizeStr, 0);
        final int K = CommandUtil.retrieveInt(Kstr, 0);

        LOG.info("Hamming threshold : {}",theta);
        LOG.info("Confidence factor : {}",delta);
        LOG.info("Bloom-Filter size : {}",S);
        LOG.info("HLSH Key size : {}",K);
        LOG.info("\n");

        final int[] params = HammingLSHBlockingUtil.optimalParameters(theta,S,delta,K);
        LOG.info("Suggested Colission limit  : {}", params[0]);
        LOG.info("Suggested Number of blocking tables : {}",params[1]);
        LOG.info("Suggested Limits on number of blocking tables : {}",
                Arrays.toString(new int[]{params[2],params[3]})
        );
        return "DONE";
    }
    /**
     *  LOCAL BLOCKING COMMANDS
     */

    @CliCommand(value = "block_encoded_local_data", help = "Privately block records of local datasets.")
    public String command2(
            @CliOption(key = {"alice_avro"}, mandatory = true, help = "Local encoded data avro files of Alice (comma separated) or including directory.")
            final String aliceAvroStr,
            @CliOption(key = {"alice_schema"}, mandatory = true, help = "Local encoded schema schema file of Alice.")
            final String aliceSchemaStr,
            @CliOption(key = {"alice_uid"}, mandatory = true, help = "UID field name of the Alice encoded dataset.")
            final String aliceUidFieldName,
            @CliOption(key = {"bob_avro"}, mandatory = true, help = "Local encoded data avro files of Bob (comma separated) or including directory.")
            final String bobAvroStr,
            @CliOption(key = {"bob_schema"}, mandatory = true, help = "Local encoded schema schema file of Bob.")
            final String bobSchemaStr,
            @CliOption(key = {"bob_uid"}, mandatory = true, help = "UID field name of the Bob encoded dataset.")
            final String bobUidFieldName,
            @CliOption(key = {"blocking_scheme"}, mandatory = true, help = "Blocking scheme name.")
            final String blockingSchemeName,
            @CliOption(key = {"blocking_output"}, mandatory = true, help = "Blocking output file (local file).")
            final String blockingOutput,
            @CliOption(key = {"hf_L"}, mandatory = true, help = "Number of blocking groups for HLSH blocking.")
            final String hlshLStr,
            @CliOption(key = {"hf_K"}, mandatory = true, help = "Number of hash values for HLSH blocking.")
            final String hlshKStr,
            @CliOption(key = {"hf_C"}, mandatory = true, help = "Number of collisions in HLSH blocking groups required to be considered frequent.")
            final String hlshCStr,
            @CliOption(key = {"hf_theta"}, mandatory = true, help = "Hamming similarity threshold.")
            final String hammingThreholdStr,
            @CliOption(key = {"hf_seed"}, mandatory = false, help = "(Optional). A seed for reproducing HLSH hashing.")
            final String seedStr

    ) {
        try {

            final Path[] aliceAvroPaths = CommandUtil.retrievePaths(aliceAvroStr);
            final Path aliceSchemaPath = CommandUtil.retrievePath(aliceSchemaStr);
            if (!CommandUtil.isValidFieldName(aliceUidFieldName))
                throw new IllegalArgumentException("Not a valid field name \"" + aliceUidFieldName  + "\"");
            final Path[] bobAvroPaths = CommandUtil.retrievePaths(bobAvroStr);
            final Path bobSchemaPath = CommandUtil.retrievePath(bobSchemaStr);
            if (!CommandUtil.isValidFieldName(bobUidFieldName))
                throw new IllegalArgumentException("Not a valid field name \"" + bobUidFieldName  + "\"");

            HammingLSHBlockingUtil.schemeNameSupported(blockingSchemeName);
            final Path blockingOutputPath = CommandUtil.retrievePath(blockingOutput);

            LOG.info("Blocking local datasets : ");
            LOG.info("\tAlice avro data path(s): {}", Arrays.toString(aliceAvroPaths));
            LOG.info("\tAlice schema path : {}", aliceSchemaPath);
            LOG.info("\tAlice UID field name : {} ",aliceUidFieldName);
            LOG.info("\tBob avro data path(s): {}", Arrays.toString(bobAvroPaths));
            LOG.info("\tBob schema path : {}", bobSchemaPath);
            LOG.info("\tBob UID field name : {} ",bobUidFieldName);
            LOG.info("\tBlocking Scheme name : {}",blockingSchemeName);

            if(blockingSchemeName.equals("HLSH_FPS")) {
                final int L = CommandUtil.retrieveInt(hlshLStr,-1);
                final int K = CommandUtil.retrieveInt(hlshKStr,-1);
                final short C = CommandUtil.retrieveShort(hlshCStr, (short) -1);
                final int hammingThreshold = CommandUtil.retrieveInt(hammingThreholdStr,-1);
                final int seed = CommandUtil.retrieveInt(seedStr,-1);
                if(seed >= 0) LOG.info("\tHLSH seed for random keys : {}",seed);
                LOG.info("\tHLSH Hamming threshold (theta) : {}",hammingThreshold);
                LOG.info("\tHLSH Blocking Groups (L) : {}",L);
                LOG.info("\tHLSH Blocking Hash Values (K) : {}",K);
                LOG.info("\tFPS Collision Limit (C) : {}",C);
                LOG.info("\n");

                final Schema aliceSchema = lds.loadSchema(aliceSchemaPath);
                final GenericRecord[] aliceRecords = lds.loadDatasetRecords(aliceAvroPaths,aliceSchema);

                final Schema bobSchema = lds.loadSchema(bobSchemaPath);
                final GenericRecord[] bobRecords = lds.loadDatasetRecords(bobAvroPaths,bobSchema);

                final HammingLSHBlocking blocking = lbs.newHammingLSHBlockingInstance(L,K,aliceSchema,bobSchema,seed);

                lbs.runFPSonHammingBlocking(
                        blocking,
                        aliceRecords, aliceUidFieldName,
                        bobRecords, bobUidFieldName,
                        C, hammingThreshold
                );

                LOG.info("\tFrequent Pairs found : {}", blocking.getResult().getFrequentPairsCount());
                LOG.info("\tMatched Pairs found : {}", blocking.getResult().getMatchedPairsCount());
                LOG.info("\tBlocking output path : {}",blockingOutputPath);
                lbs.saveResult(blocking.getResult(),blockingOutputPath);
            } else throw new UnsupportedOperationException("\"" + blockingSchemeName + "\" is not implemented yet.");

            return "DONE";
        } catch (Exception e) {
            return "Error. " + e.getClass().getSimpleName() + " : " + e.getMessage();
        }

    }

    /**
     *  HDFS BLOCKING COMMANDS
     */
    @CliCommand(value = "block_encoded_data", help = "Privately block records of HDFS datasets.")
    public String command3(
            @CliOption(key = {"alice_name"}, mandatory = true, help = "Name of Alice encoded dataset on the HDFS site.")
            final String aliceName,
            @CliOption(key = {"alice_uid"}, mandatory = true, help = "UID field-name of the Alice encoded dataset.")
            final String aliceUidFieldName,
            @CliOption(key = {"bob_name"}, mandatory = true, help = "Name of Bob encoded dataset on the HDFS site.")
            final String bobName,
            @CliOption(key = {"bob_uid"}, mandatory = true, help = "UID field-name of the Bob encoded dataset.")
            final String bobUidFieldName,
            @CliOption(key = {"blocking_scheme"}, mandatory = true, help = "Blocking scheme name.")
            final String blockingSchemeName,
            @CliOption(key = {"reducers"}, mandatory = true, help = "Number of reduces per sub-task (comma-seperated).")
            final String reducersString,
            @CliOption(key = {"hf_L"}, mandatory = true, help = "Number of blocking groups for HLSH blocking. Defaults to 32.")
            final String hlshLStr,
            @CliOption(key = {"hf_K"}, mandatory = true, help = "Number of hash values for HLSH blocking. Defaults to 5.")
            final String hlshKStr,
            @CliOption(key = {"hf_C"}, mandatory = true, help = "Number of collisions in HLSH blocking groups required to be considered frequent. Defaults to 2.")
            final String hlshCStr,
            @CliOption(key = {"hf_theta"}, mandatory = true, help = "Hamming Similarity threshold.")
            final String hammingThreholdStr,
            @CliOption(key = {"hf_seed"}, mandatory = false, help = "(Optional). A seed for reproducing HLSH hashing.")
            final String seedStr
    ) {
        try {

            final Path[] alicePaths = ds.retrieveDirectories(aliceName);
            final Path aliceAvroPath = alicePaths[1];
            final Path aliceSchemaPath = ds.retrieveSchemaPath(alicePaths[2]);
            if (!CommandUtil.isValidFieldName(aliceUidFieldName))
                throw new IllegalArgumentException("Not a valid field name \"" + aliceUidFieldName  + "\"");

            final Path[] bobPaths = ds.retrieveDirectories(bobName);
            final Path bobAvroPAth = bobPaths[1];
            final Path bobSchemaPath = ds.retrieveSchemaPath(bobPaths[2]);
            if (!CommandUtil.isValidFieldName(bobUidFieldName))
                throw new IllegalArgumentException("Not a valid field name \"" + bobUidFieldName  + "\"");

            HammingLSHBlockingUtil.schemeNameSupported(blockingSchemeName);
            final String blockingName = String.format("blocking.%s.%s.%s.%s",
					blockingSchemeName.toLowerCase(),
					aliceName,bobName,
                    (new SimpleDateFormat("yyyy.MM.dd.hh.mm")).format(new Date())
            );

            LOG.info("Blocking HDFS datasets : ");
            LOG.info("\tAlice dataset name : {}", aliceName);
            LOG.info("\tAlice avro path : {}", aliceAvroPath);
            LOG.info("\tAlice schema path : {}", aliceSchemaPath);
            LOG.info("\tAlice UID field name : {} ",aliceUidFieldName);
            LOG.info("\tBob dataset name : {}", bobName);
            LOG.info("\tBob avro path : {}", bobAvroPAth);
            LOG.info("\tBob schema path : {}", bobSchemaPath);
            LOG.info("\tBob UID field name : {} ",bobUidFieldName);
            LOG.info("\tBlocking Scheme name : {}",blockingSchemeName);
            LOG.info("\tBlocking name : {}",blockingName);

            if(blockingSchemeName.startsWith("HLSH_FPS_MR")) {
                final int L = CommandUtil.retrieveInt(hlshLStr,-1);
                final int K = CommandUtil.retrieveInt(hlshKStr,-1);
                final short C = CommandUtil.retrieveShort(hlshCStr, (short) -1);
                final int hammingThreshold = CommandUtil.retrieveInt(hammingThreholdStr,-1);
                LOG.info("\tHLSH Hamming threshold (theta) : {}",hammingThreshold);
                LOG.info("\tHLSH Blocking Groups (L) : {}",L);
                LOG.info("\tHLSH Blocking Hash Values (K) : {}",K);
                LOG.info("\tFPS Collision Limit (C) : {}",C);
                final int seed = CommandUtil.retrieveInt(seedStr,-1);
                if(seed >= 0) LOG.info("\tHLSH seed for random keys : {}",seed);
                switch (blockingSchemeName) {
                    case "HLSH_FPS_MR_v0": {
                        final int[] R = CommandUtil.retrieveInts(reducersString);
                        if (R.length != 3)
                            throw new IllegalArgumentException("This job consists of 3 sub-jobs and requires" +
                                    " to define exactly 3 reduer numbers.");
                        LOG.info("\tJob Reducers : {}", Arrays.toString(R));
                        LOG.info("\n");
                        bs.runHammingLSHFPSBlockingV0ToolRuner(
                                aliceAvroPath, aliceSchemaPath, aliceUidFieldName,
                                bobAvroPAth, bobSchemaPath, bobUidFieldName,
                                blockingName,
                                L, K, C,
                                hammingThreshold,
                                R[0], R[1], R[2],seed);

                        break;
                    }
                    case "HLSH_FPS_MR_v1": {
                        final int[] R = CommandUtil.retrieveInts(reducersString);
                        if (R.length != 3)
                            throw new IllegalArgumentException("This job consists of 3 sub-jobs and requires" +
                                    " to define exactly 3 reducer numbers.");
                        LOG.info("\tJob Reducers : {}", Arrays.toString(R));
                        LOG.info("\n");
                        bs.runHammingLSHFPSBlockingV1ToolRuner(
                                aliceAvroPath, aliceSchemaPath, aliceUidFieldName,
                                bobAvroPAth, bobSchemaPath, bobUidFieldName,
                                blockingName,
                                L, K, C,
                                hammingThreshold,
                                R[0], R[1], R[2],seed);
                        break;
                    }
                    case "HLSH_FPS_MR_v2": {
                        final int[] R = CommandUtil.retrieveInts(reducersString);
                        if (R.length != 2)
                            throw new IllegalArgumentException("This job consists of 2 sub-jobs and requires" +
                                    " to define exactly 2 reducer numbers.");
                        LOG.info("\tJob Reducers : {}", Arrays.toString(R));
                        LOG.info("\n");
                        bs.runHammingLSHFPSBlockingV2ToolRuner(
                                aliceAvroPath, aliceSchemaPath, aliceUidFieldName,
                                bobAvroPAth, bobSchemaPath, bobUidFieldName,
                                blockingName,
                                L, K, C,
                                hammingThreshold,
                                R[0], R[1],seed);
                        break;
                    }
                    case "HLSH_FPS_MR_v3": {
                        final int[] R = CommandUtil.retrieveInts(reducersString);
                        if (R.length != 2)
                            throw new IllegalArgumentException("This job consists of 2 sub-jobs and requires" +
                                    " to define exactly 2 reducer numbers.");
                        LOG.info("\tJob Reducers : {}", Arrays.toString(R));
                        LOG.info("\n");
                        bs.runHammingLSHFPSBlockingV3ToolRuner(
                                aliceAvroPath, aliceSchemaPath, aliceUidFieldName,
                                bobAvroPAth, bobSchemaPath, bobUidFieldName,
                                blockingName,
                                L, K, C,
                                hammingThreshold,
                                R[0], R[1],seed);
                        break;
                    }
                    default:
                        throw new UnsupportedOperationException("\"" + blockingSchemeName + "\" is not implemented yet.");
                }
            }

            return "DONE";
        } catch (Exception e) {
            return "Error. " + e.getClass().getSimpleName() + " : " + e.getMessage();
        }
    }
}
