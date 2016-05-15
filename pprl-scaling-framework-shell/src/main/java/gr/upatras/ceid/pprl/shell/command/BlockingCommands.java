package gr.upatras.ceid.pprl.shell.command;

import gr.upatras.ceid.pprl.blocking.BlockingUtil;
import gr.upatras.ceid.pprl.blocking.HammingLSHBlocking;
import gr.upatras.ceid.pprl.matching.PrivateSimilarityUtil;
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

import java.util.Arrays;
import java.util.List;

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

    private List<String> ENCODING_SCHEMES = BlockingUtil.SCHEME_NAMES;


    @CliAvailabilityIndicator(value = {"list_supported_blocking_schemes"})
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
        for(String methodName: ENCODING_SCHEMES) {
            LOG.info("\t{}. {}",i,methodName);
            i++;
        }
        return "DONE";
    }

    /**
     *  LOCAL BLOCKING COMMANDS
     */

    @CliCommand(value = "block_encoded_local_data", help = "Privately block records of local datasets.")
    public String command1(
            @CliOption(key = {"alice_avro"}, mandatory = true, help = "Local encoded data avro files of Alice (comma separated) or including directory.")
            final String aliceAvroStr,
            @CliOption(key = {"alice_schema"}, mandatory = true, help = "Local encoded schema schema file of Alice.")
            final String aliceSchemaStr,
            @CliOption(key = {"alice_uid"}, mandatory = true, help = "UID fieldname of the Alice encoded dataset.")
            final String aliceUidFieldName,
            @CliOption(key = {"bob_avro"}, mandatory = true, help = "Local encoded data avro files of Bob (comma separated) or including directory.")
            final String bobAvroStr,
            @CliOption(key = {"bob_schema"}, mandatory = true, help = "Local encoded schema schema file of Bob.")
            final String bobSchemaStr,
            @CliOption(key = {"bob_uid"}, mandatory = true, help = "UID fieldname of the Bob encoded dataset.")
            final String bobUidFieldName,
            @CliOption(key = {"blocking_scheme"}, mandatory = true, help = "Blocking scheme name.")
            final String blockingSchemeName,
            @CliOption(key = {"blocking_output"}, mandatory = true, help = "Blocking output file (local file)")
            final String blockingOutput,
            @CliOption(key = {"hlsh_L"}, mandatory = false, help = "(Optional) Number of blocking groups for HLSH blocking. Defaults to 32.")
            final String hlshLStr,
            @CliOption(key = {"hlsh_K"}, mandatory = false, help = "(Optional) Number of hash values for HLSH blocking. Defaults to 5.")
            final String hlshKStr,
            @CliOption(key = {"hlsh_C"}, mandatory = false, help = "(Optional) Number of collisions in HLSH blocking groups required to be considered frequent. Defaults to 2.")
            final String hlshCStr,
            @CliOption(key = {"similarity_name"}, mandatory = false, help = "(Optional) Similarity method name. Defaults to \"hamming\".")
            final String similarityMethodNameStr,
            @CliOption(key = {"similarity_threshold"}, mandatory = false, help = "(Optional) Similarity threshold. Defaults to 100 for the hamming method, 0.7 for jaccard and  dice.")
            final String similarityThresholdStr
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

            BlockingUtil.schemeNameSupported(blockingSchemeName);
            final Path blockingOutputPath = CommandUtil.retrievePath(blockingOutput);

            final String similarityMethodName = CommandUtil.retrieveString(similarityMethodNameStr,"hamming");
            PrivateSimilarityUtil.methodNameSupported(similarityMethodName);
            final double similarityThreshold = CommandUtil.retrieveDouble(similarityThresholdStr,
                    similarityMethodName.equals("hamming") ? 100.0 : 0.7 );

            LOG.info("Blocking local datasets : ");
            LOG.info("\tAlice avro data path(s): {}", Arrays.toString(aliceAvroPaths));
            LOG.info("\tAlice schema path : {}", aliceSchemaPath);
            LOG.info("\tAlice UID field name : {} ",aliceUidFieldName);
            LOG.info("\tBob avro data path(s): {}", Arrays.toString(bobAvroPaths));
            LOG.info("\tBob schema path : {}", bobSchemaPath);
            LOG.info("\tBob UID field name : {} ",bobUidFieldName);
            LOG.info("\tBlocking Scheme name : {}",blockingSchemeName);

            LOG.info("\tSimilarity method name : {}",similarityMethodName);
            LOG.info("\tSimilarity threshold : {}",similarityThreshold);

            if(blockingSchemeName.equals("HLSH_FPS")) {
                final int L = CommandUtil.retrieveInt(hlshLStr,32);
                final int K = CommandUtil.retrieveInt(hlshKStr,5);
                final short C = CommandUtil.retrieveShort(hlshCStr, (short) 2);
                LOG.info("\tHLSH Blocking Groups (L) : {}",L);
                LOG.info("\tHLSH Blocking Hash Values (K) : {}",K);
                LOG.info("\tFPS Collision Limit (C) : {}",C);
                LOG.info("\n");

                final Schema aliceSchema = lds.loadSchema(aliceSchemaPath);
                final GenericRecord[] aliceRecords = lds.loadDatasetRecords(aliceAvroPaths,aliceSchema);

                final Schema bobSchema = ds.loadSchema(bobSchemaPath);
                final GenericRecord[] bobRecords = lds.loadDatasetRecords(bobAvroPaths,bobSchema);

                final HammingLSHBlocking blocking = lbs.newHammingLSHBlockingInstance(L,K,aliceSchema,bobSchema);

                final HammingLSHBlocking.HammingLSHBlockingResult result = lbs.runFPSonHammingBlocking(
                        blocking,
                        aliceRecords, aliceUidFieldName,
                        bobRecords, bobUidFieldName,
                        C,
                        similarityMethodName, similarityThreshold
                );
                LOG.info("\tFrequent Pairs found : {}", result.getFrequentPairsCount());
                LOG.info("\tMatched Pairs found : {}", result.getMatchedPairsCount());
                LOG.info("\tBlocking output path : {}",blockingOutputPath);
                lbs.saveResult(result,blockingOutputPath);


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
    public String command2(
    ) {
        try {
            return "DONE";
        } catch (Exception e) {
            return "Error. " + e.getClass().getSimpleName() + " : " + e.getMessage();
        }
    }
}
