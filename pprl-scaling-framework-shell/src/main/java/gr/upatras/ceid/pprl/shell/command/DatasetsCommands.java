package gr.upatras.ceid.pprl.shell.command;

import gr.upatras.ceid.pprl.datasets.DatasetStatistics;
import gr.upatras.ceid.pprl.datasets.DatasetsUtil;
import gr.upatras.ceid.pprl.datasets.service.DatasetsService;
import gr.upatras.ceid.pprl.datasets.service.LocalDatasetsService;
import gr.upatras.ceid.pprl.matching.ExpectationMaximization;
import gr.upatras.ceid.pprl.matching.SimilarityMatrix;
import gr.upatras.ceid.pprl.matching.service.LocalMatchingService;
import gr.upatras.ceid.pprl.matching.service.MatchingService;
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

@Component
public class DatasetsCommands implements CommandMarker {

    private static final Logger LOG = LoggerFactory.getLogger(DatasetsCommands.class);

    @Autowired(required = false)
    @Qualifier("datasetsService")
    private DatasetsService ds;

    @Autowired(required = false)
    @Qualifier("localDatasetsService")
    private LocalDatasetsService lds;

    @Autowired(required = false)
    @Qualifier("matchingService")
    private MatchingService ms;

    @Autowired(required = false)
    @Qualifier("localMatchingService")
    private LocalMatchingService lms;

    @CliAvailabilityIndicator(value = {"local_data_sample","local_data_describe","local_data_add_ulid"})
    public boolean availability0() {
        return lds != null;
    }
    @CliAvailabilityIndicator(value = {"local_data_stats"})
    public boolean availability1() { return lds != null && lms != null;}
    @CliAvailabilityIndicator(value = {"local_data_update","data_download"})
    public boolean availability2() { return ds != null;}


    // TODO commands
    //      -LOCAL-
    //      Local data sort by field
    //      -HDFS-
    //      Download data
    //      Import dblp
    //      Sample
    //      Describe
    //      Add Ulid
    //      Sort by field

    @CliCommand(value = "local_data_sample", help = "View a sample of local data.")
    public String command0(
            @CliOption(key = {"avro"}, mandatory = true, help = "Local data avro files (comma separated) or including directory.")
            final String avroStr,
            @CliOption(key = {"schema"}, mandatory = true, help = "Local schema avro file.")
            final String schemaStr,
            @CliOption(key = {"size"}, mandatory = false, help = "(Optional) Sample size. Default is 10.")
            final String sizeStr,
            @CliOption(key = {"name"}, mandatory = false, help = "(Optional) Name to save sample to local filesystem. If not provided no sample will be saved.")
            final String nameStr
    ) {
        try{
            final Path schemaPath = CommandUtil.retrievePath(schemaStr);
            final Path[] avroPaths = CommandUtil.retrievePaths(avroStr);
            final int size = CommandUtil.retrieveInt(sizeStr, 10);
            if (size < 1) throw new IllegalArgumentException("Sample size must be greater than zero.");
            final String name = CommandUtil.retrieveString(nameStr, null);
            final boolean save = !(name == null);
            if(save && !CommandUtil.isValidName(name)) throw new IllegalArgumentException("name is not valid");

            LOG.info("Sampling from local data :");
            LOG.info("\tSelected data path(s): {}", Arrays.toString(avroPaths));
            LOG.info("\tSelected schema path : {}", schemaPath);
            LOG.info("\tSample size : {} ",size);

            final Schema schema = lds.loadSchema(schemaPath);
            final GenericRecord[] sample = lds.sample(avroPaths, schemaPath, size);
            if(save) {
                final Path savePath = lds.saveRecords(name, sample, schema);
                LOG.info("\t Saved to path : {}",savePath);
            }
            LOG.info("\n");

            LOG.info(CommandUtil.prettyRecords(sample, schema));
            return "DONE";
        } catch (Exception e) {
            return "Error. " + e.getClass().getSimpleName() + " : " + e.getMessage();
        }
    }

    @CliCommand(value = "local_data_describe", help = "View a schema desription of local data.")
    public String command1(
            @CliOption(key = {"schema"}, mandatory = true, help = "Local schema avro file.")
            final String schemaStr) {
        try {
            final Path schemaPath = CommandUtil.retrievePath(schemaStr);

            LOG.info("Describing local data :");
            LOG.info("\tSelected schema file : {}", schemaPath);
            LOG.info("\n");

            final Schema schema = lds.loadSchema(schemaPath);

            LOG.info(CommandUtil.prettySchemaDescription(schema));
            return "DONE";
        } catch (Exception e) {
            return "Error. " + e.getClass().getSimpleName() + " : " + e.getMessage();
        }
    }


    @CliCommand(value = "local_data_stats", help = "Calculate usefull field statistics from local data.")
    public String command2(
            @CliOption(key = {"avro"}, mandatory = true, help = "Local data avro files (comma separated) or including directory.")
            final String avroStr,
            @CliOption(key = {"schema"}, mandatory = true, help = "Local schema avro file.")
            final String schemaStr,
            @CliOption(key = {"fields"}, mandatory = true, help = "Filter fields to collect statistics from.")
            final String fieldsStr,
            @CliOption(key = {"name"}, mandatory = false, help = "(Optional) Name to save statistics report to local filesystem as properties file. If not provided no report will be saved.")
            final String nameStr,
            @CliOption(key = {"m"}, mandatory = false, help = "(Optional) Initial m values . 0.9 for all fields is default.")
            final String mStr,
            @CliOption(key = {"u"}, mandatory = false, help = "(Optional) Initial u values . 0.01 for all fields is default.")
            final String uStr,
            @CliOption(key = {"p"}, mandatory = false, help = "(Optional) Initial p value . 0.1 is default.")
            final String pStr
    ) {
        try {
            final Path schemaPath = CommandUtil.retrievePath(schemaStr);
            final Path[] avroPaths = CommandUtil.retrievePaths(avroStr);
            String[] fields = CommandUtil.retrieveFields(fieldsStr);
            final String name = CommandUtil.retrieveString(nameStr, null);
            final boolean save = name != null;
            if(save && !CommandUtil.isValidName(name)) throw new IllegalArgumentException("name is not valid");
            final double[] m0 = CommandUtil.retrieveProbabilities(mStr, fields.length, 0.9);
            final double[] u0 = CommandUtil.retrieveProbabilities(uStr, fields.length, 0.001);
            final double p0 = CommandUtil.retrieveProbability(pStr, 0.1);

            LOG.info("Calculating statistics on local data:");
            LOG.info("\tSelected data files : {}", Arrays.toString(avroPaths));
            LOG.info("\tSelected schema file : {}", schemaPath);
            LOG.info("\tSelected fields : {}", Arrays.toString(fields));


            final Schema schema = lds.loadSchema(schemaPath);
            if(fields.length == 0 )
                fields = DatasetsUtil.fieldNames(schema);
            else if(!Arrays.asList(DatasetsUtil.fieldNames(schema)).containsAll(Arrays.asList(fields)))
                throw new IllegalArgumentException(String.format("fields %s not found in schema",
                        Arrays.toString(fields)));

            final GenericRecord[] records = lds.loadRecords(avroPaths, schemaPath);
            final DatasetStatistics statistics = new DatasetStatistics();

            statistics.setRecordCount(records.length);
            statistics.setFieldNames(fields);
            DatasetStatistics.calculateQgramStatistics(records, schema, statistics, fields);

            final SimilarityMatrix matrix = lms.createMatrix(records,fields);
            final ExpectationMaximization estimator = lms.newEMInstance(fields,m0,u0,p0);
            estimator.runAlgorithm(matrix);

            statistics.setEmPairs(estimator.getPairCount());
            statistics.setEmAlgorithmIterations(estimator.getIteration());
            statistics.setP(estimator.getP());
            DatasetStatistics.calculateStatsUsingEstimates(
                    statistics,fields,
                    estimator.getM(),estimator.getU());

            if(save) {
                final Path statsPath = lds.saveStats(name, statistics);
                LOG.info("\tStatistics path : {}",statsPath);
            }
            LOG.info("\n");

            LOG.info(CommandUtil.prettyStats(statistics));

            return "DONE";
        } catch (Exception e) {
            return "Error. " + e.getClass().getSimpleName() + " : " + e.getMessage();
        }
    }

    @CliCommand(value = "local_data_upload", help = "Upload local data to the PPRL-site hdfs cluster.")
    public String command3(
            @CliOption(key = {"avro"}, mandatory = true, help = "Local data avro files (comma separated) or including directory.")
            final String avroStr,
            @CliOption(key = {"schema"}, mandatory = true, help = "Local schema avro file.")
            final String schemaStr,
            @CliOption(key = {"name"}, mandatory = true, help = "Name of the dataset. Will be stored at hdfs://users/${USER.HOME}/${name}")
            final String name
    ) {
        try{
            final Path schemaPath = CommandUtil.retrievePath(schemaStr);
            final Path[] avroPaths = CommandUtil.retrievePaths(avroStr);
            LOG.info("Uploading local data to hdfs :");
            LOG.info("\tSelected data files : {}", Arrays.toString(avroPaths));
            LOG.info("\tSelected schema file : {}", schemaPath);
            LOG.info("\tName : {}",name);
            final Path uploadedPath = ds.uploadFiles(avroPaths,schemaPath,name);
            LOG.info("\tDestination path : {}",uploadedPath);
            return "DONE";
        } catch (Exception e) {
            return "Error. " + e.getClass().getSimpleName() + " : " + e.getMessage();
        }
    }

    @CliCommand(value = "local_data_add_ulid", help = "Add a Unique Long Identifier as a field to existing avro data.")
    public String command4(
            @CliOption(key = {"avro"}, mandatory = true, help = "Local data avro files (comma separated) or including directory.")
            final String avroStr,
            @CliOption(key = {"schema"}, mandatory = true, help = "Local schema avro file.")
            final String schemaStr,
            @CliOption(key = {"name"}, mandatory = true, help = "(Optional) Name to save the update records.")
            final String name,
            @CliOption(key = {"field"}, mandatory = false, help = "(Optional) Name of the ULID field. Default is \"ulid\"")
            final String fieldStr
    ) {
        try {
            final Path schemaPath = CommandUtil.retrievePath(schemaStr);
            final Path[] avroPaths = CommandUtil.retrievePaths(avroStr);
            final String fieldName = CommandUtil.retrieveString(fieldStr,"ulid");
            if(!CommandUtil.isValidFieldName(fieldName)) throw new IllegalArgumentException("Invalid field name.");
            LOG.info("Add a ULID field to local data:");
            LOG.info("\tSelected data files : {}", Arrays.toString(avroPaths));
            LOG.info("\tSelected schema file : {}", schemaPath);
            LOG.info("\tName : {}",name);
            LOG.info("\tField Name : {}",fieldName);

            final Schema updatedSchema = DatasetsUtil.updateSchemaWithULID(lds.loadSchema(schemaPath),
                    fieldName);
            final GenericRecord[] updatedRecords = DatasetsUtil.updateRecordsWithULID(lds.loadRecords(avroPaths, schemaPath),
                    updatedSchema, fieldName);
            LOG.info(CommandUtil.prettyRecords(updatedRecords, updatedSchema));
            lds.saveRecords(name,updatedRecords,updatedSchema);
            return "DONE";
        } catch (Exception e) {
            return "Error. " + e.getClass().getSimpleName() + " : " + e.getMessage();
        }
    }

    @CliCommand(value = "data_download", help = "Download remote dataset to local machine.")
    public String command5(
    ) {
        try {

            return "DONE";
        } catch (Exception e) {
            return "Error. " + e.getClass().getSimpleName() + " : " + e.getMessage();
        }
    }


}
