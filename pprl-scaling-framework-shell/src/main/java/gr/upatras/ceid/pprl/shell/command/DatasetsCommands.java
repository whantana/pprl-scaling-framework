package gr.upatras.ceid.pprl.shell.command;

import gr.upatras.ceid.pprl.datasets.DatasetStatistics;
import gr.upatras.ceid.pprl.datasets.DatasetsUtil;
import gr.upatras.ceid.pprl.service.datasets.DatasetsService;
import gr.upatras.ceid.pprl.service.datasets.LocalDatasetsService;
import gr.upatras.ceid.pprl.matching.ExpectationMaximization;
import gr.upatras.ceid.pprl.matching.SimilarityVectorFrequencies;
import gr.upatras.ceid.pprl.service.matching.LocalMatchingService;
import gr.upatras.ceid.pprl.service.matching.MatchingService;
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

    @CliAvailabilityIndicator(value = {
            "local_data_sample", "local_data_describe",
            "local_data_add_ulid", "local_data_sort_by_field"})
    public boolean availability0() {
        return lds != null;
    }
    @CliAvailabilityIndicator(value = {"local_data_stats"})
    public boolean availability1() { return lds != null && lms != null;}
    @CliAvailabilityIndicator(value = {"local_data_upload","data_download","import_dblp","data_describe"})
    public boolean availability2() { return ds != null;}
    @CliAvailabilityIndicator(value = {"data_stats"})
    public boolean availability3() { return ds != null && ms != null;}


    // TODO commands
    //      -HDFS-
    //      Sample (Might do it sequencially, might work with spark on this)
    //      Add Ulid (Might do it sequencially, any other ideas?)
    //      Sort by field (Mapreduce)

    @CliCommand(value = "local_data_sample", help = "View a sample of local data.")
    public String command0(
            @CliOption(key = {"avro"}, mandatory = true, help = "Local data avro files (comma separated) or including directory.")
            final String avroStr,
            @CliOption(key = {"schema"}, mandatory = true, help = "Local schema avro file.")
            final String schemaStr,
            @CliOption(key = {"size"}, mandatory = false, help = "(Optional) Sample size. Default is 10.")
            final String sizeStr,
            @CliOption(key = {"name"}, mandatory = false, help = "(Optional) Name to save sample to local filesystem. If not provided no sample will be saved.")
            final String nameStr,
            @CliOption(key = {"partitions"}, mandatory = false, help = "(Optional) Partitions of the output. Default is 1 (No partitioning).")
            final String partitionsStr
    ) {
        try{
            final Path schemaPath = CommandUtil.retrievePath(schemaStr);
            final Path[] avroPaths = CommandUtil.retrievePaths(avroStr);
            final int size = CommandUtil.retrieveInt(sizeStr, 10);
            final int partitions = CommandUtil.retrieveInt(partitionsStr,1);
            if (size < 1) throw new IllegalArgumentException("Sample size must be greater than zero.");
            final String name = CommandUtil.retrieveString(nameStr, null);
            final boolean save = !(name == null);
            if(save && !CommandUtil.isValidName(name)) throw new IllegalArgumentException("name is not valid");

            LOG.info("Sampling from local data :");
            LOG.info("\tSelected data path(s): {}", Arrays.toString(avroPaths));
            LOG.info("\tSelected schema path : {}", schemaPath);
            LOG.info("\tSample size : {} ",size);
            LOG.info("\tPartitions : {} ",(partitions==1)?"No partitioning":partitions);

            final Schema schema = lds.loadSchema(schemaPath);
            final GenericRecord[] sample = lds.sample(avroPaths, schemaPath, size);
            if(save) {
                final Path savePath = lds.saveRecords(name, sample, schema,partitions);
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
            LOG.info("\n");

            final Schema schema = lds.loadSchema(schemaPath);
            if(fields.length == 0 ) fields = DatasetsUtil.fieldNames(schema);
            else if(!DatasetsUtil.fieldNamesBelongsToSchema(schema,fields))
                throw new IllegalArgumentException(String.format("fields %s not found in schema",
                        Arrays.toString(fields)));

            final GenericRecord[] records = lds.loadRecords(avroPaths, schemaPath);
            final DatasetStatistics statistics = new DatasetStatistics();

            statistics.setRecordCount(records.length);
            statistics.setFieldNames(fields);
            DatasetStatistics.calculateQgramStatistics(records, schema, statistics, fields);

            final SimilarityVectorFrequencies matrix = lms.createMatrix(records,fields);
            final ExpectationMaximization estimator = lms.newEMInstance(fields,m0,u0,p0);
            estimator.runAlgorithm(matrix);

            statistics.setEmPairsCount(estimator.getPairCount());
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
            LOG.info("\n");

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
            @CliOption(key = {"name"}, mandatory = true, help = "Name to save the updated records.")
            final String name,
            @CliOption(key = {"field"}, mandatory = false, help = "(Optional) Name of the ULID field. Default is \"ulid\"")
            final String fieldStr,
            @CliOption(key = {"partitions"}, mandatory = false, help = "(Optional) Partitions of the output. Default is 1 (No partitioning).")
            final String partitionsStr
    ) {
        try {
            final Path schemaPath = CommandUtil.retrievePath(schemaStr);
            final Path[] avroPaths = CommandUtil.retrievePaths(avroStr);
            final String fieldName = CommandUtil.retrieveString(fieldStr,"ulid");
            final int partitions = CommandUtil.retrieveInt(partitionsStr,1);
            if(!CommandUtil.isValidFieldName(fieldName)) throw new IllegalArgumentException("Invalid field name.");

            LOG.info("Add a ULID field to local data:");
            LOG.info("\tSelected data files : {}", Arrays.toString(avroPaths));
            LOG.info("\tSelected schema file : {}", schemaPath);
            LOG.info("\tName : {}",name);
            LOG.info("\tField Name : {}",fieldName);
            LOG.info("\tPartitions : {} ",(partitions==1)?"No partitioning":partitions);
            LOG.info("\n");

            final Schema updatedSchema = DatasetsUtil.updateSchemaWithULID(lds.loadSchema(schemaPath),
                    fieldName);
            final GenericRecord[] updatedRecords = DatasetsUtil.updateRecordsWithULID(lds.loadRecords(avroPaths, schemaPath),
                    updatedSchema, fieldName);
            LOG.info(CommandUtil.prettyRecords(updatedRecords, updatedSchema));
            lds.saveRecords(name,updatedRecords,updatedSchema,partitions);
            return "DONE";
        } catch (Exception e) {
            return "Error. " + e.getClass().getSimpleName() + " : " + e.getMessage();
        }
    }

    @CliCommand(value = "data_download", help = "Download remote dataset to local machine.")
    public String command5(
            @CliOption(key = {"uploaded"}, mandatory = true, help = "Uploaded dataset name.")
            final String uplodadedName,
            @CliOption(key = {"name"}, mandatory = true, help = "Name to save to local disk.")
            final String name
    ) {
        try {
            LOG.info("Downloading remote files:");
            LOG.info("\tDataset name : {}",uplodadedName);
            LOG.info("\tDownload name : {}",name);
            final Path path = ds.downloadFiles(uplodadedName,name);
            LOG.info("\tFiles downloaded at : {}",path);
            LOG.info("\n");

            return "DONE";
        } catch (Exception e) {
            return "Error. " + e.getClass().getSimpleName() + " : " + e.getMessage();
        }
    }

    @CliCommand(value = "import_dblp", help = "Import DBLP(xml) to the PPRL-site")
    public String command6(
            @CliOption(key = {"xml"}, mandatory = true, help = "Local DBLP(xml) file.")
            final String xmlPathStr,
            @CliOption(key = {"name"}, mandatory = false, help = "(Optional) Name of the imported dataset. Default is \"dblp\".")
            String nameStr
    ) {
        try {
            final Path xmlPath = CommandUtil.retrievePath(xmlPathStr);
            final String name = CommandUtil.retrieveString(nameStr,"dblp");
            LOG.info("Importing DBLP:");
            LOG.info("\tXML path : " + xmlPath);
            LOG.info("\tName to be saved  : " + name);
            LOG.info("\n");

            final Path path = ds.importDblpXmlDataset(xmlPath,name);
            LOG.info("DBLP imported at : ", path);
            return "DONE";
        } catch (Exception e) {
            return "Error. " + e.getClass().getSimpleName() + " : " + e.getMessage();
        }
    }

    @CliCommand(value = "local_data_sort_by_field", help = "Sort local data records by a selected field name.")
    public String command7(
            @CliOption(key = {"avro"}, mandatory = true, help = "Local data avro files (comma separated) or including directory.")
            final String avroStr,
            @CliOption(key = {"schema"}, mandatory = true, help = "Local schema avro file.")
            final String schemaStr,
            @CliOption(key = {"name"}, mandatory = true, help = "Name to save the updated records.")
            final String name,
            @CliOption(key = {"fields"}, mandatory = true, help = "Fields to be used in sorting. Order matters. Sort by first field then second and so on.")
            final String fieldsStr,
            @CliOption(key = {"partitions"}, mandatory = false, help = "(Optional) Partitions of the output. Default is 1 (No partitioning).")
            final String partitionsStr

    ) {
        try {
            final Path schemaPath = CommandUtil.retrievePath(schemaStr);
            final Path[] avroPaths = CommandUtil.retrievePaths(avroStr);
            final String[] fieldNames = CommandUtil.retrieveFields(fieldsStr);
            final int partitions = CommandUtil.retrieveInt(partitionsStr,1);

            LOG.info("Sort by selected fields:");
            LOG.info("\tSelected data files : {}", Arrays.toString(avroPaths));
            LOG.info("\tSelected schema file : {}", schemaPath);
            LOG.info("\tName : {}",name);
            LOG.info("\tField Names : {}", Arrays.toString(fieldNames));
            LOG.info("\tPartitions : {} ",(partitions==1)?"No partitioning":partitions);
            LOG.info("\n");

            final Schema updatedSchema = DatasetsUtil.updateSchemaWithOrderByFields(lds.loadSchema(schemaPath),
                    fieldNames);
            final GenericRecord[] updatedRecords = DatasetsUtil.updateRecordsWithOrderByFields(
                    lds.loadRecords(avroPaths, schemaPath), updatedSchema);
            LOG.info(CommandUtil.prettyRecords(updatedRecords, updatedSchema));
            lds.saveRecords(name,updatedRecords,updatedSchema,partitions);

            return "DONE";
        } catch (Exception e) {
            return "Error. " + e.getClass().getSimpleName() + " : " + e.getMessage();
        }
    }


    @CliCommand(value = "data_stats", help = "Calculate usefull field statistics from hdfs data.")
    public String command8(
            @CliOption(key = {"uploaded"}, mandatory = true, help = "Uploaded dataset name.")
            final String uplodadedName,
            @CliOption(key = {"uid"}, mandatory = true, help = "Unique field name.")
            final String uidFieldName,
            @CliOption(key = {"fields"}, mandatory = true, help = "Filter fields to collect statistics from.")
            final String fieldsStr,
            @CliOption(key = {"name"}, mandatory = true, help = "Name to save statistics report to HDFS as properties file.")
            final String name,
            @CliOption(key = {"m"}, mandatory = false, help = "(Optional) Initial m values . 0.9 for all fields is default.")
            final String mStr,
            @CliOption(key = {"u"}, mandatory = false, help = "(Optional) Initial u values . 0.01 for all fields is default.")
            final String uStr,
            @CliOption(key = {"p"}, mandatory = false, help = "(Optional) Initial p value . 0.1 is default.")
            final String pStr,
            @CliOption(key = {"reducers"}, mandatory = false, help = "(Optional) Reducers count. Default is 2.")
            final String reducersStr
    ) {
        try {
            String[] fields = CommandUtil.retrieveFields(fieldsStr);
            final int reducersCount = CommandUtil.retrieveInt(reducersStr,2);
            if(!CommandUtil.isValidFieldName(uidFieldName)) throw new IllegalArgumentException("Invalid field name");
            if(!CommandUtil.isValidName(name)) throw new IllegalArgumentException("name is not valid");
            final double[] m0 = CommandUtil.retrieveProbabilities(mStr, fields.length, 0.9);
            final double[] u0 = CommandUtil.retrieveProbabilities(uStr, fields.length, 0.001);
            final double p0 = CommandUtil.retrieveProbability(pStr, 0.1);

            LOG.info("Calculating statistics on HDFS data:");
            LOG.info("\tDataset name : {}",uplodadedName);
            LOG.info("\tSelected fields : {}", Arrays.toString(fields));

            final Path[] paths = ds.retrieveDirectories(name);
            final Path basePath = paths[0];
            final Path avroPath = paths[1];
            final Path schemaPath = paths[2];

            final Schema schema = ds.loadSchema(schemaPath);
            if(fields.length == 0 ) fields = DatasetsUtil.fieldNames(schema);
            else if(!DatasetsUtil.fieldNamesBelongsToSchema(schema,fields))
                throw new IllegalArgumentException(String.format("fields %s not found in schema",
                        Arrays.toString(fields)));
            if(!DatasetsUtil.fieldNamesBelongsToSchema(schema,uidFieldName))
                throw new IllegalArgumentException(String.format("Field %s not found in schema",uidFieldName));
            final Path statsPath = new Path(basePath,"stats");

            final Path qGramsPropertiesPath = ds.countAvgQgrams(avroPath, schemaPath, statsPath, "avg_qgram_count", fields);

            final DatasetStatistics statistics = ds.loadStats(qGramsPropertiesPath);

            long recordCount = statistics.getRecordCount();
            final SimilarityVectorFrequencies matrix = ms.createMatrix(avroPath,schemaPath,uidFieldName,
                    recordCount,reducersCount,statsPath,"similarity_matrix",fields);


            final ExpectationMaximization estimator = ms.newEMInstance(fields,m0,u0,p0);
            estimator.runAlgorithm(matrix);

            statistics.setEmPairsCount(estimator.getPairCount());
            statistics.setEmAlgorithmIterations(estimator.getIteration());
            statistics.setP(estimator.getP());
            DatasetStatistics.calculateStatsUsingEstimates(
                    statistics,fields,
                    estimator.getM(),estimator.getU());

            final Path propertiesPath = ds.saveStats(name,statsPath,statistics);
            LOG.info("Stats saved at : {}",propertiesPath);
            LOG.info("\n");

            LOG.info(CommandUtil.prettyStats(statistics));

            return "DONE";
        } catch (Exception e) {
            return "Error. " + e.getClass().getSimpleName() + " : " + e.getMessage();
        }
    }


    @CliCommand(value = "data_describe", help = "View schema description of HDFS data.")
    public String command9(
            @CliOption(key = {"uploaded"}, mandatory = true, help = "Uploaded dataset name.")
            final String uplodadedName
    ) {
        try{
            LOG.info("Describing HDFS data :");
            LOG.info("\tDataset name : {}",uplodadedName);
            LOG.info("\n");

            final Path[] paths = ds.retrieveDirectories(uplodadedName);
            final Path schemaPath = paths[2];
            final Schema schema = ds.loadSchema(schemaPath);

            LOG.info(CommandUtil.prettySchemaDescription(schema));
            return "DONE";
        } catch (Exception e) {
            return "Error. " + e.getClass().getSimpleName() + " : " + e.getMessage();
        }
    }
}
