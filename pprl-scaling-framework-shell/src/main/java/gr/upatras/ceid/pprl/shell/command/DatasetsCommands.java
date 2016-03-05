package gr.upatras.ceid.pprl.shell.command;

import gr.upatras.ceid.pprl.base.CombinatoricsUtil;
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

    @CliAvailabilityIndicator(value = {"local_data_sample","local_data_describe"})
    public boolean availability0() {
        return lds != null;
    }
    @CliAvailabilityIndicator(value = {"local_data_stats"})
    public boolean availability1() { return lds != null && lms != null;}

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
            final Path schemaPath = CommandUtils.retrievePath(schemaStr);
            final Path[] avroPaths = CommandUtils.retrievePaths(avroStr);
            final int size = CommandUtils.retrieveInt(sizeStr, 10);
            if (size < 1) throw new IllegalArgumentException("Sample size must be greater than zero.");
            final String name = CommandUtils.retrieveString(nameStr, null);
            final boolean save = !(name == null);
            if(save && !CommandUtils.isValidName(name)) throw new IllegalArgumentException("name is not valid");

            LOG.info("Sampling from local data :");
            LOG.info("\tSelected data path(s): {}", Arrays.toString(avroPaths));
            LOG.info("\tSelected schema path : {}", schemaPath);
            LOG.info("\tSample size : {} ",size);
            if(save) LOG.info("\tSaving sample with name : {} ",name);
            LOG.info("\n");

            final Schema schema = lds.schemaOfLocalDataset(schemaPath);
            final GenericRecord[] sample = lds.sampleOfLocalDataset(avroPaths,schemaPath,size);
            if(save) lds.localSaveOfSample(name,sample,schema);

            LOG.info(CommandUtils.prettyRecords(sample,schema));
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
            final Path schemaPath = CommandUtils.retrievePath(schemaStr);

            LOG.info("Describing local data :");
            LOG.info("\tSelected schema file : {}", schemaPath);
            LOG.info("\n");

            final Schema schema = lds.schemaOfLocalDataset(schemaPath);

            LOG.info(CommandUtils.prettySchemaDescription(schema));
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
            final Path schemaPath = CommandUtils.retrievePath(schemaStr);
            final Path[] avroPaths = CommandUtils.retrievePaths(avroStr);
            String[] fields = CommandUtils.retrieveFields(fieldsStr);
            final String name = CommandUtils.retrieveString(nameStr, null);
            final boolean save = name != null;
            if(save && !CommandUtils.isValidName(name)) throw new IllegalArgumentException("name is not valid");
            final double[] m0 = CommandUtils.retrieveProbabilities(mStr, fields.length,0.9);
            final double[] u0 = CommandUtils.retrieveProbabilities(uStr, fields.length, 0.001);
            final double p0 = CommandUtils.retrieveProbability(pStr,0.1);

            LOG.info("Calculating statistics on local data:");
            LOG.info("\tSelected data files : {}", Arrays.toString(avroPaths));
            LOG.info("\tSelected schema file : {}", schemaPath);
            LOG.info("\tSelected fields : {}", Arrays.toString(fields));
            if(save) LOG.info("\tStatistics report with name : {}",name);
            LOG.info("\n");

            final Schema schema = lds.schemaOfLocalDataset(schemaPath);
            if(fields.length == 0 )
                fields = DatasetsUtil.fieldNames(schema);
            else if(!Arrays.asList(DatasetsUtil.fieldNames(schema)).containsAll(Arrays.asList(fields)))
                throw new IllegalArgumentException(String.format("fields %s not found in schema",
                        Arrays.toString(fields)));

            final GenericRecord[] records = lds.loadLocalDataset(avroPaths,schemaPath);
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

            LOG.info(CommandUtils.prettyStats(statistics));

            if(save) lds.localSaveOfStatsProperties(name, statistics);

            return "DONE";
        } catch (Exception e) {
            return "Error. " + e.getClass().getSimpleName() + " : " + e.getMessage();
        }
    }

//    @CliAvailabilityIndicator(value = {"dat_sample","dat_describe"})
//    public boolean datasetCommandsAvailability() {
//        return ds != null;
//    }
//
//    @CliCommand(value = "dat_sample", help = " describe (dummy).")
//    public String sampleDummy() {
//        return "DUMMY!";
//    }
//
//    @CliCommand(value = "dat_describe", help = "stats (dummy).")
//    public String sampleDescribe() {
//        return "DUMMY!";
//    }



//    @CliCommand(value = "dat_import", help = "Import local avro file(s) and schema on the PPRL site.")
//    public String datasetsImportCommand(
//            @CliOption(key = {"avro_files"}, mandatory = true, help = "Local data avro files (comma separated) or including directory.")
//            final String avroPaths,
//            @CliOption(key = {"schema_file"}, mandatory = true, help = "Local schema avro file.")
//            final String schemaFilePath,
//            @CliOption(key = {"name"}, mandatory = true, help = "Dataset name.")
//            final String name) {
//
//        try {
//            final File schemaFile = new File(schemaFilePath);
//            if (!schemaFile.exists()) return "Error. Path \"" + schemaFilePath + "\" does not exist.";
//
//            final File[] avroFiles = CommandUtils.retrieveFiles(avroPaths);
//
//            final String[] absolutePaths = new String[avroFiles.length];
//            for (int i = 0; i < avroFiles.length; i++) absolutePaths[i] = avroFiles[i].getAbsolutePath();
//
//            if (!name.matches("^[a-z_A-Z][a-z_A-Z0-9]*$"))
//                return "Error. Dataset name must contain only alphanumeric characters and underscores.";
//
//            LOG.info("Importing local AVRO dataset :");
//            LOG.info("\tImported Dataset name           : {}", name);
//            LOG.info("\tSelected data files for import  : {}", Arrays.toString(absolutePaths));
//            LOG.info("\tSelected schema file for import : {}", schemaFile.getAbsolutePath());
//
//            ds.importDataset(name, schemaFile, avroFiles);
//            return "DONE";
//        } catch (DatasetException e) {
//            return "Error. " + e.getClass().getSimpleName() + " : " + e.getMessage();
//        } catch (IOException e) {
//            return "Error. " + e.getClass().getSimpleName() + " : " + e.getMessage();
//        }
//    }
//
//    @CliCommand(value = "dat_import_dblp", help = "Import DBLP XML file(s) on the PPRL site.")
//    public String datasetsImportDblpCommand(
//            @CliOption(key = {"dblp_files"}, mandatory = true, help = "Local dblp files (XML format) .")
//            final String dblpPaths,
//            @CliOption(key = {"schema_file"}, mandatory = true, help = "Local schema avro file.")
//            final String schemaFilePath,
//            @CliOption(key = {"name"}, mandatory = true, help = "Dataset name.")
//            final String name) {
//        try {
//
//            final File schemaFile = new File(schemaFilePath);
//            if (!schemaFile.exists()) return "Error. Path \"" + schemaFilePath + "\" does not exist.";
//
//            final File[] dblpFiles = CommandUtils.retrieveFiles(dblpPaths);
//
//            final String[] absolutePaths = new String[dblpFiles.length];
//            for (int i = 0; i < dblpFiles.length; i++) absolutePaths[i] = dblpFiles[i].getAbsolutePath();
//
//            if (!name.matches("^[a-z_A-Z][a-z_A-Z0-9]*$"))
//                return "Error. Dataset name must contain only alphanumeric characters and underscores.";
//
//            LOG.info("Importing local DBLP(XML) dataset :");
//            LOG.info("\tImported Dataset name           : {}", name);
//            LOG.info("\tSelected data files for import  : {}", Arrays.toString(absolutePaths));
//            LOG.info("\tSelected schema file for import : {}", schemaFile.getAbsolutePath());
//
//            ds.importDblpXmlDataset(name, schemaFile, dblpFiles);
//            return "DONE";
//        } catch (IOException e) {
//            return "Error. " + e.getClass().getSimpleName() + " : " + e.getMessage();
//        } catch (Exception e) {
//            return "Error. " + e.getClass().getSimpleName() + " : " + e.getMessage();
//        }
//    }
//
//    @CliCommand(value = "dat_list", help = "List user's imported datasets on the PPRL site.")
//    public String datasetsListCommand() {
//        final List<String> datasetsStrings = ds.listDatasets(false);
//        if(datasetsStrings.isEmpty()) {
//            LOG.info("\tThere are no datasets imported yet.");
//            return "DONE";
//        }
//        int i = 1;
//        LOG.info("Listing user's imported datasets :");
//        for(String s : datasetsStrings) {
//            LOG.info("\t{}) {}",i++,s);
//        }
//        return "DONE";
//    }
//
//    @CliCommand(value = "dat_drop", help = "Drop user's imported datasets on the PPRL site.")
//    public String datasetsDropCommand(
//            @CliOption(key = {"name"}, mandatory = true, help = "Dataset name.")
//            final String name,
//            @CliOption(key = {"delete_files"}, mandatory = false, help = "(Optional) YES or NO (default) to completelly drop dataset directory.")
//            final String deleteFilesStr) {
//        try {
//            boolean deleteFiles = false;
//            if (deleteFilesStr != null) {
//                if (!deleteFilesStr.equals("YES") && !deleteFilesStr.equals("NO"))
//                    return "Error. Please provide \"YES\" or \"NO\".";
//                deleteFiles = deleteFilesStr.equals("YES");
//            }
//            LOG.info("Droping dataset with name \"{}\" (DELETE FILES AS WELL ? {} ).", name, (deleteFiles ? "YES" : "NO"));
//            ds.dropDataset(name, deleteFiles);
//            return "DONE";
//        } catch (DatasetException e) {
//            return "Error. " + e.getClass().getSimpleName() + " : " + e.getMessage();
//        } catch (IOException e) {
//            return "Error. " + e.getClass().getSimpleName() + " : " + e.getMessage();
//        }
//    }
//
//    @CliCommand(value = "dat_drop_all", help = "Drop user's imported datasets on the PPRL site.")
//    public String datasetsDropAllCommand(
//            @CliOption(key = {"delete_files"}, mandatory = false, help = "(Optional) YES or NO (default) to completelly drop dataset directory.")
//            final String deleteFilesStr) {
//        try {
//            boolean deleteFiles = false;
//            if (deleteFilesStr != null) {
//                if (!deleteFilesStr.equals("YES") && !deleteFilesStr.equals("NO"))
//                    return "Error. Please provide \"YES\" or \"NO\".";
//                deleteFiles = deleteFilesStr.equals("YES");
//            }
//            LOG.info("Droping all datasets (DELETE FILES AS WELL ? {} ).", (deleteFiles ? "YES" : "NO"));
//            final List<String> names = ds.listDatasets(true);
//            for (String name : names) {
//                LOG.info("Droping \"{}\".", name);
//                ds.dropDataset(name, deleteFiles);
//            }
//            return "DONE";
//        } catch (DatasetException e) {
//            return "Error. " + e.getClass().getSimpleName() + " : " + e.getMessage();
//        } catch (IOException e) {
//            return "Error. " + e.getClass().getSimpleName() + " : " + e.getMessage();
//        }
//    }
//
//    @CliCommand(value = "dat_sample", help = "Sample a dataset.")
//    public String datasetsSampleCommand(
//            @CliOption(key = {"name"}, mandatory = true, help = "Dataset name.")
//            final String name,
//            @CliOption(key = {"size"}, mandatory = false, help = "(Optional) Sample size (default : 10).")
//            final String sizeStr,
//            @CliOption(key = {"sampleName"}, mandatory = false, help = "(Optional) If provided sample is saved at current working directory")
//            final String sampleName) {
//        try {
//            int size = (sizeStr != null) ? Integer.parseInt(sizeStr) : 10;
//            if (size < 1) throw new NumberFormatException("Sample size must be greater than zero.");
//            final List<String> records;
//            if (sampleName != null) {
//                records = ds.saveSampleOfDataset(name, size, sampleName);
//                File[] files = new File[2];
//                files[0] = new File(sampleName + ".avsc");
//                if(files[0].exists()) LOG.info("Schema saved at : {} .", files[0].getAbsolutePath());
//                files[1] = new File(sampleName + ".avro");
//                if(files[1].exists()) LOG.info("Data saved at : {} .", files[1].getAbsolutePath());
//            } else records = ds.sampleOfDataset(name, size);
//            LOG.info("Random sample of dataset \"{}\". Sample size : {} :", name, size);
//            for (String record : records) LOG.info("\t{}", record);
//            return "DONE";
//        } catch (DatasetException e) {
//            return "Error. " + e.getClass().getSimpleName() + " : " + e.getMessage();
//        } catch (IOException e) {
//            return "Error. " + e.getClass().getSimpleName() + " : " + e.getMessage();
//        }
//    }
//
//    @CliCommand(value = "dat_calc_stats", help = "Calculate useful statistics of the imported dataset.")
//    public String datasetsCalcStatsCommand(
//            @CliOption(key = {"name"}, mandatory = true, help = "Dataset name.")
//            final String name,
//            @CliOption(key= {"Q"}, mandatory = false, help = "(Optional) Q for Q-Grams. Default value is 2.")
//            final String Qstr) throws DatasetException {
//        try {
//            int Q = (Qstr == null) ? 2 : Integer.parseInt(Qstr);
//            LOG.info("Calculating stats : [Average field length, Avergage {}-gram count] for dataset {}", Q, name);
//            final String hdfsPath = ds.calculateDatasetStats(name, Q);
//            LOG.info("Calculated Stats stored at {}.", hdfsPath);
//            return "DONE";
//        } catch (Exception e) {
//            return "Error. " + e.getClass().getSimpleName() + " : " + e.getMessage();
//        }
//    }
//
//    @CliCommand(value = "dat_read_stats", help = "Read useful statistics of the imported dataset.")
//    public String datasetsReadStatsCommand(
//            @CliOption(key = {"name"}, mandatory = true, help = "Dataset name.")
//            final String name,
//            @CliOption(key = {"selected_fields"}, mandatory = false, help = "(Optional) Selected fields for stats")
//            final String fieldsStr,
//            @CliOption(key= {"Q"}, mandatory = false, help = "(Optional) Q for Q-Grams. Default value is 2.")
//            final String Qstr) throws DatasetException {
//        try {
//            int Q = (Qstr == null) ? 2 : Integer.parseInt(Qstr);
//            final String[] selectedFieldNames = (fieldsStr != null ) ? CommandUtils.retrieveFields(fieldsStr) :
//                    null;
//            LOG.info("Reading stats : [Average field length, Avergage {}-gram count]" +
//                    " for dataset {} on {}-grams", name , Q,Q);
//            final Map<String,double[]> stats = ds.readDatasetStats(name, Q,selectedFieldNames);
//            StringBuilder sb = new StringBuilder();
//            for(Map.Entry<String,double[]> entry : stats.entrySet()) {
//                sb.append(entry.getKey())
//                        .append(" : ")
//                        .append(CommandUtils.prettyStats(entry.getValue(),Q)).append("\n");
//            }
//            LOG.info(sb.toString());
//            return "DONE";
//        } catch (Exception e) {
//            return "Error. " + e.getClass().getSimpleName() + " : " + e.getMessage();
//        }
//    }
//

//
//
//    @CliCommand(value = "dat_describe", help = "Get Aro schema of the imported dataset.")
//    public String datasetsDescribeCommand(
//            @CliOption(key = {"name"}, mandatory = true, help = "Dataset name.")
//            final String name) {
//        try {
//            Map<String, String> desc = ds.describeDataset(name);
//            int i = 1;
//            LOG.info("Schema description for dataset \"{}\" : ", name);
//            for (Map.Entry<String, String> e : desc.entrySet()) {
//                LOG.info("\t{}.{} : {}", i++, e.getKey(), e.getValue());
//            }
//            return "DONE";
//        } catch (DatasetException e) {
//            return "Error. " + e.getClass().getSimpleName() + " : " + e.getMessage();
//        } catch (IOException e) {
//            return "Error. " + e.getClass().getSimpleName() + " : " + e.getMessage();
//        }
//    }
//
}
