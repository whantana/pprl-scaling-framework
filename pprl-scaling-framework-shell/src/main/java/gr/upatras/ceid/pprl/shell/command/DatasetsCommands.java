package gr.upatras.ceid.pprl.shell.command;

import gr.upatras.ceid.pprl.datasets.service.DatasetsService;
import gr.upatras.ceid.pprl.datasets.service.LocalDatasetsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliAvailabilityIndicator;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Component
public class DatasetsCommands implements CommandMarker {

    private static final Logger LOG = LoggerFactory.getLogger(DatasetsCommands.class);

    @Autowired(required = false)
    @Qualifier("datasetsService")
    private DatasetsService ds;

    @Autowired(required = false)
    @Qualifier("localDatasetsService")
    private LocalDatasetsService lds;

    @CliAvailabilityIndicator(value = {"local_dat_sample","local_dat_describe","local_dat_stats"})
    public boolean localDatasetCommandsAvailability() {
        return lds != null;
    }


    @CliCommand(value = "local_dat_sample", help = "View a sample of local data.")
    public String sampleOfLocalDatasetCommand(
            @CliOption(key = {"avro_files"}, mandatory = true, help = "Local data avro files (comma separated) or including directory.")
            final String avroPaths,
            @CliOption(key = {"schema_file"}, mandatory = true, help = "Local schema avro file.")
            final String schemaFilePath,
            @CliOption(key = {"sample_size"}, mandatory = false, help = "(Optional) Sample size (default : 10).")
            final String sizeStr
    ) {
        try{
            final File schemaFile = new File(schemaFilePath);
            if (!schemaFile.exists()) return "Error. Path \"" + schemaFilePath + "\" does not exist.";

            final File[] avroFiles = CommandUtils.retrieveFiles(avroPaths);

            int size = (sizeStr != null) ? Integer.parseInt(sizeStr) : 10;
            if (size < 1) throw new NumberFormatException("Sample size must be greater than zero.");

            final String[] absolutePaths = new String[avroFiles.length];
            for (int i = 0; i < avroFiles.length; i++) absolutePaths[i] = avroFiles[i].getAbsolutePath();

            LOG.info("Sampling from local data :");
            LOG.info("\tSelected data files : {}", Arrays.toString(absolutePaths));
            LOG.info("\tSelected schema file : {}", schemaFile.getAbsolutePath());
            LOG.info("\tSample size : {} ",size);
            LOG.info("\n");

            List<String> recordStrings = lds.sampleOfLocalDataset(avroFiles,schemaFile,size);
            StringBuilder sb = new StringBuilder();
            int i = 1;
            for(String string : recordStrings)
                sb.append(String.format("%d, %s\n",i++,string));
            LOG.info(sb.toString());
            return "DONE";
        } catch (Exception e) {
            return "Error. " + e.getClass().getSimpleName() + " : " + e.getMessage();
        }
    }

    @CliCommand(value = "local_dat_describe", help = "View a schema desription of local data.")
    public String describeLocalDatasetCommand(
            @CliOption(key = {"schema_file"}, mandatory = true, help = "Local schema avro file.")
            final String schemaFilePath) {
        try {
            final File schemaFile = new File(schemaFilePath);
            if (!schemaFile.exists()) return "Error. Path \"" + schemaFilePath + "\" does not exist.";

            LOG.info("Describing local data :");
            LOG.info("\tSelected schema file : {}", schemaFile.getAbsolutePath());
            LOG.info("\n");

            Map<String,String> description = lds.describeLocalDataset(schemaFile);
            StringBuilder sb = new StringBuilder();
            int i = 1;
            for(Map.Entry<String,String> entry : description.entrySet())
                sb.append(String.format("%d, %s %s\n",i++,entry.getKey(),entry.getValue()));
            LOG.info(sb.toString());
            return "DONE";
        } catch (Exception e) {
            return "Error. " + e.getClass().getSimpleName() + " : " + e.getMessage();
        }
    }


    @CliCommand(value = "local_dat_stats", help = "Calculate usefull field statistics from local data.")
    public String calculateStatisticsLocalDatasetCommand(
            @CliOption(key = {"avro_files"}, mandatory = true, help = "Local data avro files (comma separated) or including directory.")
            final String avroPaths,
            @CliOption(key = {"schema_file"}, mandatory = true, help = "Local schema avro file.")
            final String schemaFilePath,
            @CliOption(key = {"selected_fields"}, mandatory = false, help = "(Optional) Selected fields for stats")
            final String fieldsStr) {
        try {
            final File schemaFile = new File(schemaFilePath);
            if (!schemaFile.exists()) return "Error. Path \"" + schemaFilePath + "\" does not exist.";

            final File[] avroFiles = CommandUtils.retrieveFiles(avroPaths);

            final String[] absolutePaths = new String[avroFiles.length];
            for (int i = 0; i < avroFiles.length; i++) absolutePaths[i] = avroFiles[i].getAbsolutePath();

            final String[] selectedFieldNames = (fieldsStr != null ) ?
                    CommandUtils.retrieveFields(fieldsStr) : null;

            LOG.info("Calculating [Average field length, Avergage 2-gram count, Avergage 3-gram count," +
                    " Avergage 4-gram count] local AVRO dataset :");
            LOG.info("\tSelected data files for import  : {}", Arrays.toString(absolutePaths));
            LOG.info("\tSelected schema file for import : {}", schemaFile.getAbsolutePath());
            if(selectedFieldNames!=null) LOG.info("\tSelected fields : {}",
                    Arrays.toString(selectedFieldNames));
            LOG.info("\n");

            final Map<String,double[]>
                      stats = lds.calculateStatisticsLocalDataset(avroFiles, schemaFile, selectedFieldNames);
            StringBuilder sb = new StringBuilder();
              for(Map.Entry<String,double[]> entry : stats.entrySet()) {
                  sb.append(entry.getKey())
                          .append(" : ")
                          .append(CommandUtils.prettyStats(entry.getValue())).append("\n");
              }
              LOG.info(sb.toString());
              return "DONE";
        } catch (Exception e) {
            return "Error. " + e.getClass().getSimpleName() + " : " + e.getMessage();
        }
    }

    @CliAvailabilityIndicator(value = {"dat_sample","dat_describe","dat_stats"})
    public boolean datasetCommandsAvailability() {
        return ds != null;
    }

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
