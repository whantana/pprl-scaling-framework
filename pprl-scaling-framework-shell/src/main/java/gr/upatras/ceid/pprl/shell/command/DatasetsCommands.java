package gr.upatras.ceid.pprl.shell.command;

import gr.upatras.ceid.pprl.datasets.DatasetException;
import gr.upatras.ceid.pprl.datasets.service.DatasetsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

@Component
public class DatasetsCommands implements CommandMarker {

    private static final Logger LOG = LoggerFactory.getLogger(DatasetsCommands.class);

    @Autowired
	@Qualifier("datasetsService")	
    private DatasetsService service;

    @CliCommand(value = "dat_import", help = "Import local avro file(s) and schema on the PPRL site.")
    public String datasetsImportCommand(
            @CliOption(key = {"avro_files"}, mandatory = true, help = "Local data avro files (comma separated) or including directory.")
            final String avroPaths,
            @CliOption(key = {"schema_file"}, mandatory = true, help = "Local schema avro file.")
            final String schemaFilePath,
            @CliOption(key = {"name"}, mandatory = true, help = "Dataset name.")
            final String name) {

        try {
            final File schemaFile = new File(schemaFilePath);
            if (!schemaFile.exists()) return "Error. Path \"" + schemaFilePath + "\" does not exist.";

            final File[] avroFiles = CommandUtils.retrieveFiles(avroPaths);

            final String[] absolutePaths = new String[avroFiles.length];
            for (int i = 0; i < avroFiles.length; i++) absolutePaths[i] = avroFiles[i].getAbsolutePath();

            if (!name.matches("^[a-z_A-Z][a-z_A-Z0-9]*$"))
                return "Error. Dataset name must contain only alphanumeric characters and underscores.";

            LOG.info("Importing local AVRO dataset :");
            LOG.info("\tImported Dataset name           : {}", name);
            LOG.info("\tSelected data files for import  : {}", Arrays.toString(absolutePaths));
            LOG.info("\tSelected schema file for import : {}", schemaFile.getAbsolutePath());

            service.importDataset(name, schemaFile, avroFiles);
            return "DONE";
        } catch (DatasetException e) {
            return "Error. " + e.getClass().getSimpleName() + " : " + e.getMessage();
        } catch (IOException e) {
            return "Error. " + e.getClass().getSimpleName() + " : " + e.getMessage();
        }
    }

    @CliCommand(value = "dat_import_dblp", help = "Import DBLP XML file(s) on the PPRL site.")
    public String datasetsImportDblpCommand(
            @CliOption(key = {"dblp_files"}, mandatory = true, help = "Local dblp files (XML format) .")
            final String dblpPaths,
            @CliOption(key = {"schema_file"}, mandatory = true, help = "Local schema avro file.")
            final String schemaFilePath,
            @CliOption(key = {"name"}, mandatory = true, help = "Dataset name.")
            final String name) {
        try {

            final File schemaFile = new File(schemaFilePath);
            if (!schemaFile.exists()) return "Error. Path \"" + schemaFilePath + "\" does not exist.";

            final File[] dblpFiles = CommandUtils.retrieveFiles(dblpPaths);

            final String[] absolutePaths = new String[dblpFiles.length];
            for (int i = 0; i < dblpFiles.length; i++) absolutePaths[i] = dblpFiles[i].getAbsolutePath();

            if (!name.matches("^[a-z_A-Z][a-z_A-Z0-9]*$"))
                return "Error. Dataset name must contain only alphanumeric characters and underscores.";

            LOG.info("Importing local DBLP(XML) dataset :");
            LOG.info("\tImported Dataset name           : {}", name);
            LOG.info("\tSelected data files for import  : {}", Arrays.toString(absolutePaths));
            LOG.info("\tSelected schema file for import : {}", schemaFile.getAbsolutePath());

            service.importDblpXmlDataset(name, schemaFile, dblpFiles);
            return "DONE";
        } catch (IOException e) {
            return "Error. " + e.getClass().getSimpleName() + " : " + e.getMessage();
        } catch (Exception e) {
            return "Error. " + e.getClass().getSimpleName() + " : " + e.getMessage();
        }
    }

    @CliCommand(value = "dat_list", help = "List user's imported datasets on the PPRL site.")
    public String datasetsListCommand() {
        final List<String> datasetsStrings = service.listDatasets(false);
        if(datasetsStrings.isEmpty()) {
            LOG.info("\tThere are no datasets imported yet.");
            return "DONE";
        }
        int i = 1;
        LOG.info("Listing user's imported datasets :");
        for(String s : datasetsStrings) {
            LOG.info("\t{}) {}",i++,s);
        }
        return "DONE";
    }

    @CliCommand(value = "dat_drop", help = "Drop user's imported datasets on the PPRL site.")
    public String datasetsDropCommand(
            @CliOption(key = {"name"}, mandatory = true, help = "Dataset name.")
            final String name,
            @CliOption(key = {"delete_files"}, mandatory = false, help = "(Optional) YES or NO (default) to completelly drop dataset directory.")
            final String deleteFilesStr) {
        try {
            boolean deleteFiles = false;
            if (deleteFilesStr != null) {
                if (!deleteFilesStr.equals("YES") && !deleteFilesStr.equals("NO"))
                    return "Error. Please provide \"YES\" or \"NO\".";
                deleteFiles = deleteFilesStr.equals("YES");
            }
            LOG.info("Droping dataset with name \"{}\" (DELETE FILES AS WELL ? {} ).", name, (deleteFiles ? "YES" : "NO"));
            service.dropDataset(name, deleteFiles);
            return "DONE";
        } catch (DatasetException e) {
            return "Error. " + e.getClass().getSimpleName() + " : " + e.getMessage();
        } catch (IOException e) {
            return "Error. " + e.getClass().getSimpleName() + " : " + e.getMessage();
        }
    }

    @CliCommand(value = "dat_drop_all", help = "Drop user's imported datasets on the PPRL site.")
    public String datasetsDropAllCommand(
            @CliOption(key = {"delete_files"}, mandatory = false, help = "(Optional) YES or NO (default) to completelly drop dataset directory.")
            final String deleteFilesStr) {
        try {
            boolean deleteFiles = false;
            if (deleteFilesStr != null) {
                if (!deleteFilesStr.equals("YES") && !deleteFilesStr.equals("NO"))
                    return "Error. Please provide \"YES\" or \"NO\".";
                deleteFiles = deleteFilesStr.equals("YES");
            }
            LOG.info("Droping all datasets (DELETE FILES AS WELL ? {} ).", (deleteFiles ? "YES" : "NO"));
            final List<String> names = service.listDatasets(true);
            for (String name : names) {
                LOG.info("Droping \"{}\".", name);
                service.dropDataset(name, deleteFiles);
            }
            return "DONE";
        } catch (DatasetException e) {
            return "Error. " + e.getClass().getSimpleName() + " : " + e.getMessage();
        } catch (IOException e) {
            return "Error. " + e.getClass().getSimpleName() + " : " + e.getMessage();
        }
    }

    @CliCommand(value = "dat_sample", help = "Sample a dataset.")
    public String datasetsSampleCommand(
            @CliOption(key = {"name"}, mandatory = true, help = "Dataset name.")
            final String name,
            @CliOption(key = {"size"}, mandatory = false, help = "(Optional) Sample size (default : 10).")
            final String sizeStr,
            @CliOption(key = {"sampleName"}, mandatory = false, help = "(Optional) If provided sample is saved at current working directory")
            final String sampleName) {
        try {
            int size = (sizeStr != null) ? Integer.parseInt(sizeStr) : 10;
            if (size < 1) throw new NumberFormatException("Sample size must be greater than zero.");
            final List<String> records;
            if (sampleName != null) {
                records = service.saveSampleOfDataset(name, size, sampleName);
                File[] files = new File[2];
                files[0] = new File(sampleName + ".avsc");
                if(files[0].exists()) LOG.info("Schema saved at : {} .", files[0].getAbsolutePath());
                files[1] = new File(sampleName + ".avro");
                if(files[1].exists()) LOG.info("Data saved at : {} .", files[1].getAbsolutePath());
            } else records = service.sampleOfDataset(name, size);
            LOG.info("Random sample of dataset \"{}\". Sample size : {} :", name, size);
            for (String record : records) LOG.info("\t{}", record);
            return "DONE";
        } catch (DatasetException e) {
            return "Error. " + e.getClass().getSimpleName() + " : " + e.getMessage();
        } catch (IOException e) {
            return "Error. " + e.getClass().getSimpleName() + " : " + e.getMessage();
        }
    }

    @CliCommand(value = "dat_calc_stats", help = "Calculate useful statistics of the imported dataset.")
    public String datasetsCalcStatsCommand(
            @CliOption(key = {"name"}, mandatory = true, help = "Dataset name.")
            final String name,
            @CliOption(key= {"Q"}, mandatory = false, help = "(Optional) Q for Q-Grams. Default value is 2.")
            final String Qstr) throws DatasetException {
        try {
            int Q = (Qstr == null) ? 2 : Integer.parseInt(Qstr);
            LOG.info("Calculating stats : [Average field length, Avergage {}-gram count] for dataset {}", Q, name);
            final String hdfsPath = service.calculateDatasetStats(name, Q);
            LOG.info("Calculated Stats stored at {}.", hdfsPath);
            return "DONE";
        } catch (Exception e) {
            return "Error. " + e.getClass().getSimpleName() + " : " + e.getMessage();
        }
    }

    @CliCommand(value = "dat_read_stats", help = "Read useful statistics of the imported dataset.")
    public String datasetsReadStatsCommand(
            @CliOption(key = {"name"}, mandatory = true, help = "Dataset name.")
            final String name,
            @CliOption(key = {"selected_fields"}, mandatory = false, help = "(Optional) Selected fields for stats")
            final String fieldsStr,
            @CliOption(key= {"Q"}, mandatory = false, help = "(Optional) Q for Q-Grams. Default value is 2.")
            final String Qstr) throws DatasetException {
        try {
            int Q = (Qstr == null) ? 2 : Integer.parseInt(Qstr);
            final String[] selectedFieldNames = (fieldsStr != null ) ? CommandUtils.retrieveFields(fieldsStr) :
                    null;
            LOG.info("Reading stats : [Average field length, Avergage {}-gram count]" +
                    " for dataset {} on {}-grams", name , Q,Q);
            final Map<String,double[]> stats = service.readDatasetStats(name, Q,selectedFieldNames);
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

    @CliCommand(value = "dat_calc_local_stats", help = "Read useful statistics of a local dataset.")
    public String datasetsCalcReadLocalStatsCommand(
            @CliOption(key = {"avro_files"}, mandatory = true, help = "Local data avro files (comma separated) or including directory.")
            final String avroPaths,
            @CliOption(key = {"schema_file"}, mandatory = true, help = "Local schema avro file.")
            final String schemaFilePath,
            @CliOption(key = {"selected_fields"}, mandatory = false, help = "(Optional) Selected fields for stats")
            final String fieldsStr,
            @CliOption(key= {"Q"}, mandatory = false, help = "(Optional) Q for Q-Grams. Default value is 2.")
            final String Qstr) throws DatasetException {
        try {
            final File schemaFile = new File(schemaFilePath);
            if (!schemaFile.exists()) return "Error. Path \"" + schemaFilePath + "\" does not exist.";

            final File[] avroFiles = CommandUtils.retrieveFiles(avroPaths);

            final String[] absolutePaths = new String[avroFiles.length];
            for (int i = 0; i < avroFiles.length; i++) absolutePaths[i] = avroFiles[i].getAbsolutePath();

            final String[] selectedFieldNames = (fieldsStr != null ) ? CommandUtils.retrieveFields(fieldsStr) :
                    null;

            int Q = (Qstr == null) ? 2 : Integer.parseInt(Qstr);

            LOG.info("Calculating [Average field length, Avergage {}-gram count] local AVRO dataset :",Q);
            LOG.info("\tSelected data files for import  : {}", Arrays.toString(absolutePaths));
            LOG.info("\tSelected schema file for import : {}", schemaFile.getAbsolutePath());
            LOG.info("\tReading stats on Q-grams with Q : {}", Q);
            if(selectedFieldNames!=null) LOG.info("\tSelected fields : {}",
                    Arrays.toString(selectedFieldNames));
            LOG.info("\n");
            final Set<File> avroFilesSet = new TreeSet<File>(Arrays.asList(avroFiles));
            final Map<String,double[]>
                    stats = service.calculateLocalDataStats(avroFilesSet, schemaFile, selectedFieldNames, Q);
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


    @CliCommand(value = "dat_describe", help = "Get Aro schema of the imported dataset.")
    public String datasetsDescribeCommand(
            @CliOption(key = {"name"}, mandatory = true, help = "Dataset name.")
            final String name) {
        try {
            Map<String, String> desc = service.describeDataset(name);
            int i = 1;
            LOG.info("Schema description for dataset \"{}\" : ", name);
            for (Map.Entry<String, String> e : desc.entrySet()) {
                LOG.info("\t{}.{} : {}", i++, e.getKey(), e.getValue());
            }
            return "DONE";
        } catch (DatasetException e) {
            return "Error. " + e.getClass().getSimpleName() + " : " + e.getMessage();
        } catch (IOException e) {
            return "Error. " + e.getClass().getSimpleName() + " : " + e.getMessage();
        }
    }

    // TODO a command that prints avro file with schema
}
