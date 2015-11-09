package gr.upatras.ceid.pprl.shell.command;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import gr.upatras.ceid.pprl.datasets.DatasetException;
import gr.upatras.ceid.pprl.datasets.service.DatasetsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

import java.io.File;
import java.util.Map;

@Component
public class DatasetsCommands implements CommandMarker {

    private static final Logger LOG = LoggerFactory.getLogger(DatasetsCommands.class);

    @Autowired
    private DatasetsService service;

    @CliCommand(value = "dat_import", help = "Import a local avro file and schema on the PPRL site.")
    public String datasetsImportCommand(
            @CliOption(key = {"avro_file"}, mandatory = false, help = "Local data avro file.")
            final String avroPath,
            @CliOption(key = {"avro_files"}, mandatory = false, help = "Local data avro files (comma separated).")
            final String avroPaths,
            @CliOption(key = {"schema_file"}, mandatory = true, help = "Local schema avro file.")
            final String schemaFilePath,
            @CliOption(key = {"name"}, mandatory = true, help = "(Optional) Dataset name.")
            final String name) {

        final File schemaFile = new File(schemaFilePath);
        if (!schemaFile.exists()) return "Error. Path \"" + schemaFilePath + "\" does not exist.";

        boolean avroFileProvided = (avroPath != null);
        boolean avroFilesProvided = (avroPaths != null);
        if(avroFileProvided && avroFilesProvided)
            return "Error. Please provided either single or multiple files as input";
        if(!(avroFileProvided || avroFilesProvided))
            return "Error. Please provided either single or multiple files as input";

        File[] avroFiles;
        if(avroFileProvided) {
            avroFiles = new File[1];
            avroFiles[0] = new File(avroPath);
            if (!avroFiles[0].exists()) return "Error. Path \"" + avroPath + "\" does not exist.";
        }
        else {
            if(!avroPaths.contains(",")) return "Error. Paths provided must be comma separated.";
            final String[] paths = avroPaths.split(",");
            avroFiles = new File[paths.length];
            for (int i = 0; i < paths.length; i++) {
                avroFiles[i] = new File(paths[i]);
                if (!avroFiles[i].exists()) return "Error. Path \"" + paths[i] + "\" does not exist.";
            }
        }

        final String[] absolutePaths = new String[avroFiles.length];
        for (int i = 0; i < avroFiles.length; i++) absolutePaths[i] = avroFiles[i].getAbsolutePath();

        if(!name.matches("^[a-zA-Z0-9]*$")) return "Error. Dataset name must contain only alphanumeric characters.";

        LOG.info("Importing local AVRO dataset :");
        LOG.info("\tImported Dataset name           : {}",name);
        LOG.info("\tSelected data files for import : {}",Arrays.toString(absolutePaths));
        LOG.info("\tSelected schema file for import : {}",schemaFile.getAbsolutePath());

        try {
            service.importDataset(name,schemaFile,avroFiles);
        } catch (Exception e) {
            return "Error." + e.getMessage();
        }
        return "DONE";
    }

    @CliCommand(value = "dat_import_dblp", help = "Import the dblp.xml file on the PPRL site.")
    public String datasetsImportDblpCommand(
            @CliOption(key = {"dblp_file"}, mandatory = false, help = "Local dblp file (XML format).")
            final String dblpPath,
            @CliOption(key = {"dblp_files"}, mandatory = false, help = "Local dblp files (XML format).")
            final String dblpPaths,
            @CliOption(key = {"schema_file"}, mandatory = true, help = "Local schema avro file.")
            final String schemaFilePath,
            @CliOption(key = {"name"}, mandatory = false, help = "(Optional) Dataset name.")
            final String name) {

        final File schemaFile = new File(schemaFilePath);
        if (!schemaFile.exists()) return "Error. Path \"" + schemaFilePath + "\" does not exist.";

        boolean dblpFileProvided = (dblpPath != null);
        boolean dblpFilesProvided = (dblpPaths != null);
        if(dblpFileProvided && dblpFilesProvided)
            return "Error. Please provided either single or multiple files as input";
        if(!(dblpFileProvided || dblpFilesProvided))
            return "Error. Please provided either single or multiple files as input";

        File[] dblpFiles;
        if(dblpFileProvided) {
            dblpFiles = new File[1];
            dblpFiles[0] = new File(dblpPath);
            if (!dblpFiles[0].exists()) return "Error. Path \"" + dblpPath + "\" does not exist.";
        }
        else {
            if(!dblpPaths.contains(",")) return "Error. Paths provided must be comma separated.";
            final String[] paths = dblpPaths.split(",");
            dblpFiles = new File[paths.length];
            for (int i = 0; i < paths.length; i++) {
                dblpFiles[i] = new File(paths[i]);
                if (!dblpFiles[i].exists()) return "Error. Path \"" + paths[i] + "\" does not exist.";
            }
        }

        final String[] absolutePaths = new String[dblpFiles.length];
        for (int i = 0; i < dblpFiles.length; i++) absolutePaths[i] = dblpFiles[i].getAbsolutePath();

        if(!name.matches("^[a-zA-Z0-9]*$")) return "Error. Dataset name must contain only alphanumeric characters.";

        LOG.info("Importing local DBLP(XML) dataset :");
        LOG.info("\tImported Dataset name           : {}",name);
        LOG.info("\tSelected data files for import : {}",Arrays.toString(absolutePaths));
        LOG.info("\tSelected schema file for import : {}",schemaFile.getAbsolutePath());

        try {
            service.importDblpXmlDataset(name,schemaFile,dblpFiles);
        } catch (Exception e) {
            return "Error." + e.getMessage();
        }
        return "DONE";
    }

    @CliCommand(value = "dat_list", help = "List user's imported datasets on the PPRL site.")
    public String datasetsListCommand() {
        LOG.info("Listing user's imported datasets ( name => path) :");
        final List<String> names = service.listDatasets();
        if(names.isEmpty()) {
            LOG.info("\tThere are no datasets imported yet.");
            return "DONE";
        }
        int i = 1;
        for(String s : names) {
            LOG.info("\t{}) {}",i++,s);
        }
        return "DONE";
    }

    @CliCommand(value = "dat_drop", help = "Drop user's imported datasets on the PPRL site.")
    public String datasetsDropCommand(
            @CliOption(key = {"name"}, mandatory = true, help = "Dataset name.")
            final String name,
            @CliOption(key = {"delete_files"}, mandatory = false, help = "YES or NO (default) to completelly drop dataset directory.",
                    specifiedDefaultValue="NO")
            final String deleteFilesStr) {
        if(!deleteFilesStr.equals("YES") && !deleteFilesStr.equals("NO")) return "Error. Please provide \"YES\" or \"NO\".";
        boolean deleteFiles = deleteFilesStr.equals("YES");
        LOG.info("Droping dataset with name \"{}\" (DELETE FILES AS WELL ? {} ).",name,deleteFilesStr);
        try {
            service.dropDataset(name,deleteFiles);
        } catch (IOException e) {
            return "Error." + e.getMessage();
        } catch (DatasetException e) {
            return "Error." + e.getMessage();
        }
        return "DONE";
    }

    @CliCommand(value = "dat_drop_all", help = "Drop user's imported datasets on the PPRL site.")
    public String datasetsDropAllCommand(
            @CliOption(key = {"delete_files"}, mandatory = false, help = "YES or NO (default) to completelly drop dataset directory.",
                    specifiedDefaultValue="NO")
            final String deleteFilesStr) {
        if(!deleteFilesStr.equals("YES") && !deleteFilesStr.equals("NO")) return "Error. Please provide \"YES\" or \"NO\".";
        boolean deleteFiles = deleteFilesStr.equals("YES");
        LOG.info("Droping all datasets (DELETE FILES AS WELL ?" + deleteFiles +").");
        final List<String> names = service.listDatasets();
        try{
            for(String name : names) {
                LOG.info("Droping \"{}\".",name);
                service.dropDataset(name, deleteFiles);
            }
        } catch (IOException e) {
            return "Error." + e.getMessage();
        } catch (DatasetException e) {
            return "Error." + e.getMessage();
        }
        return "DONE";
    }

    @CliCommand(value = "dat_sample", help = "Sample a user's imported dataset.")
    public String datasetsSampleCommand(
            @CliOption(key = {"name"}, mandatory = true, help = "Dataset name.")
            final String name,
            @CliOption(key = {"size"}, mandatory = false, help = "Sampe size (default : 10).",
                    specifiedDefaultValue="10")
            final String str) {
        int size = 10;
        try{
            size = Integer.parseInt(str);
        } catch (NumberFormatException nfe) {
            return "Error." + nfe.getMessage();
        }
        LOG.info("Taking random sample of dataset {}. Rows : {} :",name,size);
        try {
            final List<String> records = service.sampleOfDataset(name,size);
            for(String record : records) LOG.info("\t{}",record);
        } catch (DatasetException e) {
            return "Error." + e.getMessage();
        } catch (IOException e) {
            return "Error." + e.getMessage();
        }
        return "DONE";
    }

    @CliCommand(value = "dat_get_stats", help = "Retrieve useful statistics of the imported dataset.")
    public String datasetsStatsCommand(
            @CliOption(key = {"name"}, mandatory = true, help = "Dataset name.")
            final String name) {
        LOG.info("Column stats for dataset {} : ",name);
        return "NOT IMPLEMENTED";
    }


    @CliCommand(value = "dat_describe", help = "Get Aro schema of the imported dataset.")
    public String datasetsDescribeCommand(
            @CliOption(key = {"name"}, mandatory = true, help = "Dataset name.")
            final String name) {
        LOG.info("Schema description for dataset {} : ",name);
        try{
            Map<String,String> desc = service.describeDataset(name);
            for(Map.Entry<String,String> e : desc.entrySet()) {
                LOG.info("{} : {}",e.getKey(),e.getValue());
            }
        } catch (DatasetException e) {
            return "Error." + e.getMessage();
        } catch (IOException e) {
            return "Error." + e.getMessage();
        }
        return "DONE";
    }
}
