package gr.upatras.ceid.pprl.shell.command;

import java.io.IOException;
import java.util.List;

import gr.upatras.ceid.pprl.datasets.DatasetException;
import gr.upatras.ceid.pprl.datasets.service.DatasetsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliAvailabilityIndicator;
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

    @CliAvailabilityIndicator({"datasets import"})
    public boolean datasetsImportAvailability() {
        return true;
    }

    @CliAvailabilityIndicator({"datasets import_dblp"})
    public boolean datasetsImportDblpAvailability() {
        return true;
    }

    @CliAvailabilityIndicator({"datasets list"})
    public boolean datasetsListAvailability() {
        return true;
    }

    @CliAvailabilityIndicator({"datasets drop"})
    public boolean datasetsDropAvailability() {
        return true;
    }

    @CliAvailabilityIndicator({"datasets drop_all"})
    public boolean datasetsDropAllAvailability() {
        return true;
    }

    @CliAvailabilityIndicator({"datasets sample"})
    public boolean datasetsSampleAvailability() {
        return true;
    }

    @CliAvailabilityIndicator({"datasets get_stats"})
    public boolean datasetsStatsAvailability() {
        return true;
    }

    @CliAvailabilityIndicator({"datasets describe"})
    public boolean datasetsDescribeAvailability() {
        return true;
    }


    @CliCommand(value = "datasets import", help = "Import a local avro file and schema on the PPRL site.")
    public String datasetsImportCommand(
            @CliOption(key = {"avro_file"}, mandatory = true, help = "Local data avro file.")
            final String avroPath,
            @CliOption(key = {"schema_file"}, mandatory = true, help = "Local schema avro file.")
            final String schemaFilePath,
            @CliOption(key = {"name"}, mandatory = false, help = "(Optional) Dataset name.")
            final String name) {


        final File avroFile = new File(avroPath);
        if (!avroFile.exists()) return "Error. Path \"" + avroPath + "\" does not exist.";
        final File schemaFile = new File(schemaFilePath);
        if (!schemaFile.exists()) return "Error. Path \"" + schemaFilePath + "\" does not exist.";
        final String datasetName = (name!=null) ? name :
                avroFile.getName().substring(0, avroFile.getName().lastIndexOf('.'));
        // TODO check name [a-zA-Z0-9]

        LOG.info("Importing local AVRO dataset :");
        LOG.info("\tSelected data file for import   : {}",avroFile.getAbsolutePath());
        LOG.info("\tSelected schema file for import : {}",schemaFile.getAbsolutePath());
        LOG.info("\tImported Dataset name           : {}",datasetName);

        try {
            service.importDataset(avroFile,schemaFile,datasetName);
        } catch (Exception e) {
            return "Error." + e.getMessage();
        }
        return "DONE";
    }

    @CliCommand(value = "datasets import_dblp", help = "Import the dblp.xml file on the PPRL site.")
    public String datasetsImportDblpCommand(
            @CliOption(key = {"dblp_file"}, mandatory = true, help = "Local dblp file (XML format).")
            final String dblpPath,
            @CliOption(key = {"schema_file"}, mandatory = true, help = "Local schema avro file.")
            final String schemaFilePath,
            @CliOption(key = {"name"}, mandatory = false, help = "(Optional) Dataset name.")
            final String name) {

        final File dblpFile = new File(dblpPath);
        if (!dblpFile.exists()) return "Error. Path \"" + dblpPath + "\" does not exist.";
        final File schemaFile = new File(schemaFilePath);
        if (!schemaFile.exists()) return "Error. Path \"" + schemaFilePath + "\" does not exist.";
        final String datasetName = (name!=null) ? name :
                        dblpFile.getName().substring(0, dblpFile.getName().lastIndexOf('.'));

        LOG.info("Importing local DBLP(XML) dataset : ");
        LOG.info("\tSelected data file for import   : {}",dblpFile.getAbsolutePath());
        LOG.info("\tSelected schema file for import : {}",schemaFile.getAbsolutePath());
        LOG.info("\tImported Dataset name           : {}",datasetName);

        try {
            service.importDblpXmlDataset(dblpFile,schemaFile,datasetName);
        } catch (Exception e) {
            return "Error." + e.getMessage();
        }
        return "DONE";
    }

    @CliCommand(value = "datasets list", help = "List user's imported datasets on the PPRL site.")
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

    @CliCommand(value = "datasets drop", help = "Drop user's imported datasets on the PPRL site.")
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

    @CliCommand(value = "datasets drop_all", help = "Drop user's imported datasets on the PPRL site.")
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

    @CliCommand(value = "datasets sample", help = "Sample a user's imported dataset.")
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
            for(String record : records) LOG.info("\t{}",record); // TODO make this pretty
        } catch (DatasetException e) {
            return "Error." + e.getMessage();
        } catch (IOException e) {
            return "Error." + e.getMessage();
        }
        return "DONE";
    }

    @CliCommand(value = "datasets get_stats", help = "Retrieve useful statistics of the imported dataset.")
    public String datasetsStatsCommand(
        @CliOption(key = {"name"}, mandatory = true, help = "Dataset name.")
        final String name) {
        LOG.info("Column stats for dataset {} : ",name);
        return "NOT IMPLEMENTED";
    }


    @CliCommand(value = "datasets describe", help = "Get Aro schema of the imported dataset.")
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
