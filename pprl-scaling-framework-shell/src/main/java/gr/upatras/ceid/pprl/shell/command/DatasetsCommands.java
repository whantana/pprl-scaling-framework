package gr.upatras.ceid.pprl.shell.command;

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


    @CliCommand(value = "datasets import", help = "Import a local avro file and schema on the PPRL site.")
    public String datasetsImportCommand(
            @CliOption(key = {"avro_file"}, mandatory = true, help = "Local data avro file.")
            final String avroPath,
            @CliOption(key = {"schema_file"}, mandatory = true, help = "Local schema avro file.")
            final String schemaFilePath,
            @CliOption(key = {"name"}, mandatory = false, help = "(Optional) name of dataset.")
            final String name) {

        LOG.info("Importing local AVRO dataset.");
        final File avroFile = new File(avroPath);
        if (!avroFile.exists()) return "Error. Path \"" + avroPath + "\" does not exist.";
        LOG.info("Selected data file for import \"" + avroPath + "\".");
        final File schemaFile = new File(schemaFilePath);
        if (!schemaFile.exists()) return "Error. Path \"" + schemaFilePath + "\" does not exist.";
        LOG.info("Selected schema file for import \"" + schemaFilePath + "\".");
        final String datasetName = (name!=null) ? name :
                avroFile.getName().substring(0, avroFile.getName().lastIndexOf('.'));
        LOG.info("Imported Dataset name \"" + datasetName + "\"");
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
            @CliOption(key = {"name"}, mandatory = false, help = "(Optional) name of dataset.")
            final String name) {
        LOG.info("Importing local DBLP(XML) dataset.");
        final File dblpFile = new File(dblpPath);
        if (!dblpFile.exists()) return "Error. Path \"" + dblpPath + "\" does not exist.";
        LOG.info("Selected data file for import \"" + dblpPath + "\".");
        final File schemaFile = new File(schemaFilePath);
        if (!schemaFile.exists()) return "Error. Path \"" + schemaFilePath + "\" does not exist.";
        LOG.info("Selected schema file for import \"" + schemaFilePath + "\".");
        final String datasetName = (name!=null) ? name :
                dblpFile.getName().substring(0, dblpFile.getName().lastIndexOf('.'));
        LOG.info("Imported Dataset with name \"" + datasetName + "\"");
        try {
            service.importDblpXmlDataset(dblpFile,schemaFile,datasetName);
        } catch (Exception e) {
            return "Error." + e.getMessage();
        }
        return "DONE";
    }

    @CliCommand(value = "datasets list", help = "List user's imported datasets on the PPRL site.")
    public String datasetsListCommand() {
        LOG.info("Listing datasets");
        return "DONE";
    }

    @CliCommand(value = "datasets drop", help = "Drop user's imported datasets on the PPRL site.")
    public String datasetsDropCommand(
            @CliOption(key = {"name"}, mandatory = true, help = "Name of the dataset to be dropped.")
            final String name,
            @CliOption(key = {"delete_files"}, mandatory = false, help = "YES or NO (default) to completelly drop dataset directory.",
                    specifiedDefaultValue="NO")
            final String deleteFilesStr) {
        if(!deleteFilesStr.equals("YES") && !deleteFilesStr.equals("NO")) return "Error. Please provide \"YES\" or \"NO\".";
        boolean deleteFiles = deleteFilesStr.equals("YES");
        LOG.info("Droping dataset with name \"" + name + "\" (DELETE FILES AS WELL ?" + deleteFilesStr +").");
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
        return "DONE";
    }

    @CliCommand(value = "datasets sample", help = "Sample a user's imported dataset.")
    public String datasetsSampleCommand(
            @CliOption(key = {"name"}, mandatory = true, help = "Sample rows from an imported dataset")
            final String name,
            @CliOption(key = {"count"}, mandatory = false, help = "Rows count (default : 10).",
                    specifiedDefaultValue="10")
            final String countString) {
        int count = 10;
        try{
            count = Integer.parseInt(countString);
        } catch (NumberFormatException nfe) {
            return "Error." + nfe.getMessage();
        }
        LOG.info("Taking random sample of dataset \"" + name  +"\" . Rows : " + count + ".");
        return "DONE";
    }
}
