package gr.upatras.ceid.pprl.shell.command;

import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliAvailabilityIndicator;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.stereotype.Component;

@Component
public class DatasetsCommand implements CommandMarker {

    @CliAvailabilityIndicator({"pprl-datasets-hdfs-setup","pprl-datasets-import"})
   	public boolean datasetSetupImportAvailablilty() {
        return true;
   	}

    @CliCommand(value = "pprl-datasets-hdfs-setup", help = "Access dataset commands")
    public String pprlDatasetsHdfsSetup() {
        return "PPRL Dataset Hdfs Setup";
    }

    @CliCommand(value = "pprl-datasets-import", help = "Leave dataset commands")
    public String pprlDatasetsHdfsImport() {
        return "PPRL Dataset Hdfs Import";
    }
}
