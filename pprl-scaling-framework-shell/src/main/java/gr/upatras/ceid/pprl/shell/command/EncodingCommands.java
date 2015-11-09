package gr.upatras.ceid.pprl.shell.command;

import gr.upatras.ceid.pprl.datasets.service.DatasetsService;
import gr.upatras.ceid.pprl.encoding.service.EncodingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliAvailabilityIndicator;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class EncodingCommands implements CommandMarker {
    private Logger LOG = LoggerFactory.getLogger(EncodingCommands.class);

    @Autowired
    private EncodingService service;


    // Place holders

    //@CliAvailabilityIndicator({"enc_list"})
    //@CliAvailabilityIndicator({"enc_list_supported_encodings"})
    //@CliAvailabilityIndicator({"enc_import"})
    //@CliAvailabilityIndicator({"enc_encode_dataset"})
    //@CliAvailabilityIndicator({"enc_drop"})
    //@CliAvailabilityIndicator({"enc_drop_all"})
    //@CliAvailabilityIndicator({"enc_sample"})
    //@CliAvailabilityIndicator({"enc_describe"})
    //@CliAvailabilityIndicator({"enc_grant_read_access"})

    @CliCommand(value = "enc_list", help = "List the available encodings.")
    public String encodingListCommand(
            @CliOption(key = {"name"}, mandatory = false, help = "(Optional) Dataset name.")
            final String name,
            @CliOption(key = {"method"}, mandatory = false, help = "(Optional) One of the available Bloom-filter encodings.")
            final String method) {
        return "NOT IMPLEMENTED";
    }

    @CliCommand(value = "enc_list_supported_encodings", help = "List system's supported Bloom-filter encodings.")
    public String encodingSupportedCommand() {
        int i = 1;
        for(Map.Entry<String,Class> entry : EncodingService.AVAILABLE_METHODS.entrySet()) {
            LOG.info("{}. {} class: {}",i,entry.getKey(),entry.getValue());
            i++;
        }
        return "DONE";
    }

    @CliCommand(value = "enc_import", help = "Import local avro file(s) and schema as an encoding on the PPRL site.")
    public String encodingImportCommand(
            @CliOption(key = {"avro_file"}, mandatory = false, help = "Local data avro file.")
            final String avroPath,
            @CliOption(key = {"avro_files"}, mandatory = false, help = "Local data avro files (comma separated).")
            final String avroPaths,
            @CliOption(key = {"schema_file"}, mandatory = true, help = "Local schema avro file.")
            final String schemaFilePath,
            @CliOption(key = {"name"}, mandatory = true, help = "Encoding name.")
            final String name,
            @CliOption(key = {"dataset"}, mandatory = false, help = "(Optional) Dataset name to be related with the encoding.")
            final String datasetName) {
        return "NOT IMPLEMENTED";
    }

    @CliCommand(value = "enc_encode_dataset", help = "Encode an existing dataset on the PPRL site.")
    public String encodingEncodeDatasetCommand() {
        return "NOT IMPLEMENTED";
    }
}
