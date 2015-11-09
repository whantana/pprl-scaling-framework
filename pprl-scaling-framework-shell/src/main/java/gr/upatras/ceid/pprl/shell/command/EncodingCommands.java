package gr.upatras.ceid.pprl.shell.command;

import gr.upatras.ceid.pprl.datasets.service.DatasetsService;
import gr.upatras.ceid.pprl.encoding.service.EncodingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliAvailabilityIndicator;
import org.springframework.stereotype.Component;

@Component
public class EncodingCommands implements CommandMarker {
    private Logger LOG = LoggerFactory.getLogger(EncodingCommands.class);

    @Autowired
    private EncodingService service;


    // Place holders

    // @CliAvailabilityIndicator({"enc_list"})
    // @CliAvailabilityIndicator({"enc_list_supported_encodings"})
    // @CliAvailabilityIndicator({"enc_import"})
    //@CliAvailabilityIndicator({"enc_encode_dataset"})
    //@CliAvailabilityIndicator({"enc_drop"})
    //@CliAvailabilityIndicator({"enc_drop_all"})
    //@CliAvailabilityIndicator({"enc_sample"})
    //@CliAvailabilityIndicator({"enc_describe"})
    //@CliAvailabilityIndicator({"enc_grant_read_access"})
}
