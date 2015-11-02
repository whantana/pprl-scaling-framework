package gr.upatras.ceid.pprl.shell.command;

import gr.upatras.ceid.pprl.datasets.service.DatasetsService;
import gr.upatras.ceid.pprl.encoding.service.EncodingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.shell.core.CommandMarker;
import org.springframework.stereotype.Component;

@Component
public class EncodingCommands implements CommandMarker {
    private Logger LOG = LoggerFactory.getLogger(EncodingCommands.class);

    @Autowired
    private EncodingService service;

}
