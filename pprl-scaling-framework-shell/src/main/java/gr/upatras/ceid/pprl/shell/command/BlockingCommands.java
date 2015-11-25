package gr.upatras.ceid.pprl.shell.command;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.shell.core.CommandMarker;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;

public class BlockingCommands implements CommandMarker {
    private static Logger LOG = LoggerFactory.getLogger(BlockingCommands.class);
}
