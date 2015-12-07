package gr.upatras.ceid.pprl.shell.command;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;

public class CommandUtils {

    private static Logger LOG = LoggerFactory.getLogger(CommandUtils.class);

    public static File[] retrieveFiles(final String avroPaths) throws IOException {
        File[] avroFiles;
        if(!avroPaths.contains(",")) {
            final File avroFile = new File(avroPaths);
            if (!avroFile.exists())
                throw new IOException("Path \"" + avroPaths + "\" does not exist.");
            if(avroFile.isDirectory()) {
                LOG.info("Avro file input is the directory {}",avroFile);
                avroFiles = avroFile.listFiles(new FilenameFilter() {
                    public boolean accept(File dir, String name) {
                        return name.endsWith(".avro");
                    }
                });
                LOG.info("Found {} avro files in directory {}",avroFiles.length,avroFile);
            } else {
                avroFiles = new File[1];
                avroFiles[0] = avroFile;
            }
        } else {
            String[] paths = avroPaths.split(",");
            avroFiles = new File[paths.length];
            for (int j = 0; j < avroFiles.length; j++) {
                avroFiles[j] = new File(paths[j]);
                if (!avroFiles[j].exists())
                    throw new IOException("Path \"" + avroPaths + "\" does not exist.");
            }
        }
        return avroFiles;
    }

    public static String[] retrieveFields(final String fieldsStr) throws IllegalArgumentException {
        String[] columns = null;
        if (fieldsStr != null && fieldsStr.contains(",")) {
            columns = fieldsStr.split(",");
            for (String c : columns)
                if (!c.matches("^[a-z_A-Z][a-z_A-Z0-9]*$"))
                    throw new IllegalArgumentException("Field names must contain only alphanumeric characters and underscores.");
        } else if (fieldsStr != null) {
            if (!fieldsStr.matches("^[a-z_A-Z][a-z_A-Z0-9]*$"))
                throw new IllegalArgumentException("Field names must contain only alphanumeric characters and underscores.");
            columns = new String[1];
            columns[0] = fieldsStr;
        }
        return columns;
    }
}
