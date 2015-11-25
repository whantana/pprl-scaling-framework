package gr.upatras.ceid.pprl.shell.command;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

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

    public static String[] retrieveColumns(final String columnsStr) throws IllegalArgumentException {
        String[] columns = null;
        if (columnsStr != null && columnsStr.contains(",")) {
            columns = columnsStr.split(",");
            for (String c : columns)
                if (!c.matches("^[a-z_A-Z][a-z_A-Z0-9]*$"))
                    throw new IllegalArgumentException("Column names must contain only alphanumeric characters and underscores.");
        } else if (columnsStr != null) {
            if (!columnsStr.matches("^[a-z_A-Z][a-z_A-Z0-9]*$"))
                throw new IllegalArgumentException("Column names must contain only alphanumeric characters and underscores.");
            columns = new String[1];
            columns[0] = columnsStr;
        }
        return columns;
    }

    public static String retrieveName(final String name, final String methodName , int N, int K, int Q,
                                      final String[] columns) {
        if (name == null) {
            String finalName = String.format("enc_%s_%d_%d_%d", methodName, N, K, Q);
            if (columns != null) for (String column : columns) finalName += "_" + column;
            return finalName;
        } else {
            if (!name.matches("^[a-z_A-Z][a-z_A-Z0-9]*$"))
                return "Error. Encoded Dataset name must contain only alphanumeric characters and underscores.";
            return name;
        }
    }
}
