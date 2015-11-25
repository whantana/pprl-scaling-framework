package gr.upatras.ceid.pprl.shell.command;

import gr.upatras.ceid.pprl.datasets.DatasetException;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncodingException;
import gr.upatras.ceid.pprl.encoding.EncodedDatasetException;
import gr.upatras.ceid.pprl.encoding.service.EncodingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

@Component
public class EncodingCommands implements CommandMarker {

    private static Logger LOG = LoggerFactory.getLogger(EncodingCommands.class);

    @Autowired
    private EncodingService service;

    private Set<String> AVAILABLE_ENCODING_METHODS;

    @CliCommand(value = "enc_list", help = "List user encodings on the PPRL site.")
    public String encodingListCommand(
            @CliOption(key = {"name"}, mandatory = false, help = "(Optional) Source dataset name.")
            final String name,
            @CliOption(key = {"method"}, mandatory = false, help = "(Optional) Bloom-Filter encoding method name.")
            final String method) {
        try {
            List<String> encodedDatasetsStrings =
                    (name == null && method == null) ? service.listDatasets(false) :
                            service.listDatasets(name, method);

            if (encodedDatasetsStrings.isEmpty()) {
                LOG.info("Found no encoded datasets" +
                        ((name != null || method != null) ? " matching your criteria." : "."));
                return "DONE";
            }

            int i = 1;
            LOG.info("Listing user encodings " +
                    ((name != null) ? String.format("(name=%s)", name) : "") +
                    ((method != null) ? String.format("(method=%s)", method) : "") + ":");
            for (String s : encodedDatasetsStrings) {
                LOG.info("\t{}) {}", i++, s);
            }
            return "DONE";
        } catch (EncodedDatasetException e) {
            return "Error. " + e.getClass().getName() + " : " + e.getMessage();
        } catch (BloomFilterEncodingException e) {
            return "Error. " + e.getClass().getName() + " : " + e.getMessage();
        }
    }

    @CliCommand(value = "enc_list_supported_encodings", help = "List system's supported Bloom-filter encoding methods.")
    public String encodingSupportedCommand() {
        LOG.info("Supported Bloom Filter Encoding Methods : ");
        if(AVAILABLE_ENCODING_METHODS == null)
            AVAILABLE_ENCODING_METHODS = service.listSupportedEncodingMethodsNames();
        int i = 1;
        for(String methodName: AVAILABLE_ENCODING_METHODS) {
            LOG.info("\t{}. {}",i,methodName);
            i++;
        }
        return "DONE";
    }

    @CliCommand(value = "enc_import", help = "Import local avro file(s) and schema as an encoded dataset on the PPRL site.")
    public String encodingImportCommand(
            @CliOption(key = {"avro_files"}, mandatory = true, help = "Local data avro files (comma separated).")
            final String avroPaths,
            @CliOption(key = {"schema_file"}, mandatory = true, help = "Local schema avro file.")
            final String schemaFilePath,
            @CliOption(key = {"name"}, mandatory = false, help = "(Optional) Encoded dataset name.")
            final String name,
            @CliOption(key = {"dataset"}, mandatory = false, help = "(Optional) Imported source dataset name " +
                    "this encoding was generated.")
            final String datasetName,
            @CliOption(key = {"columns"}, mandatory = false, help = "(Optional) If source dataset is provided" +
                    " must also provide columns that were encoded.")
            final String columnsStr,
            @CliOption(key= {"method"}, mandatory = true, help = "One of the following encoding methods : {SIMPLE,ROW,MULTI}.")
            final String methodName,
            @CliOption(key= {"N"}, mandatory = false, help = "(Optional) Length of bloom filter. Default value is 1024.")
            final String Nstr,
            @CliOption(key= {"K"}, mandatory = false, help = "(Optional) Hash function count. Default value is 30.")
            final String Kstr,
            @CliOption(key= {"Q"}, mandatory = false, help = "(Optional) Q for Q-Grams. Default value is 2.")
            final String Qstr) {
        try {
            final File schemaFile = new File(schemaFilePath);
            if (!schemaFile.exists()) return "Error. Path \"" + schemaFilePath + "\" does not exist.";

            final File[] avroFiles = CommandUtils.retrieveFiles(avroPaths);

            final String[] absolutePaths = new String[avroFiles.length];
            for (int i = 0; i < avroFiles.length; i++) absolutePaths[i] = avroFiles[i].getAbsolutePath();

            if (datasetName != null && !datasetName.matches("^[a-z_A-Z][a-z_A-Z0-9]*$"))
                return "Error. Source Dataset name must contain only alphanumeric characters and underscores.";

            if (datasetName != null && columnsStr == null) return "Error. Provide columns along side source dataset";
            if (datasetName == null && columnsStr != null) return "Error. Provide columns along side source dataset";

            final String[] columns = CommandUtils.retrieveColumns(columnsStr);

            if(AVAILABLE_ENCODING_METHODS == null)
                AVAILABLE_ENCODING_METHODS = service.listSupportedEncodingMethodsNames();
            if (!AVAILABLE_ENCODING_METHODS.contains(methodName))
                return "Error. Method name " + methodName + " is not supported.";

            int N = (Nstr == null) ? 1024 : Integer.parseInt(Nstr);
            int K = (Kstr == null) ? 30 : Integer.parseInt(Kstr);
            int Q = (Qstr == null) ? 2 : Integer.parseInt(Qstr);

            final String finalName = CommandUtils.retrieveName(name, methodName, N, K, Q,columns);

            LOG.info("Importing local AVRO Encoded Dataset :");
            LOG.info("\tImported Encoded Dataset name           : {}", finalName);
            LOG.info("\tSelected data files for import          : {}", Arrays.toString(absolutePaths));
            LOG.info("\tSelected schema file for import         : {}", schemaFile.getAbsolutePath());
            if (datasetName != null) LOG.info("\tSelected source dataset name            : {}", datasetName);
            if (columns != null) LOG.info("\tSelected source dataset encoded columns : {}", Arrays.toString(columns));
            LOG.info("\tSelected encoding method                : {}, N={}, K={}, Q={}", methodName, N, K, Q);

            if(datasetName == null || columns == null)
                service.importEncodedDatasets(finalName,
                        methodName, N, K, Q, schemaFile, avroFiles);
            else
                service.importEncodedDatasets(finalName, datasetName, Arrays.asList(columns),
                        methodName, N, K, Q, schemaFile, avroFiles);
            return "DONE";
        } catch (IOException e) {
            return "Error. " + e.getClass().getName() + " : " + e.getMessage();
        } catch (IllegalArgumentException e) {
            return "Error. " + e.getClass().getName() + " : " + e.getMessage();
        } catch (BloomFilterEncodingException e) {
            return "Error. " + e.getClass().getName() + " : " + e.getMessage();
        } catch (DatasetException e) {
            return "Error. " + e.getClass().getName() + " : " + e.getMessage();
        }
    }

    @CliCommand(value = "enc_encode_dataset", help = "Encode an existing dataset on the PPRL site.")
    public String encodingEncodeDatasetCommand(
            @CliOption(key = {"name"}, mandatory = false, help = "(Optional) Encoded dataset name.")
            final String name,
            @CliOption(key = {"dataset"}, mandatory = true, help = "Source dataset name " +
                    "to be related with the encoding." +
                    " Must be alread on pprl site")
            final String datasetName,
            @CliOption(key = {"columns"}, mandatory = true, help = "Columns to be encoded")
            final String columnsStr,
            @CliOption(key= {"method"}, mandatory = true, help = "One of the following encoding methods : {SIMPLE,ROW,MULTI}.")
            final String methodName,
            @CliOption(key= {"N"}, mandatory = false, help = "(Optional) Length of bloom filter. Default value is 1024.")
            final String Nstr,
            @CliOption(key= {"K"}, mandatory = false, help = "(Optional) Hash function count. Default value is 30.")
            final String Kstr,
            @CliOption(key= {"Q"}, mandatory = false, help = "(Optional) Q for Q-Grams. Default value is 2.")
            final String Qstr) {
        try {
            if (!datasetName.matches("^[a-z_A-Z][a-z_A-Z0-9]*$"))
                return "Error. Source Dataset name must contain only alphanumeric characters and underscores.";

            final String[] columns = CommandUtils.retrieveColumns(columnsStr);

            if(AVAILABLE_ENCODING_METHODS == null)
                AVAILABLE_ENCODING_METHODS = service.listSupportedEncodingMethodsNames();
            if (!AVAILABLE_ENCODING_METHODS.contains(methodName))
                return "Error. Method name " + methodName + " is not supported.";

            int N = (Nstr == null) ? 1024 : Integer.parseInt(Nstr);
            int K = (Kstr == null) ? 30 : Integer.parseInt(Kstr);
            int Q = (Qstr == null) ? 2 : Integer.parseInt(Qstr);

            final String finalName = CommandUtils.retrieveName(name, methodName, N, K, Q,columns);

            LOG.info("Encoding dataset stored at the PPRL-site");
            LOG.info("\tEncoded Dataset name                    : {}", finalName);
            LOG.info("\tSelected source dataset name            : {}", datasetName);
            LOG.info("\tSelected source dataset encoded columns : {}", Arrays.toString(columns));
            LOG.info("\tSelected encoding method                : {}, N={}, K={}, Q={}", methodName, N, K, Q);

            service.encodeImportedDataset(finalName,datasetName,Arrays.asList(columns),methodName,N,K,Q);
            return "DONE";
        } catch (IllegalArgumentException e) {
            return "Error. " + e.getClass().getName() + " : " + e.getMessage();
        } catch (Exception e) {
            return "Error. " + e.getClass().getName() + " : " + e.getMessage();
        }
    }

    @CliCommand(value = "enc_encode_local_avro", help = "Encode local avro files.")
    public String encodingEncodeLocalCommand(
            @CliOption(key = {"avro_files"}, mandatory = true, help = "Local data avro files (comma separated).")
            final String avroPaths,
            @CliOption(key = {"schema_file"}, mandatory = true, help = "Local schema avro file.")
            final String schemaFilePath,
            @CliOption(key = {"columns"}, mandatory = true, help = "Columns to be encoded")
            final String columnsStr,
            @CliOption(key = {"name"}, mandatory = false, help = "(Optional) Encoded dataset name.")
            final String name,
            @CliOption(key= {"method"}, mandatory = true, help = "One of the following encoding methods : {SIMPLE,ROW,MULTI}.")
            final String methodName,
            @CliOption(key= {"N"}, mandatory = false, help = "(Optional) Length of bloom filter. Default value is 1024.")
            final String Nstr,
            @CliOption(key= {"K"}, mandatory = false, help = "(Optional) Hash function count. Default value is 30.")
            final String Kstr,
            @CliOption(key= {"Q"}, mandatory = false, help = "(Optional) Q for Q-Grams. Default value is 2.")
            final String Qstr) {
        try {
            final File schemaFile = new File(schemaFilePath);
            if (!schemaFile.exists()) return "Error. Path \"" + schemaFilePath + "\" does not exist.";

            final File[] avroFiles = CommandUtils.retrieveFiles(avroPaths);

            final String[] absolutePaths = new String[avroFiles.length];
            for (int i = 0; i < avroFiles.length; i++) absolutePaths[i] = avroFiles[i].getAbsolutePath();

            final String[] columns = CommandUtils.retrieveColumns(columnsStr);

            if(AVAILABLE_ENCODING_METHODS == null)
                AVAILABLE_ENCODING_METHODS = service.listSupportedEncodingMethodsNames();
            if (!AVAILABLE_ENCODING_METHODS.contains(methodName))
                return "Error. Method name " + methodName + " is not supported.";

            int N = (Nstr == null) ? 1024 : Integer.parseInt(Nstr);
            int K = (Kstr == null) ? 30 : Integer.parseInt(Kstr);
            int Q = (Qstr == null) ? 2 : Integer.parseInt(Qstr);

            final String finalName = CommandUtils.retrieveName(name, methodName, N, K, Q,columns);

            LOG.info("Encoding local avro files");
            LOG.info("\tEncoded Dataset name                      : {}", finalName);
            LOG.info("\tSelected local data files for encoding    : {}", Arrays.toString(absolutePaths));
            LOG.info("\tSelected local schema file for encoding   : {}", schemaFile.getAbsolutePath());
            LOG.info("\tSelected local data columns to be encoded : {}", Arrays.toString(columns));
            LOG.info("\tSelected encoding method                  : {}, N={}, K={}, Q={}", methodName, N, K, Q);

            final Set<File> avroFilesSet = new TreeSet<File>(Arrays.asList(avroFiles));
            final File encodedFile = new File(finalName + ".avro");
            final File encodedSchemaFile = new File(finalName + ".avsc");
            service.encodeLocalFile(Arrays.asList(columns),
                    methodName, N, K, Q,
                    avroFilesSet,schemaFile,
                    encodedFile,encodedSchemaFile);

            LOG.info("Encoding local avro files");
            LOG.info("\tEncoded schema file saved at : {}",encodedSchemaFile.getAbsolutePath());
            LOG.info("\tEncoded avro file saved at   : {}",encodedFile.getAbsolutePath());
            return "DONE";
        } catch (IOException e) {
            return "Error. " + e.getClass().getName() + " : " + e.getMessage();
        } catch (BloomFilterEncodingException e) {
            return "Error. " + e.getClass().getName() + " : " + e.getMessage();
        }
    }

    @CliCommand(value = "enc_drop", help = "Drop one encoded dataset of the user on the PPRL site.")
    public String encodingDropCommand(
            @CliOption(key = {"name"}, mandatory = true, help = "Encoded dataset name.")
            final String name,
            @CliOption(key = {"delete_files"}, mandatory = false, help = "(Optional) YES or NO (default) to completelly drop encoded dataset directory.")
            final String deleteFilesStr) {
        try {
            boolean deleteFiles = false;
            if (deleteFilesStr != null) {
                if (!deleteFilesStr.equals("YES") && !deleteFilesStr.equals("NO"))
                    return "Error. Please provide \"YES\" or \"NO\".";
                deleteFiles = deleteFilesStr.equals("YES");
            }
            LOG.info("Droping dataset with name \"{}\" (DELETE FILES AS WELL ? {} ).", name, (deleteFiles ? "YES" : "NO"));
            service.dropDataset(name, deleteFiles);
            return "DONE";
        } catch (DatasetException e) {
            return "Error. " + e.getClass().getName() + " : " + e.getMessage();
        } catch (IOException e) {
            return "Error. " + e.getClass().getName() + " : " + e.getMessage();
        }
    }

    @CliCommand(value = "enc_drop_all", help = "Drop all encoded dataset of the user on the PPRL site.")
    public String encodingDropAllCommand(
            @CliOption(key = {"delete_files"}, mandatory = false, help = "(Optional) YES or NO (default) to completelly drop encoded dataset directory.")
            final String deleteFilesStr) {
        try {
            boolean deleteFiles = false;
            if (deleteFilesStr != null) {
                if (!deleteFilesStr.equals("YES") && !deleteFilesStr.equals("NO"))
                    return "Error. Please provide \"YES\" or \"NO\".";
                deleteFiles = deleteFilesStr.equals("YES");
            }
            LOG.info("Droping all datasets (DELETE FILES AS WELL ? {} ).", (deleteFiles ? "YES" : "NO"));
            final List<String> names = service.listDatasets(true);
            for (String name : names) {
                LOG.info("Droping \"{}\".", name);
                service.dropDataset(name, deleteFiles);
            }
            return "DONE";
        } catch (DatasetException e) {
            return "Error. " + e.getClass().getName() + " : " + e.getMessage();
        } catch (IOException e) {
            return "Error. " + e.getClass().getName() + " : " + e.getMessage();
        }
    }

    @CliCommand(value = "enc_sample", help = "Sample a encoded dataset.")
    public String encodingSampleCommand(
            @CliOption(key = {"name"}, mandatory = true, help = "Encoded Dataset name.")
            final String name,
            @CliOption(key = {"size"}, mandatory = false, help = "(Optional) Sample size (default : 10).")
            final String sizeStr,
            @CliOption(key = {"sampleName"}, mandatory = false, help = "(Optional) If provided sample is saved at current working directory")
            final String sampleName) {
        try {
            int size = (sizeStr != null) ? Integer.parseInt(sizeStr) : 10;
            if (size < 1) throw new NumberFormatException("Sample size must be greater than zero.");
            final List<String> records;
            if (sampleName != null) {
                final File sampleSchemaFile = new File(sampleName + ".avsc");
                sampleSchemaFile.createNewFile();
                final File sampleDataFile = new File(sampleName + ".avro");
                sampleDataFile.createNewFile();
                records = service.saveSampleOfDataset(name, size, sampleSchemaFile, sampleDataFile);
                LOG.info("Schema saved at : {} .", sampleSchemaFile.getAbsolutePath());
                LOG.info("Data saved at   : {} .", sampleDataFile.getAbsolutePath());
            } else records = service.sampleOfDataset(name, size);
            LOG.info("Random sample of dataset \"{}\". Sample size : {} :", name, size);
            for (String record : records) LOG.info("\t{}", record);
            return "DONE";
        } catch (IOException e) {
            return "Error. " + e.getClass().getName() + " : " + e.getMessage();
        } catch (DatasetException e) {
            return "Error. " + e.getClass().getName() + " : " + e.getMessage();
        }
    }

    @CliCommand(value = "enc_describe", help = "Get Aro schema of the encoded dataset.")
    public String encodingDescribeCommand(
            @CliOption(key = {"name"}, mandatory = true, help = "Encoded dataset name.")
            final String name) {
        try {
            Map<String, String> desc = service.describeDataset(name);
            int i = 1;
            LOG.info("Schema description for dataset \"{}\" : ", name);
            for (Map.Entry<String, String> e : desc.entrySet()) {
                LOG.info("\t{}.{} : {}", i++, e.getKey(), e.getValue());
            }
            return "DONE";
        } catch (DatasetException e) {
            return "Error. " + e.getClass().getName() + " : " + e.getMessage();
        } catch (IOException e) {
            return "Error. " + e.getClass().getName() + " : " + e.getMessage();
        }
    }

    @CliCommand(value = "enc_grant_read_access", help = "Grants read access to an encoded dataset to another PPRL party.")
    public String encodingGrantReadAccess(
            @CliOption(key = {"name"}, mandatory = true, help = "Encoded dataset name.")
            final String name,
            @CliOption(key = {"partyName"}, mandatory = true, help = "A pprl user/party name.")
            final String partyName) {
        return "NOT IMPLEMENTED";
    }
}
