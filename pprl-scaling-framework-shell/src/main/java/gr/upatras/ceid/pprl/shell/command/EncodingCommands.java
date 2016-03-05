package gr.upatras.ceid.pprl.shell.command;

import gr.upatras.ceid.pprl.datasets.DatasetStatistics;
import gr.upatras.ceid.pprl.datasets.service.DatasetsService;
import gr.upatras.ceid.pprl.datasets.service.LocalDatasetsService;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncoding;
import gr.upatras.ceid.pprl.encoding.service.EncodingService;
import gr.upatras.ceid.pprl.encoding.service.LocalEncodingService;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliAvailabilityIndicator;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

@Component
public class EncodingCommands implements CommandMarker {

    private static Logger LOG = LoggerFactory.getLogger(EncodingCommands.class);

    @Autowired(required = false)
    @Qualifier("encodingService")
    private EncodingService es;

    @Autowired(required = false)
    @Qualifier("localEncodingService")
    private LocalEncodingService elds;

    @Autowired(required = false)
    @Qualifier("datasetsService")
    private DatasetsService ds;

    @Autowired(required = false)
    @Qualifier("localDatasetsService")
    private LocalDatasetsService lds;

    private List<String> ENCODING_SCHEMES = BloomFilterEncoding.SCHEME_NAMES;

    @CliAvailabilityIndicator(value = {"encode_supported_schemes","encode_calculate_encoding_sizes"})
    public boolean availability0() { return true; }
    @CliAvailabilityIndicator(value = {"encode_local_data"})
    public boolean availability1() { return elds != null && lds != null; }


    @CliCommand(value = "encode_supported_schemes", help = "List system's supported Bloom-filter encoding schemes.")
    public String command0() {
        LOG.info("Supported bloom filter encoding schemes : ");
        int i = 1;
        for(String methodName: ENCODING_SCHEMES) {
            LOG.info("\t{}. {}",i,methodName);
            i++;
        }
        return "DONE";
    }

    @CliCommand(value = "encode_calculate_encoding_sizes", help = "Calculate FBF & RBF sizes for given data statistics.")
    public String command1(
            @CliOption(key = {"stats"}, mandatory = true, help = "Path to property file containing the required data statistics")
            final String pathStr,
            @CliOption(key = {"Q"}, mandatory = false, help = "Q for q-grams. Limited to Q={2,3,4}. Default is 2.")
            final String qStr,
            @CliOption(key = {"K"}, mandatory = false, help = "K for number of hash functions.Default is 15.")
            final String kStr
    ) {
        try {
            final Path statsPath = CommandUtils.retrievePath(pathStr);
            LOG.info("Calculating encoding sizes :");
            LOG.info("\tSelected stats file : {}", statsPath);
            final int Q = CommandUtils.retrieveInt(qStr,2);
            if(Q < 2 || Q > 4) throw new IllegalArgumentException("Q is limited to {2,3,4}.");
            final int K = CommandUtils.retrieveInt(kStr,15);
            if(K < 1) throw new IllegalArgumentException("K must be at least 1.");
            DatasetStatistics statistics = lds.localLoadStatsProperties(statsPath);
            LOG.info(CommandUtils.prettyBFEStats(statistics.getFieldStatistics(),K,Q));
            return "DONE";
        } catch (Exception e) {
            return "Error. " + e.getClass().getSimpleName() + " : " + e.getMessage();
        }
    }

    @CliCommand(value = "encode_local_data", help = "Encode local data.")
    public String command2(
            @CliOption(key = {"avro"}, mandatory = true, help = "Local data avro files (comma separated) or including directory.")
            final String avroStr,
            @CliOption(key = {"schema"}, mandatory = true, help = "Local schema avro file.")
            final String schemaStr,
            @CliOption(key = {"name"}, mandatory = true, help = "Name of encoding.")
            final String name,
            @CliOption(key = {"fields"}, mandatory = true, help = "Selected fields to be encoded")
            final String fieldsStr,
            @CliOption(key= {"scheme"}, mandatory = true, help = "One of the following encoding schemes : {FBF,RBF,CLK}.")
            final String scheme,
            @CliOption(key = {"include"}, mandatory = false, help = "(Optional) Fields to be included")
            final String includeStr,
            @CliOption(key= {"fbfN"}, mandatory = false, help = "(Optional) Size of FBFs used in encoding." +
                    "Scheme set must be FBF or RBF. By not providing this FBFs produced will be dynamicaly sized based" +
                    " on dataset statistics.")
            final String fbfNstr,
            @CliOption(key= {"N"}, mandatory = false, help = "(Optional) Setting whole row encoding size." +
                    " Scheme set must be RBF or CLK. " +
                    " For RBF schema ,if set bit selection will be uniform from all the FBFs involved, weighted otherwise.")
            final String Nstr,
            @CliOption(key= {"K"}, mandatory = false, help = "(Optional) Hash function count. Default value is 15.")
            final String Kstr,
            @CliOption(key= {"Q"}, mandatory = false, help = "(Optional) Q for Q-Grams. Limited to {2,3,4} .Default value is 2.")
            final String Qstr,
            @CliOption(key = {"stats"}, mandatory = true, help = "Path to property file containing the required data statistics")
            final String pathStr
    ) {
        try {
            if(!ENCODING_SCHEMES.contains(scheme)) throw new IllegalArgumentException("Scheme " + scheme + " does not exist");

            final Path schemaPath = CommandUtils.retrievePath(schemaStr);
            final Path[] avroPaths = CommandUtils.retrievePaths(avroStr);
            final String[] fields = CommandUtils.retrieveFields(fieldsStr);
            final String[] included = CommandUtils.retrieveFields(includeStr);
            LOG.info("Encoding local data :");
            LOG.info("\tEncoding name : {}", name);
            LOG.info("\tSelected data files : {}", Arrays.toString(avroPaths));
            LOG.info("\tSelected schema file : {}", schemaPath);
            LOG.info("\tSelected fields to be encoded : {}", Arrays.toString(fields));
            if(included.length !=0)
                LOG.info("\tSelected fields to included   : {}", Arrays.toString(included));


            final int fbfN = CommandUtils.retrieveInt(fbfNstr,-1);
            final int N = CommandUtils.retrieveInt(Nstr,-1);
            final int K = CommandUtils.retrieveInt(Kstr,15);
            final int Q = CommandUtils.retrieveInt(Qstr,2);
            final boolean clk = scheme.equals("CLK");
            if(clk && N < 0) throw new IllegalArgumentException("CLK Encoding requires N to be set.");
            final boolean fbfStatic = scheme.equals("FBF") && (fbfN > 0);
            final boolean fbfDynamic = scheme.equals("FBF") && (fbfN < 0);
            final boolean rbfUniformFbfStatic = scheme.equals("RBF") && N > 0 && (fbfN > 0);
            final boolean rbfUniformFbfDynamic = scheme.equals("RBF") && N > 0 && (fbfN < 0);
            final boolean rbfWeighetdFbfStatic = scheme.equals("RBF") && N < 0 && (fbfN > 0);
            final boolean rbfWeighetdFbfDynamic = scheme.equals("RBF") && N < 0 && (fbfN < 0);

            final boolean requiresStatistics = fbfDynamic || rbfUniformFbfDynamic || rbfWeighetdFbfDynamic
                    || rbfUniformFbfStatic;
            final Path statsPath = CommandUtils.retrievePath(pathStr);
            if(requiresStatistics && statsPath == null)
                throw new IllegalArgumentException("Encoding schema requires statistics");
            if(statsPath !=null)
                LOG.info("\tSelected stats file : {}", statsPath);
            if(clk) {
                LOG.info("\tScheme : CLK");
                LOG.info("\tBloom-Filter size : {}",N);
            } else if (fbfStatic) {
                LOG.info("\tScheme : FBF/Static");
                LOG.info("\tField Bloom-Filter size : {}",fbfN);
            } else if (fbfDynamic) {
                LOG.info("\tScheme : FBF/Dynamic");
                LOG.info("\tField Bloom-Filter size : 0 FIXME");
            } else if (rbfUniformFbfStatic) {
                LOG.info("\tScheme : RBF/Uniform/Static");
                LOG.info("\tField Bloom-Filter size : {}",fbfN);
                LOG.info("\tRow Bloom-Filter size : {}", N);
            } else if (rbfUniformFbfDynamic) {
                LOG.info("\tScheme : RBF/Uniform/Dynamic");
                LOG.info("\tField Bloom-Filter size : 0 FIXME");
                LOG.info("\tRow Bloom-Filter size : {}", N);
            } else if (rbfWeighetdFbfStatic) {
                LOG.info("\tScheme : RBF/Weighted/Static");
                LOG.info("\tField Bloom-Filter size : {}",fbfN);
                LOG.info("\tRow Bloom-Filter size : 0 FIXME");
            } else if (rbfWeighetdFbfDynamic) {
                LOG.info("\tScheme : RBF/Weighted/Dynamic");
                LOG.info("\tField Bloom-Filter size : 0 FIXME");
                LOG.info("\tRow Bloom-Filter size : 0 FIXME");
            }
            LOG.info("\tNumber of Hash functions  (K) : {}", K);
            LOG.info("\tHashing Q-Grams (Q) : {}", Q);

            return "DONE";
        } catch (Exception e) {
            return "Error. " + e.getClass().getSimpleName() + " : " + e.getMessage();
        }
    }





//
//    @CliCommand(value = "enc_list", help = "List user encodings on the PPRL site.")
//    public String encodingListCommand(
//            @CliOption(key = {"name"}, mandatory = false, help = "(Optional) Source dataset name.")
//            final String name,
//            @CliOption(key = {"method"}, mandatory = false, help = "(Optional) Bloom-Filter encoding method name.")
//            final String method) {
//        try {
//            List<String> encodedDatasetsStrings =
//                    (name == null && method == null) ? service.listDatasets(false) :
//                            service.listDatasets(name, method);
//
//            if (encodedDatasetsStrings.isEmpty()) {
//                LOG.info("\tFound no encoded datasets" +
//                        ((name != null || method != null) ? " matching your criteria." : "."));
//                return "DONE";
//            }
//
//            int i = 1;
//            LOG.info("Listing user encodings " +
//                    ((name != null) ? String.format("(name=%s)", name) : "") +
//                    ((method != null) ? String.format("(method=%s)", method) : "") + ":");
//            for (String s : encodedDatasetsStrings) {
//                LOG.info("\t{}) {}", i++, s);
//            }
//            return "DONE";
//        } catch (EncodedDatasetException e) {
//            return "Error. " + e.getClass().getSimpleName() + " : " + e.getMessage();
//        } catch (BloomFilterEncodingException e) {
//            return "Error. " + e.getClass().getSimpleName() + " : " + e.getMessage();
//        }
//    }
//

//
//    @CliCommand(value = "enc_import", help = "Import local avro file(s) and schema as an encoded dataset on the PPRL site.")
//    public String encodingImportCommand(
//            @CliOption(key = {"avro_files"}, mandatory = true, help = "Local data avro files (comma separated).")
//            final String avroPaths,
//            @CliOption(key = {"schema_file"}, mandatory = true, help = "Local schema avro file.")
//            final String schemaFilePath,
//            @CliOption(key = {"name"}, mandatory = false, help = "(Optional) Encoded dataset name.")
//            final String name,
//            @CliOption(key = {"dataset"}, mandatory = false, help = "(Optional) Imported source dataset name " +
//                    "this encoding was generated.")
//            final String datasetName,
//            @CliOption(key= {"method"}, mandatory = true, help = "One of the following encoding methods : {FBF,RBF}.")
//            final String methodName){
//        try {
//            final File schemaFile = new File(schemaFilePath);
//            if (!schemaFile.exists()) return "Error. Path \"" + schemaFilePath + "\" does not exist.";
//
//            final File[] avroFiles = CommandUtils.retrieveFiles(avroPaths);
//
//            final String[] absolutePaths = new String[avroFiles.length];
//            for (int i = 0; i < avroFiles.length; i++) absolutePaths[i] = avroFiles[i].getAbsolutePath();
//
//            if (datasetName != null && !datasetName.matches("^[a-z_A-Z][a-z_A-Z0-9]*$"))
//                return "Error. Source Dataset name must contain only alphanumeric characters and underscores.";
//
//            if(ENCODING_SCHEMES == null)
//                ENCODING_SCHEMES = service.listSupportedEncodingMethodsNames();
//            if (!ENCODING_SCHEMES.contains(methodName))
//                return "Error. Method name " + methodName + " is not supported.";
//
//            LOG.info("Importing local AVRO Encoded Dataset :");
//            if(name != null) {
//                if (!name.matches("^[a-z_A-Z][a-z_A-Z0-9]*$"))
//                    return "Error. Encoded Dataset name must contain only alphanumeric characters and underscores.";
//                LOG.info("\tImported Encoded Dataset name           : {}", name);
//            }
//            LOG.info("\tSelected data files for import          : {}", Arrays.toString(absolutePaths));
//            LOG.info("\tSelected schema file for import         : {}", schemaFile.getAbsolutePath());
//            if (datasetName != null) LOG.info("\tSelected source dataset name            : {}", datasetName);
//
//            service.importEncodedDataset(name, datasetName, methodName, schemaFile, avroFiles);
//
//            return "DONE";
//        } catch (IOException e) {
//            return "Error. " + e.getClass().getSimpleName() + " : " + e.getMessage();
//        } catch (IllegalArgumentException e) {
//            return "Error. " + e.getClass().getSimpleName() + " : " + e.getMessage();
//        } catch (BloomFilterEncodingException e) {
//            return "Error. " + e.getClass().getSimpleName() + " : " + e.getMessage();
//        } catch (DatasetException e) {
//            return "Error. " + e.getClass().getSimpleName() + " : " + e.getMessage();
//        }
//    }
//
//    @CliCommand(value = "enc_encode_dataset", help = "Encode an existing dataset on the PPRL site.")
//    public String encodingEncodeDatasetCommand(
//            @CliOption(key = {"name"}, mandatory = false, help = "(Optional) Encoded dataset name.")
//            final String name,
//            @CliOption(key = {"dataset"}, mandatory = true, help = "Source dataset name " +
//                    "to be related with the encoding." +
//                    " Must be alread on pprl site")
//            final String datasetName,
//            @CliOption(key = {"selected_fields"}, mandatory = true, help = "Selected fields to be encoded")
//            final String fieldsStr,
//            @CliOption(key = {"rest_fields"}, mandatory = false, help = "(Optional) Rest of fields to be encoded")
//            final String restFieldsStr,
//            @CliOption(key= {"method"}, mandatory = true, help = "One of the following encoding methods : {FBF,RBF}.")
//            final String methodName,
//            @CliOption(key= {"N"}, mandatory = false, help = "(Optional) If method == FBF not defining N produces dynamic bloom filters." +
//                    " Else if method == RBF not defining N enforces weighted bit selection instead of uniform bit selection.")
//            final String Nstr,
//            @CliOption(key= {"K"}, mandatory = false, help = "(Optional) Hash function count. Default value is 30.")
//            final String Kstr,
//            @CliOption(key= {"Q"}, mandatory = false, help = "(Optional) Q for Q-Grams. Default value is 2.")
//            final String Qstr,
//            @CliOption(key= {"weights"}, mandatory = false, help = "(Optional) If method == RBF and N is not defined, " +
//                    "user can provide the FBF bit selection weights with respect to selected fields.")
//            final String Wstr
//    ) {
//        try {
//            if (!datasetName.matches("^[a-z_A-Z][a-z_A-Z0-9]*$"))
//                return "Error. Source Dataset name must contain only alphanumeric characters and underscores.";
//
//            final String[] selectedFieldNames = CommandUtils.retrieveFields(fieldsStr);
//
//            final String[] restFieldNames;
//            if(restFieldsStr != null) restFieldNames = CommandUtils.retrieveFields(restFieldsStr);
//            else restFieldNames = null;
//
//            if(ENCODING_SCHEMES == null)
//                ENCODING_SCHEMES = service.listSupportedEncodingMethodsNames();
//            if (!ENCODING_SCHEMES.contains(methodName))
//                return "Error. Method name " + methodName + " is not supported.";
//
//            int N = (Nstr == null) ? -1 : Integer.parseInt(Nstr);
//            int K = (Kstr == null) ? 30 : Integer.parseInt(Kstr);
//            int Q = (Qstr == null) ? 2 : Integer.parseInt(Qstr);
//
//            LOG.info("Encoding dataset stored at the PPRL-site");
//            if(name != null)
//                LOG.info("\tEncoded Dataset name                    : {}", name);
//            LOG.info("\tSelected source dataset name            : {}", datasetName);
//            LOG.info("\tSelected source dataset encoded fields  : {}", Arrays.toString(selectedFieldNames));
//            if(restFieldNames != null)
//                LOG.info("\tRest source dataset encoded fields      : {}", Arrays.toString(restFieldNames));
//
//            if(methodName.equals("FBF") && N > 0) {
//                LOG.info("\tSelected encoding method                : FBF, N={}, K={}, Q={}",N, K, Q);
//                service.encodeFBFStaticImportedDataset(name, datasetName, selectedFieldNames, restFieldNames, N, K, Q);
//            } else if (methodName.equals("FBF")) {
//                LOG.info("\tSelected encoding method                : FBF, Dynamic Bloom Filter Sizing, K={}, Q={}",K, Q);
//                service.encodeFBFDynamicImportedDataset(name, datasetName, selectedFieldNames, restFieldNames, K, Q);
//            } else if (methodName.equals("RBF") && N > 0) {
//                LOG.info("\tSelected encoding method                : RBF, N={} (uniform-bit-selection), K={}, Q={}",K, Q);
//                service.encodeRBFUniformImportedDataset(name, datasetName, selectedFieldNames, restFieldNames, N, K, Q);
//            } else {
//                LOG.info("\tSelected encoding method                : RBF, (weighted-bit-selection), K={}, Q={}",K, Q);
//                final double[] weights = CommandUtils.retrieveWeights(Wstr);
//                if(weights != null && weights.length != selectedFieldNames.length) return "Error. weights and selected_fields sizes must agree";
//                service.encodeRBFWeightedImportedDataset(name, datasetName, selectedFieldNames, restFieldNames, weights, K, Q);
//            }
//            return "DONE";
//        } catch (IllegalArgumentException e) {
//            return "Error. " + e.getClass().getSimpleName() + " : " + e.getMessage();
//        } catch (Exception e) {
//            return "Error. " + e.getClass().getSimpleName() + " : " + e.getMessage();
//        }
//    }
//

//
//    @CliCommand(value = "enc_drop", help = "Drop one encoded dataset of the user on the PPRL site.")
//    public String encodingDropCommand(
//            @CliOption(key = {"name"}, mandatory = true, help = "Encoded dataset name.")
//            final String name,
//            @CliOption(key = {"delete_files"}, mandatory = false, help = "(Optional) YES or NO (default) to completelly drop encoded dataset directory.")
//            final String deleteFilesStr) {
//        try {
//            boolean deleteFiles = false;
//            if (deleteFilesStr != null) {
//                if (!deleteFilesStr.equals("YES") && !deleteFilesStr.equals("NO"))
//                    return "Error. Please provide \"YES\" or \"NO\".";
//                deleteFiles = deleteFilesStr.equals("YES");
//            }
//            LOG.info("Droping dataset with name \"{}\" (DELETE FILES AS WELL ? {} ).", name, (deleteFiles ? "YES" : "NO"));
//            service.dropDataset(name, deleteFiles);
//            return "DONE";
//        } catch (DatasetException e) {
//            return "Error. " + e.getClass().getSimpleName() + " : " + e.getMessage();
//        } catch (IOException e) {
//            return "Error. " + e.getClass().getSimpleName() + " : " + e.getMessage();
//        }
//    }
//
//    @CliCommand(value = "enc_drop_all", help = "Drop all encoded dataset of the user on the PPRL site.")
//    public String encodingDropAllCommand(
//            @CliOption(key = {"delete_files"}, mandatory = false, help = "(Optional) YES or NO (default) to completelly drop encoded dataset directory.")
//            final String deleteFilesStr) {
//        try {
//            boolean deleteFiles = false;
//            if (deleteFilesStr != null) {
//                if (!deleteFilesStr.equals("YES") && !deleteFilesStr.equals("NO"))
//                    return "Error. Please provide \"YES\" or \"NO\".";
//                deleteFiles = deleteFilesStr.equals("YES");
//            }
//            LOG.info("Droping all datasets (DELETE FILES AS WELL ? {} ).", (deleteFiles ? "YES" : "NO"));
//            final List<String> names = service.listDatasets(true);
//            for (String name : names) {
//                LOG.info("Droping \"{}\".", name);
//                service.dropDataset(name, deleteFiles);
//            }
//            return "DONE";
//        } catch (DatasetException e) {
//            return "Error. " + e.getClass().getSimpleName() + " : " + e.getMessage();
//        } catch (IOException e) {
//            return "Error. " + e.getClass().getSimpleName() + " : " + e.getMessage();
//        }
//    }
//
//    @CliCommand(value = "enc_sample", help = "Sample a encoded dataset.")
//    public String encodingSampleCommand(
//            @CliOption(key = {"name"}, mandatory = true, help = "Encoded Dataset name.")
//            final String name,
//            @CliOption(key = {"size"}, mandatory = false, help = "(Optional) Sample size (default : 10).")
//            final String sizeStr,
//            @CliOption(key = {"sampleName"}, mandatory = false, help = "(Optional) If provided sample is saved at current working directory")
//            final String sampleName) {
//        try {
//            int size = (sizeStr != null) ? Integer.parseInt(sizeStr) : 10;
//            if (size < 1) throw new NumberFormatException("Sample size must be greater than zero.");
//            final List<String> records;
//            if (sampleName != null) {
//                records = service.saveSampleOfDataset(name, size, sampleName);
//                File[] files = new File[2];
//                files[0] = new File(sampleName + ".avsc");
//                if(files[0].exists()) LOG.info("Schema saved at : {} .", files[0].getAbsolutePath());
//                files[1] = new File(sampleName + ".avro");
//                if(files[1].exists()) LOG.info("Data saved at : {} .", files[1].getAbsolutePath());
//            } else records = service.sampleOfDataset(name, size);
//            LOG.info("Random sample of dataset \"{}\". Sample size : {} :", name, size);
//            for (String record : records) LOG.info("\t{}", record);
//            return "DONE";
//        } catch (IOException e) {
//            return "Error. " + e.getClass().getSimpleName() + " : " + e.getMessage();
//        } catch (DatasetException e) {
//            return "Error. " + e.getClass().getSimpleName() + " : " + e.getMessage();
//        }
//    }
//
//    @CliCommand(value = "enc_describe", help = "Get Aro schema of the encoded dataset.")
//    public String encodingDescribeCommand(
//            @CliOption(key = {"name"}, mandatory = true, help = "Encoded dataset name.")
//            final String name) {
//        try {
//            Map<String, String> desc = service.describeDataset(name);
//            int i = 1;
//            LOG.info("Schema description for dataset \"{}\" : ", name);
//            for (Map.Entry<String, String> e : desc.entrySet()) {
//                LOG.info("\t{}.{} : {}", i++, e.getKey(), e.getValue());
//            }
//            return "DONE";
//        } catch (DatasetException e) {
//            return "Error. " + e.getClass().getSimpleName() + " : " + e.getMessage();
//        } catch (IOException e) {
//            return "Error. " + e.getClass().getSimpleName() + " : " + e.getMessage();
//        }
//    }
//
//    @CliCommand(value = "enc_grant_read_access", help = "Grants read access to an encoded dataset to another PPRL party.")
//    public String encodingGrantReadAccess(
//            @CliOption(key = {"name"}, mandatory = true, help = "Encoded dataset name.")
//            final String name,
//            @CliOption(key = {"partyName"}, mandatory = true, help = "A pprl user/party name.")
//            final String partyName) {
//        return "NOT IMPLEMENTED";
//    }
}
