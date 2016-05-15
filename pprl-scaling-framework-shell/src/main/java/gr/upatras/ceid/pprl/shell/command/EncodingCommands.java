package gr.upatras.ceid.pprl.shell.command;

import gr.upatras.ceid.pprl.datasets.DatasetStatistics;
import gr.upatras.ceid.pprl.datasets.DatasetsUtil;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncoding;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncodingException;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncodingUtil;
import gr.upatras.ceid.pprl.encoding.FieldBloomFilterEncoding;
import gr.upatras.ceid.pprl.encoding.RowBloomFilterEncoding;
import gr.upatras.ceid.pprl.service.datasets.DatasetsService;
import gr.upatras.ceid.pprl.service.datasets.LocalDatasetsService;
import gr.upatras.ceid.pprl.service.encoding.EncodingService;
import gr.upatras.ceid.pprl.service.encoding.LocalEncodingService;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
public class EncodingCommands implements CommandMarker {

    private static Logger LOG = LoggerFactory.getLogger(EncodingCommands.class);

    @Autowired(required = false)
    @Qualifier("encodingService")
    private EncodingService es;

    @Autowired(required = false)
    @Qualifier("localEncodingService")
    private LocalEncodingService les;

    @Autowired(required = false)
    @Qualifier("datasetsService")
    private DatasetsService ds;

    @Autowired(required = false)
    @Qualifier("localDatasetsService")
    private LocalDatasetsService lds;

    private List<String> ENCODING_SCHEMES = BloomFilterEncodingUtil.SCHEME_NAMES;

    @CliAvailabilityIndicator(value = {"list_supported_encoding_schemes"})
    public boolean availability0() { return true; }
    @CliAvailabilityIndicator(value = {"encode_local_data","encode_local_data_by_schema"})
    public boolean availability1() { return les != null && lds != null; }
    @CliAvailabilityIndicator(value = {"encode_calculate_encoding_sizes"})
    public boolean availability2() { return lds != null; }
    @CliAvailabilityIndicator(value = {"encode_data","encode_data_by_schema"})
    public boolean availability3() { return ds != null && es != null; }

    /**
     *  COMMON ENCODING COMMANDS
     */


    @CliCommand(value = "list_supported_encoding_schemes", help = "List system's supported Bloom-filter encoding schemes.")
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
            @CliOption(key = {"local"}, mandatory = false, help = "(Optional).Either local file or HDFS look up for stats. Default is true")
            final String localStr,
            @CliOption(key = {"Q"}, mandatory = false, help = "Q for q-grams. Limited to Q={2,3,4}. Default is 2.")
            final String qStr,
            @CliOption(key = {"K"}, mandatory = false, help = "K for number of hash functions.Default is 15.")
            final String kStr
    ) {
        try {
            final Path statsPath = CommandUtil.retrievePath(pathStr);
            final boolean localFile = CommandUtil.retrieveBoolean(localStr,true);
            final int Q = CommandUtil.retrieveInt(qStr, 2);
            if(Q < 2 || Q > 4) throw new IllegalArgumentException("Q is limited to {2,3,4}.");
            final int K = CommandUtil.retrieveInt(kStr, 15);
            if(K < 1) throw new IllegalArgumentException("K must be at least 1.");


            LOG.info("Calculating encoding sizes :");
            LOG.info("\tSelected stats file : {}", statsPath);
            LOG.info("\tQ : {}",Q);
            LOG.info("\tK : {}",K);
            LOG.info("\n");

            DatasetStatistics statistics;
            if(localFile) statistics = lds.loadStats(statsPath);
            else {
                if(ds == null) throw new IllegalArgumentException("Cannot look up in the pprl site");
                statistics = ds.loadStats(statsPath);
            }

            LOG.info(DatasetStatistics.prettyBFEStats(statistics.getFieldStatistics(), K, Q));
            LOG.info("\n");

            return "DONE";
        } catch (Exception e) {
            return "Error. " + e.getClass().getSimpleName() + " : " + e.getMessage();
        }
    }


    /**
     *  LOCAL ENCODING COMMANDS
     */


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
            @CliOption(key = {"stats"}, mandatory = true, help = "Path to property file containing the required data statistics")
            final String pathStr,
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
            @CliOption(key = {"partitions"}, mandatory = false, help = "(Optional) Partitions of the output. Default is 1 (No partitioning).")
            final String partitionsStr
    ) {
        try {
            BloomFilterEncodingUtil.schemeNameSupported(scheme);

            final Path schemaPath = CommandUtil.retrievePath(schemaStr);
            final Path[] avroPaths = CommandUtil.retrievePaths(avroStr);
            final String[] fields = CommandUtil.retrieveFields(fieldsStr);
            final String[] included = CommandUtil.retrieveFields(includeStr);
            final int fbfN = CommandUtil.retrieveInt(fbfNstr, -1);
            final int N = CommandUtil.retrieveInt(Nstr, -1);
            final int K = CommandUtil.retrieveInt(Kstr, 15);
            final int Q = CommandUtil.retrieveInt(Qstr, 2);
            final int partitions = CommandUtil.retrieveInt(partitionsStr,1);
            final Path statsPath = CommandUtil.retrievePath(pathStr);
            final double[] avgQgrams = (statsPath == null)  ? null : new double[fields.length];
            final double[] weights = (statsPath == null)  ? null : new double[fields.length];
            if(statsPath != null) {
                DatasetStatistics statistics = lds.loadStats(statsPath);
                if(scheme.equals("RBF") && N < 0 && fields.length != statistics.getFieldCount())
                    throw new IllegalStateException("In the case of weighted RBF all fields in stats must be included. Should recalculate stats");
                int i = 0;
                for (String fieldName : fields) {
                    avgQgrams[i] = statistics.getFieldStatistics().get(fieldName).getQgramCount(Q);
                    weights[i] =  statistics.getFieldStatistics().get(fieldName).getNormalizedRange();
                    i++;
                }
            }

            LOG.info("Encoding local data :");
            LOG.info("\tEncoding name : {}", name);
            LOG.info("\tSelected data files : {}", Arrays.toString(avroPaths));
            LOG.info("\tSelected schema file : {}", schemaPath);
            LOG.info("\tSelected fields to be encoded : {}", Arrays.toString(fields));
            if(included.length !=0)
                LOG.info("\tSelected fields to included   : {}", Arrays.toString(included));
            if(statsPath != null) {
                LOG.info("\tSelected stats file : {}", statsPath);
                LOG.info("\tAvg (Q={})-grams count : {}",Q,avgQgrams);
                LOG.info("\tRBF Bit selection weights : {}",weights);
            }
            LOG.info("\tNumber of Hash functions  (K) : {}", K);
            LOG.info("\tHashing Q-Grams (Q) : {}", Q);
            LOG.info("\tScheme : {}", scheme);
            if(scheme.equals("FBF") || scheme.equals("RBF")) {
                if (fbfN > 0) LOG.info("\tFBF static size : {}", fbfN);
                else LOG.info("\tFBF dynamic sizes : {}",Arrays.toString(FieldBloomFilterEncoding.dynamicsizes(avgQgrams, K)));
            }
            if(scheme.equals("RBF")) {
                int fbfNs[] = (fbfN > 0)  ? FieldBloomFilterEncoding.staticsizes(fbfN,fields.length) :
                        FieldBloomFilterEncoding.dynamicsizes(avgQgrams, K);
                LOG.info("\tRBF size : {}", N > 0 ? N : RowBloomFilterEncoding.weightedsize(fbfNs, weights));
            }else if(scheme.equals("CLK"))
                LOG.info("\tCLK size : {}", N);
            LOG.info("\tPartitions : {} ",(partitions==1)?"No partitioning":partitions);
            LOG.info("\n");

            final BloomFilterEncoding encoding = BloomFilterEncodingUtil.instanceFactory(
                    scheme, fields.length, N, fbfN, K, Q, avgQgrams, weights);
            final Schema schema = lds.loadSchema(schemaPath);
            encoding.makeFromSchema(schema,fields,included);
            if(!encoding.isEncodingOfSchema(schema))
                throw new BloomFilterEncodingException("Encoding does not validate with source dataset.");
            final GenericRecord[] records = lds.loadDatasetRecords(avroPaths, schemaPath);

            final GenericRecord[] encodedRecords = les.encodeRecords(records, encoding);
            final Schema encodingSchema = encoding.getEncodingSchema();

            LOG.info(DatasetsUtil.prettyRecords(encodedRecords,encodingSchema));
            LOG.info("\n");

            final Path[] datasetPaths = lds.createDirectories(name,DatasetsService.OTHERS_CAN_READ_PERMISSION);

            final Path encodingSchemaPath = datasetPaths[2];
            lds.saveSchema(name,encodingSchemaPath,encodingSchema);

            final Path encodingAvroPath = datasetPaths[1];
            lds.saveDatasetRecords(name, encodedRecords, encodingSchema, encodingAvroPath, partitions);

            final Path encodingBasePath = datasetPaths[0];
            LOG.info("Encoded data path : {}",encodingBasePath);
            LOG.info("\n");

            return "DONE";
        } catch (Exception e) {
            return "Error. " + e.getClass().getSimpleName() + " : " + e.getMessage();
        }
    }

    @CliCommand(value = "encode_local_data_by_schema", help = "Encode local data based on existing encoding.")
    public String command3(
            @CliOption(key = {"avro"}, mandatory = true, help = "Local data avro files (comma separated) or including directory.")
            final String avroStr,
            @CliOption(key = {"schema"}, mandatory = true, help = "Local schema avro file.")
            final String schemaStr,
            @CliOption(key = {"name"}, mandatory = true, help = "Name of encoding.")
            final String name,
            @CliOption(key = {"fields"}, mandatory = true, help = "Selected fields to be encoded")
            final String fieldsStr,
            @CliOption(key = {"encoding_schema"}, mandatory = true, help = "Local schema avro file.")
            final String encodingSchemaStr,
            @CliOption(key = {"mapping"}, mandatory = true, help = "Mapping of fields with the encoded counterparts.")
            final String mappingStr,
            @CliOption(key = {"include"}, mandatory = false, help = "(Optional) Fields to be included")
            final String includeStr,
            @CliOption(key = {"partitions"}, mandatory = false, help = "(Optional) Partitions of the output. Default is 1 (No partitioning).")
            final String partitionsStr
    ) {
        try {

            final Path schemaPath = CommandUtil.retrievePath(schemaStr);
            final Path[] avroPaths = CommandUtil.retrievePaths(avroStr);
            final String[] fields = CommandUtil.retrieveFields(fieldsStr);
            final String[] included = CommandUtil.retrieveFields(includeStr);
            final Path existingEncodingSchemaPath = CommandUtil.retrievePath(encodingSchemaStr);
            final String[] mappings = CommandUtil.retrieveFields(mappingStr);
            final int partitions = CommandUtil.retrieveInt(partitionsStr,1);

            if(fields.length != mappings.length)
                throw new IllegalArgumentException("Not the same length of fields");

            LOG.info("Encoding local data by existing schema:");
            LOG.info("\tEncoding name : {}", name);
            LOG.info("\tSelected data files : {}", Arrays.toString(avroPaths));
            LOG.info("\tSelected schema file : {}", schemaPath);
            LOG.info("\tSelected fields to be encoded : {}", Arrays.toString(fields));
            if(included.length !=0)
                LOG.info("\tSelected fields to included   : {}", Arrays.toString(included));
            LOG.info("\tBase Encoding schema stored in file : {}",existingEncodingSchemaPath);
            final Map<String,String> field2fieldMap = new HashMap<String,String>();
            for (int i = 0; i < fields.length; i++)
                field2fieldMap.put(fields[i],mappings[i]);
            LOG.info("\tField Mappings are : {}",field2fieldMap);
            LOG.info("\tPartitions : {} ",(partitions==1)?"No partitioning":partitions);
            LOG.info("\n");

            final Schema existingEncodingSchema = lds.loadSchema(existingEncodingSchemaPath);
            final Schema schema = lds.loadSchema(schemaPath);
            final String schemeName = BloomFilterEncodingUtil.retrieveSchemeName(existingEncodingSchema);
            final BloomFilterEncoding encoding =
                    BloomFilterEncodingUtil.newInstance(schemeName);
            encoding.setupFromSchema(
                    BloomFilterEncodingUtil.basedOnExistingSchema(
                            schema, fields, included,
                            existingEncodingSchema, mappings)
            );

            if(!encoding.isEncodingOfSchema(schema))
                throw new BloomFilterEncodingException("Encoding does not validate with source dataset.");

            final GenericRecord[] records = lds.loadDatasetRecords(avroPaths, schemaPath);

            final GenericRecord[] encodedRecords = les.encodeRecords(records, encoding);
            final Schema encodingSchema = encoding.getEncodingSchema();

            LOG.info(DatasetsUtil.prettyRecords(encodedRecords,encodingSchema));
            LOG.info("\n");

            final Path[] datasetPaths = lds.createDirectories(name,DatasetsService.OTHERS_CAN_READ_PERMISSION);

            final Path encodingSchemaPath = datasetPaths[2];
            lds.saveSchema(name,encodingSchemaPath,encodingSchema);

            final Path encodingAvroPath = datasetPaths[1];
            lds.saveDatasetRecords(name, encodedRecords, encodingSchema, encodingAvroPath, partitions);

            final Path encodingBasePath = datasetPaths[0];
            LOG.info("Encoded data path : {}",encodingBasePath);
            LOG.info("\n");

            return "DONE";
        } catch (Exception e) {
            return "Error. " + e.getClass().getSimpleName() + " : " + e.getMessage();
        }
    }

    /**
     *  HDFS ENCODING COMMANDS
     */

    @CliCommand(value = "encode_data", help = "Encode HDFS data.")
    public String command4(
            @CliOption(key = {"name"}, mandatory = true, help = "Uploaded dataset name.")
            final String name,
            @CliOption(key = {"encoding_name"}, mandatory = true, help = "Name of encoding.")
            final String encodingName,
            @CliOption(key = {"fields"}, mandatory = true, help = "Selected fields to be encoded")
            final String fieldsStr,
            @CliOption(key= {"scheme"}, mandatory = true, help = "One of the following encoding schemes : {FBF,RBF,CLK}.")
            final String scheme,
            @CliOption(key = {"stats"}, mandatory = true, help = "Path to property file containing the required data statistics")
            final String pathStr,
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
            final String Qstr
    ) {
        try {
            BloomFilterEncodingUtil.schemeNameSupported(scheme);

            final String[] fields = CommandUtil.retrieveFields(fieldsStr);
            final String[] included = CommandUtil.retrieveFields(includeStr);
            final int fbfN = CommandUtil.retrieveInt(fbfNstr, -1);
            final int N = CommandUtil.retrieveInt(Nstr, -1);
            final int K = CommandUtil.retrieveInt(Kstr, 15);
            final int Q = CommandUtil.retrieveInt(Qstr, 2);
            final Path statsPath = CommandUtil.retrievePath(pathStr);
            final double[] avgQgrams = (statsPath == null)  ? null : new double[fields.length];
            final double[] weights = (statsPath == null)  ? null : new double[fields.length];
            if(statsPath != null) {
                DatasetStatistics statistics = ds.loadStats(statsPath);
                if(scheme.equals("RBF") && N < 0 && fields.length != statistics.getFieldCount())
                    throw new IllegalStateException("In the case of weighted RBF all fields in stats must be included. Should recalculate stats");
                int i = 0;
                for (String fieldName : fields) {
                    avgQgrams[i] = statistics.getFieldStatistics().get(fieldName).getQgramCount(Q);
                    weights[i] =  statistics.getFieldStatistics().get(fieldName).getNormalizedRange();
                    i++;
                }
            }

            LOG.info("Encoding HDFS data :");
            LOG.info("\tDataset name : {}",name);
            LOG.info("\tEncoding name : {}", encodingName);
            LOG.info("\tSelected fields to be encoded : {}", Arrays.toString(fields));
            if(included.length !=0)
                LOG.info("\tSelected fields to included   : {}", Arrays.toString(included));
            if(statsPath != null) {
                LOG.info("\tSelected stats file : {}", statsPath);
                LOG.info("\tAvg (Q={})-grams count : {}",Q,avgQgrams);
                LOG.info("\tRBF Bit selection weights : {}",weights);
            }
            LOG.info("\tNumber of Hash functions  (K) : {}", K);
            LOG.info("\tHashing Q-Grams (Q) : {}", Q);
            LOG.info("\tScheme : {}", scheme);
            if(scheme.equals("FBF") || scheme.equals("RBF")) {
                if (fbfN > 0) LOG.info("\tFBF static size : {}", fbfN);
                else LOG.info("\tFBF dynamic sizes : {}",Arrays.toString(FieldBloomFilterEncoding.dynamicsizes(avgQgrams, K)));
            }
            if(scheme.equals("RBF")) {
                int fbfNs[] = (fbfN > 0)  ? FieldBloomFilterEncoding.staticsizes(fbfN,fields.length) :
                        FieldBloomFilterEncoding.dynamicsizes(avgQgrams, K);
                LOG.info("\tRBF size : {}", N > 0 ? N : RowBloomFilterEncoding.weightedsize(fbfNs, weights));
            }else if(scheme.equals("CLK"))
                LOG.info("\tCLK size : {}", N);
            LOG.info("\n");

            final Path[] datasetPaths = ds.retrieveDirectories(name);
            final Path inputBasePath = datasetPaths[0];
            final Path inputAvroPath = datasetPaths[1];
            final Path inputSchemaPath = ds.retrieveSchemaPath(datasetPaths[2]);

            final BloomFilterEncoding encoding = BloomFilterEncodingUtil.instanceFactory(
                    scheme, fields.length, N, fbfN, K, Q, avgQgrams, weights);
            final Schema schema = ds.loadSchema(inputSchemaPath);
            encoding.makeFromSchema(schema,fields,included);
            if(!encoding.isEncodingOfSchema(schema))
                throw new BloomFilterEncodingException("Encoding does not validate with source dataset.");
            final Schema encodingSchema = encoding.getEncodingSchema();

            final Path[] encodingPaths = ds.createDirectories(encodingName,inputBasePath,DatasetsService.OTHERS_CAN_READ_PERMISSION);
            final Path encodingBasePath = encodingPaths[0];
            final Path encodingAvroPath = encodingPaths[1];
            final Path encodingSchemaPath =
                    new Path(encodingPaths[2],String.format("%s.avsc",encodingName));
            ds.saveSchema(encodingSchemaPath,encodingSchema);

            es.runEncodeDatasetTool(inputAvroPath,inputSchemaPath,encodingAvroPath,encodingSchemaPath);
            ds.setOthersCanReadPermission(encodingAvroPath);

            LOG.info("\tEncoded data path : {}",encodingBasePath);
            LOG.info("\n");
            return "DONE";
        } catch(Exception e) {
            return "Error. " + e.getClass().getSimpleName() + " : " + e.getMessage();
        }
    }

    @CliCommand(value = "encode_data_by_schema", help = "Encode HDFS data by existing schema.")
    public String command5(
            @CliOption(key = {"name"}, mandatory = true, help = "Uploaded dataset name.")
            final String name,
            @CliOption(key = {"encoding_name"}, mandatory = true, help = "Name of encoding.")
            final String encodingName,
            @CliOption(key = {"fields"}, mandatory = true, help = "Selected fields to be encoded")
            final String fieldsStr,
            @CliOption(key = {"encoding_schema"}, mandatory = true, help = "HDFS schema avro file.")
            final String encodingSchemaStr,
            @CliOption(key = {"mapping"}, mandatory = true, help = "Mapping of fields with the encoded counterparts.")
            final String mappingStr,
            @CliOption(key = {"include"}, mandatory = false, help = "(Optional) Fields to be included")
            final String includeStr
    ) {
        try {
            final String[] fields = CommandUtil.retrieveFields(fieldsStr);
            final String[] included = CommandUtil.retrieveFields(includeStr);
            final Path existingEncodingSchemaPath = CommandUtil.retrievePath(encodingSchemaStr);
            final String[] mappings = CommandUtil.retrieveFields(mappingStr);

            if(fields.length != mappings.length)
                throw new IllegalArgumentException("Not the same length of fields");

            LOG.info("Encoding HDFS data by existing schema:");
            LOG.info("\tEncoding name : {}", encodingName);
            LOG.info("\tDataset name : {}", name);
            LOG.info("\tSelected fields to be encoded : {}", Arrays.toString(fields));
            if(included.length !=0)
                LOG.info("\tSelected fields to included   : {}", Arrays.toString(included));
            LOG.info("\tBase Encoding schema stored in file : {}",existingEncodingSchemaPath);
            final Map<String,String> field2fieldMap = new HashMap<String,String>();
            for (int i = 0; i < fields.length; i++)
                field2fieldMap.put(fields[i],mappings[i]);
            LOG.info("\tField Mappings are : {}",field2fieldMap);

            final Path[] datasetPaths = ds.retrieveDirectories(name);
            final Path inputBasePath = datasetPaths[0];
            final Path inputAvroPath = datasetPaths[1];
            final Path inputSchemaPath = ds.retrieveSchemaPath(datasetPaths[2]);

            final Schema schema = ds.loadSchema(inputSchemaPath);
            final Schema existingEncodingSchema = lds.loadSchema(existingEncodingSchemaPath);
            final String schemeName = BloomFilterEncodingUtil.retrieveSchemeName(existingEncodingSchema);
            final BloomFilterEncoding encoding =
                    BloomFilterEncodingUtil.newInstance(schemeName);
            encoding.setupFromSchema(
                    BloomFilterEncodingUtil.basedOnExistingSchema(
                            schema, fields, included,
                            existingEncodingSchema, mappings)
            );
            if(!encoding.isEncodingOfSchema(schema))
                throw new BloomFilterEncodingException("Encoding does not validate with source dataset.");
            final Schema encodingSchema = encoding.getEncodingSchema();

            final Path[] encodingPaths = ds.createDirectories(encodingName,inputBasePath,DatasetsService.OTHERS_CAN_READ_PERMISSION);
            final Path encodingBasePath = encodingPaths[0];
            final Path encodingAvroPath = encodingPaths[1];
            final Path encodingSchemaPath =
                    new Path(encodingPaths[2],String.format("%s.avsc",encodingName));
            ds.saveSchema(encodingSchemaPath,encodingSchema);


            es.runEncodeDatasetTool(inputAvroPath,inputSchemaPath,encodingAvroPath,encodingSchemaPath);
            ds.setOthersCanReadPermission(encodingAvroPath);

            LOG.info("Encoded data path : {}",encodingBasePath);
            LOG.info("\n");

            return "DONE";

        } catch(Exception e) {
            return "Error. " + e.getClass().getSimpleName() + " : " + e.getMessage();
        }
    }
}
