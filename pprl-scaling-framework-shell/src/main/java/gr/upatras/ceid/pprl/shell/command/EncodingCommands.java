package gr.upatras.ceid.pprl.shell.command;

import gr.upatras.ceid.pprl.datasets.DatasetStatistics;
import gr.upatras.ceid.pprl.datasets.service.DatasetsService;
import gr.upatras.ceid.pprl.datasets.service.LocalDatasetsService;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncoding;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncodingException;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncodingUtil;
import gr.upatras.ceid.pprl.encoding.FieldBloomFilterEncoding;
import gr.upatras.ceid.pprl.encoding.RowBloomFilterEncoding;
import gr.upatras.ceid.pprl.encoding.service.EncodingService;
import gr.upatras.ceid.pprl.encoding.service.LocalEncodingService;
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

    @CliAvailabilityIndicator(value = {"encode_supported_schemes","encode_calculate_encoding_sizes"})
    public boolean availability0() { return true; }
    @CliAvailabilityIndicator(value = {"encode_local_data","encode_local_data_by_schema"})
    public boolean availability1() { return les != null && lds != null; }

    // TODO commands
    //      encode_data
    //      encode_data_by_schema


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
            final Path statsPath = CommandUtil.retrievePath(pathStr);
            LOG.info("Calculating encoding sizes :");
            LOG.info("\tSelected stats file : {}", statsPath);
            final int Q = CommandUtil.retrieveInt(qStr, 2);
            if(Q < 2 || Q > 4) throw new IllegalArgumentException("Q is limited to {2,3,4}.");
            final int K = CommandUtil.retrieveInt(kStr, 15);
            if(K < 1) throw new IllegalArgumentException("K must be at least 1.");
            DatasetStatistics statistics = lds.loadStats(statsPath);
            LOG.info(CommandUtil.prettyBFEStats(statistics.getFieldStatistics(), K, Q));
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
            BloomFilterEncodingUtil.schemeNameSupported(scheme);

            final Path schemaPath = CommandUtil.retrievePath(schemaStr);
            final Path[] avroPaths = CommandUtil.retrievePaths(avroStr);
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

            final BloomFilterEncoding encoding = BloomFilterEncodingUtil.instanceFactory(
                    scheme, fields.length, N, fbfN, K, Q, avgQgrams, weights);
            final Schema schema = lds.loadSchema(schemaPath);
            encoding.makeFromSchema(schema,fields,included);
            if(!encoding.isEncodingOfSchema(schema))
                throw new BloomFilterEncodingException("Encoding does not validate with source dataset.");
            final GenericRecord[] records = lds.loadRecords(avroPaths,schemaPath);

            final GenericRecord[] encodedRecords = les.encodeRecords(records, encoding);
            final Schema encodingSchema = encoding.getEncodingSchema();

            final Path encodingDatapath = lds.saveRecords(name,encodedRecords,encodingSchema);

            LOG.info("\tEncoded data path = {}",encodingDatapath);
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
            final String includeStr
    ) {
        try { // TODO Test this command. Need to have at least two datasets here

            final Path schemaPath = CommandUtil.retrievePath(schemaStr);
            final Path[] avroPaths = CommandUtil.retrievePaths(avroStr);
            final String[] fields = CommandUtil.retrieveFields(fieldsStr);
            final String[] included = CommandUtil.retrieveFields(includeStr);
            final Path encodingSchemaPath = CommandUtil.retrievePath(encodingSchemaStr);
            final String[] existingFieldNames = CommandUtil.retrieveFields(fieldsStr);

            if(fields.length != existingFieldNames.length)
                throw new IllegalArgumentException("Not the same length of fields");

            LOG.info("Encoding local data :");
            LOG.info("\tEncoding name : {}", name);
            LOG.info("\tSelected data files : {}", Arrays.toString(avroPaths));
            LOG.info("\tSelected schema file : {}", schemaPath);
            LOG.info("\tSelected fields to be encoded : {}", Arrays.toString(fields));
            if(included.length !=0)
                LOG.info("\tSelected fields to included   : {}", Arrays.toString(included));
            LOG.info("\tBase Encoding schema stored in file : {}",encodingSchemaPath);
            final Map<String,String> field2fieldMap = new HashMap<String,String>();
            for (int i = 0; i < fields.length; i++)
                field2fieldMap.put(fields[i],existingFieldNames[i]);
            LOG.info("\tField Mappings are : {}",field2fieldMap);

            final Schema existingEncodingSchema = lds.loadSchema(encodingSchemaPath);
            final Schema schema = lds.loadSchema(schemaPath);
            final BloomFilterEncoding encoding =
                    BloomFilterEncodingUtil.newInstance(existingEncodingSchema);
            encoding.setupFromSchema(
                    BloomFilterEncodingUtil.basedOnExistingSchema(
                            schema, fields, included,
                            existingEncodingSchema, existingFieldNames)
            );

            if(!encoding.isEncodingOfSchema(schema))
                throw new BloomFilterEncodingException("Encoding does not validate with source dataset.");

            final GenericRecord[] records = lds.loadRecords(avroPaths,schemaPath);

            final GenericRecord[] encodedRecords = les.encodeRecords(records, encoding);
            final Schema encodingSchema = encoding.getEncodingSchema();

            final Path encodingDatapath = lds.saveRecords(name,encodedRecords,encodingSchema);

            LOG.info("\tEncoded data path = {}",encodingDatapath);
            LOG.info("\n");

            return "DONE";
        } catch (Exception e) {
            return "Error. " + e.getClass().getSimpleName() + " : " + e.getMessage();
        }
    }
}
