package gr.upatras.ceid.pprl.shared;

/**
 * Names and Constants project wide class
 */
public class Names {

    /**
     * Import Datasets
     */
    public static final String IMPORT_DATA_HDFS_INPUT_PATH = "pprl.import.input.path";
    public static final String IMPORT_DATA_HDFS_OUTPUT_PATH = "pprl.import.output.path";
    public static final String IMPORT_DATA_INPUT_TYPE = "pprl.import.input.type";
    public static final String IMPORT_DATA_OUTPUT_TYPE = "pprl.import.output.type";
    public static final String IMPORT_DATA_HBASE_TABLE = "pprl.import.hbase.table";
    public static final String IMPORT_DATA_OVERWRITE_OUTPUT = "pprl.import.output.overwrite";
    public static final String[] IMPORT_DATA_INPUT_TYPES = {
            "text",
            "xml"
    };
    public static final String[] IMPORT_DATA_OUTPUT_TYPES = {
            "avro",
            "hbase"
    };
    public final static String[] IMPORT_DATA_ALL = {
            IMPORT_DATA_HDFS_INPUT_PATH,
            IMPORT_DATA_HDFS_OUTPUT_PATH,
            IMPORT_DATA_INPUT_TYPE,
            IMPORT_DATA_OUTPUT_TYPE,
            IMPORT_DATA_HBASE_TABLE,
            IMPORT_DATA_OVERWRITE_OUTPUT
    };
}
