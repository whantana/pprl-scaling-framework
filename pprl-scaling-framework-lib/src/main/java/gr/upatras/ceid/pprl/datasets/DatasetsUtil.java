package gr.upatras.ceid.pprl.datasets;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.hadoop.io.AvroKeyComparator;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.AvroFSInput;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Datasets utility class.
 */
public class DatasetsUtil {
    // TODO more DEBUG LOGGING HERE
    private static final Logger LOG = LoggerFactory.getLogger(DatasetsUtil.class);

    /**
     * Returns true if <code>FileSystem</code> is local. False for HDFS filesystem.
     *
     * @param fs a <code>FileSystem</code> reference.
     * @return true if local , false if hdfs.
     */
    private static boolean fsIsLocal(final FileSystem fs) {
        return fs.getUri().toString().contains("file");
    }

    /**
     * Returns the paths consisting of a dataset directory.
     *
     * @param fs a <code>FileSystem</code> reference.
     * @param name a dataset name.
     * @param basePath a base path.
     * @return the paths consisting of a dataset directory.
     * @throws IOException
     */
    public static Path[] retrieveDatasetDirectories(final FileSystem fs, final String name, final Path basePath)
            throws IOException {
        return retrieveDatasetDirectories(fs,new Path(basePath,name));
    }

    /**
     * Returns the paths consisting of a dataset directory.
     *
     * @param fs a <code>FileSystem</code> reference.
     * @param path dataset base path.
     * @return the paths consisting of a dataset directory
     * @throws IOException
     */
    public static Path[] retrieveDatasetDirectories(final FileSystem fs, final Path path)
            throws IOException {
        final Path paths[] = new Path[3];
        paths[0] = path;
        checkIfExists(fs,paths[0]);
        paths[1] = new Path(paths[0],"avro");
        checkIfExists(fs,paths[1]);
        paths[2] = new Path(paths[0],"schema");
        checkIfExists(fs,paths[2]);
        return paths;
    }


    /**
     * Checks if path exists. Nothing if it does, exception is thrown otherwise.
     *
     * @param fs a <code>FileSystem</code> reference.
     * @param path a path.
     * @throws IOException
     */
    public static void checkIfExists(final FileSystem fs, final Path path)
            throws IOException {
        boolean baseExists = fs.exists(path);
        if(!baseExists) throw new IllegalArgumentException(
                String.format("Path does not exist  [FileSystem=%s,Path=%s]",
                        fsIsLocal(fs) ? "local" : fs.getUri(), path));
    }

    /**
     * Returns true if selected field names belong to schema.
     *
     * @param schema an avro schema.
     * @param selectedNames selected field names.
     * @return true if selected field names belong to schema.
     */
    public static boolean fieldNamesBelongsToSchema(final Schema schema, final String... selectedNames) {
        for(String name : selectedNames) {
            boolean nameFound = false;
            for(Schema.Field field : schema.getFields())
                if(field.name().equals(name)) { nameFound = true ; break; }
            if(!nameFound) return false;
        }
        return true;
    }

    /**
     * Create dataset directories.
     *
     * @param fs a <code>FileSystem</code> reference.
     * @param name a dataset name.
     * @param basePath a base path.
     * @return created dataset directories.
     * @throws IOException
     */
    public static Path[] createDatasetDirectories(final FileSystem fs, final String name, final Path basePath)
            throws IOException {
        return createDatasetDirectories(fs,name,basePath,null);
    }

    /**
     * Create dataset directories.
     *
     * @param fs a <code>FileSystem</code> reference.
     * @param name a dataset name.
     * @param basePath a base path.
     * @param permission a filesystem permission.
     * @return created dataset directories.
     * @throws IOException
     */
    public static Path[] createDatasetDirectories(final FileSystem fs, final String name, final Path basePath,
                                                  final FsPermission permission)
            throws IOException {
        LOG.debug(String.format("Creating dataset directories [FileSystem=%s,Path=%s]",
                fsIsLocal(fs) ? "local" : fs.getUri(), name));
        final Path datasetPath = new Path(basePath,name);
        if (fs.exists(datasetPath)) {
            LOG.debug(String.format("Directory it already exists " +
                            "[FileSystem=%s,basePath=%s]",
                    fsIsLocal(fs) ? "local" : fs.getUri(), name));
        } else {
            LOG.debug(String.format("Making base directory [FileSystem=%s,basePath=%s]",
                    fsIsLocal(fs) ? "local" : fs.getUri(), datasetPath));
            if(permission == null ) fs.mkdirs(datasetPath);
            else fs.mkdirs(datasetPath, permission);
        }

        final Path datasetAvroPath = new Path(datasetPath,"avro");
        if (fs.exists(datasetAvroPath)) {
            LOG.debug(String.format("Deleting avro directory because it exists " +
                            "[FileSystem=%s,datasetAvroPath=%s]",
                    fsIsLocal(fs) ? "local" : fs.getUri(), datasetAvroPath));
            fs.delete(datasetAvroPath, true);

        }
        LOG.debug(String.format("Making avro directory [FileSystem=%s,datasetAvroPath=%s]",
                fsIsLocal(fs) ? "local" : fs.getUri(), datasetAvroPath));
        if(permission == null ) fs.mkdirs(datasetPath);
        else fs.mkdirs(datasetPath, permission);


        final Path datasetSchemaPath = new Path(datasetPath,"schema");
        if (fs.exists(datasetSchemaPath)) {
            LOG.debug(String.format("Deleting avro directory because it exists " +
                            "[FileSystem=%s,datasetAvroPath=%s]",
                    fsIsLocal(fs) ? "local" : fs.getUri(), datasetSchemaPath));
            fs.delete(datasetSchemaPath,true);
        }
        LOG.debug(String.format("Making schema directory [FileSystem=%s,datasetSchemaPath=%s]",
                fsIsLocal(fs) ? "local" : fs.getUri(), datasetSchemaPath));
        if(permission == null ) fs.mkdirs(datasetPath);
        else fs.mkdirs(datasetPath, permission);

        return new Path[]{datasetPath,datasetAvroPath,datasetSchemaPath};
    }

    /**
     * Load Avro Records.
     *
     * @param fs a <code>FileSystem</code> reference.
     * @param schema schema.
     * @param paths a <code>Path</code> array.
     * @return an array of avro generic records.
     * @throws IOException
     */
    public static GenericRecord[] loadAvroRecordsFromFSPaths(final FileSystem fs,
                                                             final Schema schema,
                                                             final Path... paths) throws IOException {
        LOG.debug("Loading records");
        final List<GenericRecord> recordList = new ArrayList<GenericRecord>();
        final DatasetsUtil.DatasetRecordReader reader =
                new DatasetsUtil.DatasetRecordReader(fs, schema, paths);
        int i = 0;
        while (reader.hasNext()) {
            recordList.add(i, reader.next());
            i++;
            LOG.debug("Loading Record #{}", i);
        }
        LOG.debug("Loaded {} records.",recordList.size());
        return recordList.toArray(new GenericRecord[recordList.size()]);
    }

    /**
     * Save Avro Records.
     *
     * @param fs a <code>FileSystem</code> reference.
     * @param records  a generic records array.
     * @param schema a schema instance.
     * @param basePath a base path.
     * @param name a name.
     * @param partitions number of partitions.
     * @throws IOException
     */
    public static void saveAvroRecordsToFSPath(final FileSystem fs,
                                               final GenericRecord[] records,
                                               final Schema schema,
                                               final Path basePath,
                                               final String name,
                                               final int partitions) throws IOException {
        LOG.debug("Writing records {} records.",records.length);
        final DatasetsUtil.DatasetRecordWriter writer =
                new DatasetsUtil.DatasetRecordWriter(fs,name,schema,basePath,partitions);
        writer.writeRecords(records);
        writer.close();
    }

    /**
     * Return schema read from a filesystem path.
     *
     * @param fs a <code>FileSystem</code> reference.
     * @param schemaPath an avro schema path.
     * @return schema.
     * @throws DatasetException
     * @throws IOException
     */
    public static Schema loadSchemaFromFSPath(final FileSystem fs, final Path schemaPath)
            throws DatasetException, IOException {
        LOG.debug(String.format("Loading path [FileSystem=%s,Path=%s]",
                fsIsLocal(fs) ? "local" : fs.getUri(), schemaPath));
        FileStatus fss = fs.getFileStatus(schemaPath);
        if (fss.isFile() && fss.getPath().getName().endsWith(".avsc"))
            return (new Schema.Parser()).parse(fs.open(fss.getPath()));
        else throw new DatasetException("Path provided not a schema file.");
    }

    /**
     * Write schema to a filesystem path.
     *
     * @param fs a <code>FileSystem</code> reference.
     * @param schema an avro schema.
     * @param schemaPath an avro schema path.
     * @throws DatasetException
     * @throws IOException
     */
    public static void saveSchemaToFSPath(final FileSystem fs, final Schema schema, final Path schemaPath)
            throws DatasetException, IOException {
        LOG.debug(String.format("Saving path [FileSystem=%s,Path=%s]",
                fsIsLocal(fs) ? "local" : fs.getUri(), schemaPath));
        final FSDataOutputStream fsdos = fs.create(schemaPath, true);
        fsdos.write(schema.toString(true).getBytes());
        fsdos.close();
    }

    /**
     * Create a schema.
     *
     * @param schemaName schema name.
     * @param doc schema doc.
     * @param ns schema namespace.
     * @param fieldNames field names.
     * @param fieldTypes field types.
     * @param docs field docs.
     * @return created schema.
     */
    public static Schema avroSchema(final String schemaName,
                                    final String doc, final String ns,
                                    final String[] fieldNames, final Schema.Type[] fieldTypes,
                                    final String[] docs) {
        Schema schema = Schema.createRecord(schemaName,doc,ns,false);
        assert fieldNames.length == fieldTypes.length;
        List<Schema.Field> fields = new ArrayList<Schema.Field>();
        for (int i = 0; i < fieldNames.length; i++)
            fields.add(new Schema.Field(fieldNames[i], Schema.create(fieldTypes[i]),docs[i],null));
        schema.setFields(fields);
        return schema;
    }

    /**
     * Returns the names of the fields in the schema.
     *
     * @param schema an avro schema.
     * @return the names of the fields in the schema.
     */
    public static String[] fieldNames(final Schema schema) {
        final String[] fieldNames = new String[schema.getFields().size()];
        int i = 0;
        for(Schema.Field f : schema.getFields()) fieldNames[i++] = f.name();
        return fieldNames;
    }

    /**
     * CSV to Avro. It creates a dataset directory set.
     *
     * @param fs a <code>FileSystem</code> reference.
     * @param schema an avro schema.
     * @param name name of the dataset.
     * @param basePath a base path.
     * @param csvPath a csv path.
     * @return created dataset base path.
     * @throws DatasetException
     * @throws IOException
     */
    public static Path csv2avro(final FileSystem fs, final Schema schema ,
                                final String name,
                                final Path basePath,
                                final Path csvPath)
            throws DatasetException, IOException {
        final Path[] paths = createDatasetDirectories(fs,name,basePath);
        final Path avroBasePath = paths[1];
        final Path schemaBasePath = paths[2];

        saveSchemaToFSPath(fs, schema, new Path(schemaBasePath,name+".avsc"));
        DatasetRecordWriter writer = new DatasetRecordWriter(fs,name,schema,avroBasePath);
        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(csvPath)));
        try {

            final String[] fieldNames = new String[schema.getFields().size()];
            int j = 0;
            for (Schema.Field field : schema.getFields()) {
                fieldNames[j] = field.name();
                j++;
            }
            String line;
            while ((line = reader.readLine()) != null) {
                final String[] parts = line.split(",",fieldNames.length);
                final GenericRecord record = new GenericData.Record(schema);
                for (int i = 0; i < parts.length; i++) {
                    final String part = parts[i];
                    final Schema.Type type = schema.getField(fieldNames[i]).schema().getType();
                    Object obj;
                    switch (type) {
                        case BOOLEAN:
                            obj = Boolean.parseBoolean((part == null || part.isEmpty()) ? null : part);
                            break;
                        case STRING:
                            obj = (part == null || part.isEmpty()) ? "-NA-" : part;
                            break;
                        case INT:
                            obj = (part == null || part.isEmpty()) ? 0 : Integer.parseInt(part);
                            break;
                        case LONG:
                            obj = (part == null || part.isEmpty()) ? 0 : Long.parseLong(part);
                            break;
                        case DOUBLE:
                            obj = (part == null || part.isEmpty()) ? Double.NaN : Double.parseDouble(part);
                            break;
                        case FLOAT:
                            obj = (part == null || part.isEmpty()) ? Float.NaN : Float.parseFloat(part);
                            break;
                        default:
                            obj = null;
                    }
                    record.put(fieldNames[i], obj);
                }
                writer.writeRecord(record);
            }
            writer.close();
            reader.close();
            return paths[0];
        } finally {
            writer.close();
            reader.close();
        }
    }

    /**
     * A pretty schema description.
     *
     * @param schema dataset schema.
     * @return a pretty schema description.
     */
    public static String prettySchemaDescription(final Schema schema) {
        final Map<String, String> description = new HashMap<String, String>();
        for (Schema.Field f : schema.getFields())
            description.put(f.name(), f.schema().getType().toString());
        final StringBuilder sb = new StringBuilder();
        int i = 1;
        for(Map.Entry<String,String> entry : description.entrySet())
            sb.append(String.format("%d, %s %s\n",i++,entry.getKey(),entry.getValue()));
        return sb.toString();
    }

    /**
     * Pretty records listing.
     *
     * @param records generic avro record array.
     * @param schema schema.
     * @return a pretty records listing.
     */
    public static String prettyRecords(final GenericRecord[] records,
                                       final Schema schema) {
        final StringBuilder sb = new StringBuilder();
        final List<Schema.Field> fields = schema.getFields();
        final List<Schema.Type> types = new ArrayList<Schema.Type>();
        final List<String> fieldNames = new ArrayList<String>();

        for (int i = 0; i < fields.size() ; i++) {
            fieldNames.add(i, fields.get(i).name());
            types.add(i,fields.get(i).schema().getType());
        }
        sb.append("#Records =").append(records.length).append("\n");
        sb.append("#Fields =").append(fields.size()).append("\n");

        final StringBuilder hsb = new StringBuilder();
        for (int i = 0; i < fields.size() ; i++) {
            final Schema.Type type = types.get(i);
            hsb.append(String.format(
                    (type.equals(Schema.Type.FIXED)) ? "%100s|" : "%25s|",
                    String.format("%s (%s)", fieldNames.get(i),types.get(i))));
        }
        final String header = hsb.toString();
        sb.append(header).append("\n");
        sb.append(new String(new char[header.length()]).replace("\0", "-")).append("\n");
        for (GenericRecord record : records) {
            final StringBuilder rsb = new StringBuilder();
            for (int i = 0; i < fields.size(); i++) {
                final String fieldName = fieldNames.get(i);
                final Schema.Type type = types.get(i);
                if (type.equals(Schema.Type.FIXED)) {
                    GenericData.Fixed fixed = (GenericData.Fixed) record.get(i);
                    String val = prettyBinary(fixed.bytes());
                    if(fixed.bytes().length * 8 < 100)
                        rsb.append(String.format("%100s|", val));
                    else
                        rsb.append(String.format("%100s|",
                                val.substring(0,48) + "..." + val.substring(val.length()-48,val.length())));
                } else {
                    String val = String.valueOf(record.get(fieldName));
                    if (val.length() > 25) {
                        val = val.substring(0, 10) + "..." + val.substring(val.length() - 10);
                    }
                    rsb.append(String.format("%25s|", val));
                }
            }
            sb.append(rsb.toString()).append("\n");
        }

        return sb.toString();
    }

    /**
     * Pretty binary representation.
     *
     * @param binary a byte array.
     * @return pretty binary representation.
     */
    public static String prettyBinary(final byte[] binary) {
        final StringBuilder sb = new StringBuilder();
        for (int i = (binary.length - 1); i >= 0 ; i--) {
            byte b = binary[i];
            sb.append(String.format("%8s", Integer.toBinaryString(b & 0xFF)).replace(' ', '0'));
        }
        return sb.toString();
    }

    /**
     * Dataset Record Writer class.
     */
    public static class DatasetRecordWriter implements Closeable {

        private static final Logger LOG = LoggerFactory.getLogger(DatasetRecordWriter.class);
        private List<DataFileWriter<GenericRecord>> fileWriters; // writers 1 each files
        private int partitions; // number of files


        public DatasetRecordWriter(final FileSystem fs, final String name, final Schema schema)
                throws IOException {
            this(fs,name,schema,fs.getWorkingDirectory(),1);
        }

        public DatasetRecordWriter(final FileSystem fs, final String name,
                                   final Schema schema, final Path parentPath)
                throws IOException {
            this(fs,name,schema,parentPath,1);
        }

        /**
         * Constructor
         *
         * @param fs a <code>FileSystem</code> reference.
         * @param name a dataset name.
         * @param schema an avro schema.
         * @param parentPath parent path of data files.
         * @param partitions number of files to spit writing.
         * @throws IOException
         */
        public DatasetRecordWriter(final FileSystem fs, final String name,
                                   final Schema schema,
                                   final Path parentPath,int partitions)
                throws IOException {
            fileWriters = new ArrayList<DataFileWriter<GenericRecord>>(partitions);
            this.partitions = partitions;
            LOG.debug("#Writers= {}",partitions);
            for (int i = 0; i < partitions; i++) {
                DataFileWriter<GenericRecord> fw =
                        new DataFileWriter<GenericRecord>(new GenericDatumWriter<GenericRecord>(schema));
                final Path filePath;
                if(partitions == 1) filePath = new Path(parentPath,name + ".avro");
                else {
                    final Path partitionPath = new Path(parentPath,String.valueOf(i));
                    fs.mkdirs(partitionPath);
                    filePath = new Path(partitionPath,name + ".avro");
                }
                try {
                    fw.create(schema,fs.create(filePath,true));
                    fileWriters.add(i,fw);
                    LOG.debug("Adding writer {}",i);
                } catch (IOException e) {
                    LOG.error(e.getMessage());
                    fw.close();
                    if(!fileWriters.isEmpty()) close();
                }
            }
        }

        /**
         * Write in the first writer a record.
         *
         * @param record avro record.
         * @throws IOException
         */
        public void writeRecord(final GenericRecord record) throws IOException {
            writeRecord(record,0);
        }

        /**
         * Write records.
         *
         * @param records avro records array.
         * @throws IOException
         */
        public void writeRecords(final GenericRecord[] records) throws IOException {
            final int[] recordIndexOfPartition = new int[partitions];
            final int recordCount = records.length;
            double range = (double)recordCount/partitions;
            for (int i = 0; i < partitions; i++) {
                recordIndexOfPartition[i] = i * (int) ((i == 1) ? Math.ceil(range) : Math.floor(range));
            }
            for (int i = 0; i < partitions; i++) {
                int start = recordIndexOfPartition[i];
                int end = (i < partitions - 1) ? recordIndexOfPartition[i+1] : recordCount ;
                LOG.debug("Writer {} writes records in range {}",i,String.format("[%d,%d)",start,end));
                for(int j = start ; j < end ; j++) writeRecord(records[j],i);
                fileWriters.get(i).close();
            }
        }

        /**
         * Write record in the i-th writer.
         * @param record avro record.
         * @param i i-th writer
         * @throws IOException
         */
        private void writeRecord(final GenericRecord record , final int i) throws IOException {
            try {
                fileWriters.get(i).append(record);
            } catch (IOException e) {
                fileWriters.get(i).close();
            }
        }

        /**
         * Close all writers
         * @throws IOException
         */
        public void close() throws IOException {
            for (DataFileWriter fw : fileWriters) fw.close();
        }
    }

    /**
     * Returns all avro data files paths.
     *
     * @param fs a <code>FileSystem</code> reference.
     * @param parentPath a parent path.
     * @return avro data files paths.
     * @throws IOException
     */
    public static SortedSet<Path> getPathsRecurcively(final FileSystem fs, final Path parentPath) throws IOException {
        final SortedSet<Path> paths = new TreeSet<Path>();
        final RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(parentPath, true);
        while(iterator.hasNext()) {
            final LocatedFileStatus lfs = iterator.next();
            if (lfs.isFile() && lfs.getPath().toString().endsWith(".avro"))
                paths.add(lfs.getPath());
        }
        LOG.debug("getPathsRecurcively returns {}", paths);
        return paths;
    }

    /**
     * Returns all avro data files paths.
     *
     * @param fs a <code>FileSystem</code> reference.
     * @param pathArray parent paths array.
     * @return avro data files paths.
     * @throws IOException
     */
    public static SortedSet<Path> getAllAvroPaths(final FileSystem fs, final Path... pathArray) throws IOException {
        final SortedSet<Path> paths = new TreeSet<Path>();
        for(Path p : pathArray) {
            if (fs.isDirectory(p)) {
                final RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(p, true);
                while (iterator.hasNext()) {
                    final LocatedFileStatus lfs = iterator.next();
                    if (lfs.isFile() && lfs.getPath().toString().endsWith(".avro"))
                        paths.add(lfs.getPath());
                }

            } else {
                if (fs.isFile(p) && p.toString().endsWith(".avro"))
                    paths.add(p);
            }
        }
        LOG.debug("getAllAvroPaths returns {}", paths);
        return paths;
    }

    /**
     * Returns schema path from a parent path.
     *
     * @param fs a <code>FileSystem</code> reference.
     * @param parentPath a parent path.
     * @return schema path.
     * @throws IOException
     */
    public static Path getSchemaPath(final FileSystem fs, final Path parentPath) throws IOException{
        final RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(parentPath, true);
        while (iterator.hasNext()) {
            final LocatedFileStatus lfs = iterator.next();
            if (lfs.isFile() && lfs.getPath().toString().endsWith(".avsc")) {
                LOG.debug("getSchemaPath returns {}", lfs.getPath());
                return lfs.getPath();
            }
        }
        throw new IOException("Could not find schema path.");
    }

    /**
     * Returns all properties paths from an array of parent paths.
     *
     * @param fs a <code>FileSystem</code> reference.
     * @param pathArray parent paths array.
     * @return  all properties paths.
     * @throws IOException
     */
    public static SortedSet<Path> getAllPropertiesPaths(final FileSystem fs, final Path[] pathArray) throws IOException {
        final SortedSet<Path> paths = new TreeSet<Path>();
        for(Path p : pathArray) {
            if (fs.isDirectory(p)) {
                final RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(p, true);
                while (iterator.hasNext()) {
                    final LocatedFileStatus lfs = iterator.next();
                    if (lfs.isFile() && lfs.getPath().toString().endsWith(".properties"))
                        paths.add(lfs.getPath());
                }

            } else {
                if (fs.isFile(p) && p.toString().endsWith(".properties"))
                    paths.add(p);
            }
        }
        LOG.debug("getAllPropertiesPaths returns {}", paths);
        return paths;
    }

    /**
     * Dataset Record Reader class.
     */
    public static class DatasetRecordReader implements Iterator<GenericRecord>, Closeable {
        private static final Logger LOG = LoggerFactory.getLogger(DatasetRecordReader.class);
        private List<FileReader<GenericRecord>> fileReaders;  // file readers as many it different files it can detect
        private int current; // current reader

        public DatasetRecordReader(final FileSystem fs, final Schema schema, final Path parentPath)
                throws IOException {
            final SortedSet<Path> paths =
                    fs.isDirectory(parentPath) ? getPathsRecurcively(fs,parentPath) :
                            new TreeSet<Path>(Arrays.asList(new Path[]{fs.makeQualified(parentPath)}));
            int count = paths.size();
            LOG.debug(String.format("Found %d files to read [FileSystem=%s,Path=%s]",
                    count, fsIsLocal(fs) ? "local" : fs.getUri(), parentPath));
            fileReaders = new ArrayList<FileReader<GenericRecord>>();
            for (Path p : paths) {
                final long len = fs.getFileStatus(p).getLen();
                LOG.debug("Creating reader for file {} (len : {})", p, len);
                fileReaders.add(DataFileReader.openReader(
                        new AvroFSInput(fs.open(p), len), new GenericDatumReader<GenericRecord>(schema)));
            }
            current = 0;
        }

        public DatasetRecordReader(final FileSystem fs, final Schema schema, final Path... avroPaths)
                throws IOException {
            final SortedSet<Path> paths = new TreeSet<Path>();
            for (Path avroPath : avroPaths) {
                if(fs.isDirectory(avroPath))
                    paths.addAll(getPathsRecurcively(fs,avroPath));
                else
                    paths.add(fs.makeQualified(avroPath));
            }
            int count = paths.size();
            LOG.debug(String.format("Found %d files to read [FileSystem=%s,Paths=%s]",
                    count, fsIsLocal(fs) ? "local" : fs.getUri(), Arrays.toString(avroPaths)));
            fileReaders = new ArrayList<FileReader<GenericRecord>>();
            for (Path p : paths) {
                final long len = fs.getFileStatus(p).getLen();
                LOG.debug("Creating reader for file {} (len : {})", p, len);
                fileReaders.add(DataFileReader.openReader(
                        new AvroFSInput(fs.open(p), len), new GenericDatumReader<GenericRecord>(schema)));
            }
            current = 0;
        }


        public void close() throws IOException {
            if (current < fileReaders.size())
                fileReaders.get(current).close();
            for (FileReader f : fileReaders)
                if (f != null) f.close();
        }

        public boolean hasNext() {
            if (fileReaders.get(current).hasNext()) return true;
            if (current < fileReaders.size()) {
                try {
                    fileReaders.get(current).close();
                } catch (IOException e) {
                    LOG.error("Can't close this!");
                }
                LOG.debug("Moving readers {} -> {}", current, current + 1);
                LOG.debug("Before : {}", fileReaders.get(current));
                current++;
                if (current >= fileReaders.size()) return false;
                LOG.debug("After current : {} hasNext ? {} ", fileReaders.get(current),
                        fileReaders.get(current).hasNext());
                return fileReaders.get(current).hasNext();
            }
            return false;
        }

        /**
         * Returns next record. Readers open/read/close in a and sorted sequential order.
         * @return next record.
         */
        public GenericRecord next() {
            LOG.debug("Current reader : {}", current);
            return fileReaders.get(current).next();
        }

        public void remove() {
            throw new UnsupportedOperationException("Not supported");
        }
    }

    /**
     * Returns a sample (random selection) from the records array.
     *
     * @param records generic avro records array.
     * @param sampleSize sample size;
     * @return a sample from the records array.
     */
    public static GenericRecord[] sampleDataset(final GenericRecord[] records,final int sampleSize) {
        LOG.debug(String.format("Sampling from records [size=%d]", sampleSize));
        final SecureRandom RANDOM = new SecureRandom();
        int i = 0;
        int sampled = 0;
        final GenericRecord[] sample = new GenericRecord[sampleSize];
        do{
            if(RANDOM.nextBoolean()) {
                sample[sampled] = records[i];
                sampled++;
            }
            i++;
            i = (i == records.length) ? 0 : i;
        }while(sampled < sampleSize);
        return sample;
    }

    /**
     * Update schema with a Unique Long IDentifier.
     *
     * @param schema an avro schema.
     * @param fieldName field name for the ULID field.
     * @return an updated schema.
     */
    public static Schema updateSchemaWithULID(final Schema schema, final String fieldName) {
        Schema newSchema = Schema.createRecord(
                schema.getName(),schema.getDoc(),schema.getNamespace(),schema.isError());
        final Schema.Field field = new Schema.Field(fieldName,Schema.create(Schema.Type.LONG),
                "Unique Long IDentifier",null, Schema.Field.Order.ASCENDING);
        List<Schema.Field> newFields = new ArrayList<Schema.Field>();
        newFields.add(field);
        for(Schema.Field f : schema.getFields())
            newFields.add(new Schema.Field(f.name(),f.schema(),f.doc(),f.defaultValue(),f.order()));
        newSchema.setFields(newFields);
        return newSchema;
    }

    /**
     * Update records with a Unique Long IDentifier.
     *
     * @param records avro records array.
     * @param newSchema the updated schema.
     * @param fieldName field name for the ULID field.
     * @return updated records array.
     */
    public static GenericRecord[] updateRecordsWithULID(final GenericRecord[] records, final Schema newSchema,
                                                        final String fieldName) {
        return updateRecordsWithULID(records, newSchema, fieldName, 0);
    }

    /**
     * Update records with a Unique Long IDentifier.
     *
     * @param records avro records array.
     * @param newSchema the updated schema.
     * @param fieldName field name for the ULID field.
     * @param start base of the ULID.
     * @return updated records array with ULID field.
     */
    public static GenericRecord[] updateRecordsWithULID(final GenericRecord[] records, final Schema newSchema,
                                                        final String fieldName, final long start) {
        long ulid = start;
        final GenericRecord[] newRecords = new GenericRecord[records.length];
        for (int i = 0 ; i < records.length ; i++) {
            LOG.debug("{} = {}",fieldName,ulid);
            newRecords[i] = new GenericData.Record(newSchema);
            newRecords[i].put(fieldName, ulid);
            for (String f : fieldNames(records[i].getSchema()))
                newRecords[i].put(f, records[i].get(f));
            ulid++;
        }
        return newRecords;
    }

    /**
     * Update ordering of fields in a schema.
     *
     * @param schema an avro schema.
     * @param fieldNames sequence of field names to order by (order by fieldname1 then by fieldname2...).
     * @return updated schema.
     */
    public static Schema updateSchemaWithOrderByFields(final Schema schema, final String[] fieldNames) {
        final Schema.Field.Order[] orders = new Schema.Field.Order[fieldNames.length];
        Arrays.fill(orders,Schema.Field.Order.ASCENDING);
        return updateSchemaWithOrderByFields(schema,fieldNames,orders);
    }

    /**
     * Update ordering of fields in a schema.
     *
     * @param schema an avro schema.
     * @param fieldNames sequence of field names to order by (order by fieldname1 then by fieldname2...).
     * @param orders schema field order array.
     * @return updated schema.
     */
    public static Schema updateSchemaWithOrderByFields(final Schema schema, final String[] fieldNames,
                                                       Schema.Field.Order[] orders) {
        assert orders.length == fieldNames.length;
        final Schema newSchema = Schema.createRecord(
                schema.getName(),schema.getDoc(),schema.getNamespace(),schema.isError());

        List<Schema.Field> fields = schema.getFields();
        Schema.Field[] selectedFields = new Schema.Field[fieldNames.length];
        List<Schema.Field> otherFields = new ArrayList<Schema.Field>();
        for (Schema.Field f : fields) {
            if(Arrays.asList(fieldNames).contains(f.name())) {
                int index = Arrays.asList(fieldNames).indexOf(f.name());
                assert index < fieldNames.length;
                selectedFields[index] = new Schema.Field(f.name(),
                        f.schema(),f.doc(),f.defaultValue(), orders[index]);
            }
            else otherFields.add(
                    new Schema.Field(f.name(),
                            f.schema(),f.doc(),f.defaultValue(),f.order()));
        }
        List<Schema.Field> newFields = new ArrayList<Schema.Field>();

        Collections.addAll(newFields, selectedFields);
        for(Schema.Field f : otherFields)
            newFields.add(f);
        newSchema.setFields(newFields);
        return newSchema;
    }

    /**
     * Update ordering of records using an updated schema.
     *
     * @param records avro records array.
     * @param newSchema the updated schema.
     * @return avro records array with updated ordering.
     */
    public static GenericRecord[] updateRecordsWithOrderByFields(final GenericRecord[] records,
                                                                 final Schema newSchema) {
        AvroKeyComparator<GenericRecord> comparator = new AvroKeyComparator<GenericRecord>();
        if (comparator.getConf() == null) {
            final Configuration conf = new Configuration();
            conf.set("avro.serialization.key.writer.schema", newSchema.toString());
            comparator.setConf(conf);
        }

        final Set<AvroKey<GenericRecord>> avroKeys = new TreeSet<AvroKey<GenericRecord>>(comparator);
        for (GenericRecord record : records) {
            GenericRecord updatedRecord = new GenericData.Record(newSchema);
            for (Schema.Field f : record.getSchema().getFields())
                updatedRecord.put(f.name(), record.get(f.name()));
            avroKeys.add(new AvroKey<GenericRecord>(updatedRecord));
        }
        assert avroKeys.size() == records.length;
        final GenericRecord[] updatedRecords = new GenericRecord[records.length];
        int i = 0;
        for (AvroKey<GenericRecord> key : avroKeys ) updatedRecords[i++] = key.datum();
        return updatedRecords;
    }
}
