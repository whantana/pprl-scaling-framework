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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

public class DatasetsUtil {

    private static final Logger LOG = LoggerFactory.getLogger(DatasetsUtil.class);

    private static boolean fsIsLocal(final FileSystem fs) {
        return fs.getUri().toString().contains("file");
    }

    public static Path[] retrieveDatasetDirectories(final FileSystem fs, final String name, final Path basePath)
            throws IOException {
        return retrieveDatasetDirectories(fs,new Path(basePath,name));
    }

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



    public static void checkIfExists(final FileSystem fs, final Path path)
            throws IOException {
        boolean baseExists = fs.exists(path);
        if(!baseExists) throw new IllegalArgumentException(
                String.format("Path does not exist  [FileSystem=%s,Path=%s]",
                        fsIsLocal(fs) ? "local" : fs.getUri(), path));
    }

    public static Path[] createDatasetDirectories(final FileSystem fs, final String name, final Path basePath)
            throws IOException {
        return createDatasetDirectories(fs,name,basePath,null);
    }

    public static boolean nameBelongsToSchema(final Schema schema, final String... selectedNames) {
        for(String name : selectedNames) {
            boolean nameFound = false;
            for(Schema.Field field : schema.getFields())
                if(field.name().equals(name)) { nameFound = true ; break; }
            if(!nameFound) return false;
        }
        return true;
    }

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

    public static Schema loadSchemaFromFSPath(final FileSystem fs, final Path schemaPath)
            throws DatasetException, IOException {
        LOG.debug(String.format("Loading path [FileSystem=%s,Path=%s]",
                fsIsLocal(fs) ? "local" : fs.getUri(), schemaPath));
        FileStatus fss = fs.getFileStatus(schemaPath);
        if (fss.isFile() && fss.getPath().getName().endsWith(".avsc"))
            return (new Schema.Parser()).parse(fs.open(fss.getPath()));
        else throw new DatasetException("Path provided not a schema file.");
    }

    public static void saveSchemaToFSPath(final FileSystem fs, final Schema schema, final Path schemaPath)
            throws DatasetException, IOException {
        LOG.debug(String.format("Saving path [FileSystem=%s,Path=%s]",
                fsIsLocal(fs) ? "local" : fs.getUri(), schemaPath));
        final FSDataOutputStream fsdos = fs.create(schemaPath, true);
        fsdos.write(schema.toString(true).getBytes());
        fsdos.close();
    }

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

    public static String[] fieldNames(final Schema schema) {
        final String[] fieldNames = new String[schema.getFields().size()];
        int i = 0;
        for(Schema.Field f : schema.getFields()) fieldNames[i++] = f.name();
        return fieldNames;
    }

    public static Path csv2avro(final FileSystem fs, final Schema schema ,
                                final Path basePath,
                                final Path csvPath)
            throws DatasetException, IOException {
        final Path[] paths = createDatasetDirectories(fs,schema.getName(),basePath);
        final Path avroBasePath = paths[1];
        final Path schemaBasePath = paths[2];

        saveSchemaToFSPath(fs, schema, new Path(schemaBasePath,schema.getName()+".avsc"));
        DatasetRecordWriter writer = new DatasetRecordWriter(fs,schema.getName(),schema,avroBasePath);
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

    public static class DatasetRecordWriter implements Closeable {

        private static final Logger LOG = LoggerFactory.getLogger(DatasetRecordWriter.class);
        private List<DataFileWriter<GenericRecord>> fileWriters;
        private int partitions;

        public DatasetRecordWriter(final FileSystem fs, final String name, final Schema schema)
                throws IOException {
            this(fs,name,schema,fs.getWorkingDirectory(),1);
        }

        public DatasetRecordWriter(final FileSystem fs, final String name,
                                   final Schema schema, final Path parentPath)
                throws IOException {
            this(fs,name,schema,parentPath,1);
        }

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

        public void writeRecord(final GenericRecord record) throws IOException {
            try {
                fileWriters.get(0).append(record);
            } catch (IOException e) {
                fileWriters.get(0).close();
            }
        }


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

        private void writeRecord(final GenericRecord record , final int i) throws IOException {
            try {
                fileWriters.get(i).append(record);
            } catch (IOException e) {
                fileWriters.get(i).close();
            }
        }

        public void close() throws IOException {
            for (DataFileWriter fw : fileWriters) fw.close();
        }
    }

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

    public static SortedSet<Path> getAllAvroPaths(final FileSystem fs, final Path[] pathArray) throws IOException {
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

    public static Path getSchemaPath(final FileSystem fs, final Path schemaPath) throws IOException{
        final RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(schemaPath, true);
        while (iterator.hasNext()) {
            final LocatedFileStatus lfs = iterator.next();
            if (lfs.isFile() && lfs.getPath().toString().endsWith(".avsc")) {
                LOG.debug("getSchemaPath returns {}", lfs.getPath());
                return lfs.getPath();
            }
        }
        throw new IOException("Could not find schema path.");
    }

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



    public static class DatasetRecordReader implements Iterator<GenericRecord>, Closeable {
        private static final Logger LOG = LoggerFactory.getLogger(DatasetRecordReader.class);
        private List<FileReader<GenericRecord>> fileReaders;
        private int current;

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

        public DatasetRecordReader(final FileSystem fs, final Schema schema, final Path[] avroPaths)
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

        public GenericRecord next() {
            LOG.debug("Current reader : {}", current);
            return fileReaders.get(current).next();
        }

        public void remove() {
            throw new UnsupportedOperationException("Not supported");
        }
    }

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

    public static GenericRecord[] updateRecordsWithULID(final GenericRecord[] records, final Schema newSchema,
                                                        final String fieldName) {
        return updateRecordsWithULID(records, newSchema, fieldName, 0);
    }

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

    public static Schema updateSchemaWithOrderByFields(final Schema schema, final String[] fieldNames) {
        final Schema.Field.Order[] orders = new Schema.Field.Order[fieldNames.length];
        Arrays.fill(orders,Schema.Field.Order.ASCENDING);
        return updateSchemaWithOrderByFields(schema,fieldNames,orders);
    }

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
