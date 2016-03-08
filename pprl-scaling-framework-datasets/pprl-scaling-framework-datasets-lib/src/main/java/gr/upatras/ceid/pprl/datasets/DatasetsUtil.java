package gr.upatras.ceid.pprl.datasets;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

public class DatasetsUtil {

    private static final Logger LOG = LoggerFactory.getLogger(DatasetsUtil.class);

    private static boolean fsIsLocal(final FileSystem fs) {
        return fs.getScheme().equals("file");
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
                                    final String[] fieldNames, final Schema.Type[] fieldTypes) {
        Schema schema = Schema.createRecord(schemaName,"",
                "gr.upatas.ceid.pprl.datasets."+schemaName.toLowerCase(),false);
        assert fieldNames.length == fieldTypes.length;
        List<Schema.Field> fields = new ArrayList<Schema.Field>();
        for (int i = 0; i < fieldNames.length; i++)
            fields.add(new Schema.Field(fieldNames[i], Schema.create(fieldTypes[i]),"",null));
        schema.setFields(fields);
        return schema;
    }

    public static String[] fieldNames(final Schema schema) {
        final String[] fieldNames = new String[schema.getFields().size()];
        int i = 0;
        for(Schema.Field f : schema.getFields()) fieldNames[i++] = f.name();
        return fieldNames;
    }

    public static void csv2avro(final FileSystem fs, final Schema schema , final Path csvPath)
            throws DatasetException, IOException {
        final Path p = new Path(csvPath.getParent() + "/" + schema.getName());
        fs.mkdirs(p);
        LOG.debug(String.format("Making directory [FileSystem=%s,Path=%s]",
                fsIsLocal(fs) ? "local" : fs.getUri(), p));
        fs.mkdirs(new Path(p + "/schema"));
        LOG.debug(String.format("Making directory [FileSystem=%s,Path=%s]",
                fsIsLocal(fs) ? "local" : fs.getUri(), p + "/schema"));
        fs.mkdirs(new Path(p + "/avro"));
        LOG.debug(String.format("Making directory [FileSystem=%s,Path=%s]",
                fsIsLocal(fs) ? "local" : fs.getUri(), p + "/avro"));


        final Path schemaPath = new Path(String.format("%s/schema/%s.avsc",
                csvPath.getParent() + "/" + schema.getName(),
                schema.getName()));
        saveSchemaToFSPath(fs, schema, schemaPath);
        final Path avroPath = new Path(String.format("%s/avro/%s.avro",
                csvPath.getParent() + "/" + schema.getName(),
                schema.getName()));

        DataFileWriter<GenericRecord> writer = new DataFileWriter<GenericRecord>(
                new GenericDatumWriter<GenericRecord>(schema));
        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(csvPath)));
        try {

            writer.create(schema, fs.create(avroPath, true));

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
                writer.append(record);
            }
            writer.close();
            reader.close();
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
            LOG.debug("#Writers=",partitions);
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
                    fw.create(schema,fs.create(filePath));
                    fileWriters.add(i,fw);
                    LOG.debug("Adding writer {}",i);
                } catch (IOException e) {
                    fw.close();
                    if(!fileWriters.isEmpty()) close();
                }
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

    public static class DatasetRecordReader implements Iterator<GenericRecord>, Closeable {
        private static final Logger LOG = LoggerFactory.getLogger(DatasetRecordReader.class);
        private List<FileReader<GenericRecord>> fileReaders;
        private int current;

        private static SortedSet<Path> getPathsRecurcively(final FileSystem fs, final Path parentPath) throws IOException {
            final SortedSet<Path> paths = new TreeSet<Path>();
            final RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(parentPath, true);
            while(iterator.hasNext()) {
                final LocatedFileStatus lfs = iterator.next();
                if (lfs.isFile() && lfs.getPath().toString().endsWith(".avro"))
                    paths.add(lfs.getPath());
            }
            return paths;
        }

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
                LOG.debug("Creating reader for file {}", p);
                fileReaders.add(DataFileReader.openReader(
                        new FsInput(p, fs.getConf()), new GenericDatumReader<GenericRecord>(schema)));
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
                LOG.debug("Creating reader for file {}", p);
                fileReaders.add(DataFileReader.openReader(
                        new FsInput(p, fs.getConf()), new GenericDatumReader<GenericRecord>(schema)));
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
}
