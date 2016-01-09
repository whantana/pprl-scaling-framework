package gr.upatras.ceid.pprl.datasets;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

public class Dataset {

    protected String name;
    protected Path basePath;
    protected Path avroPath;
    protected Path avroSchemaPath;
    protected Schema schema;

    public Dataset(final String name, final Path userHomeDirectory) {
        this(name,
                new Path(userHomeDirectory + "/" + name),
                new Path(userHomeDirectory + "/" + name + "/avro"),
                new Path(userHomeDirectory + "/" + name + "/schema"));
    }

    public Dataset(final String name, final Path basePath, final Path avroPath, final Path avroSchemaPath) {
        this.name = name;
        this.basePath = basePath;
        this.avroPath = avroPath;
        this.avroSchemaPath = avroSchemaPath;
    }

    public String getName() {
        return name;
    }

    public Path getBasePath() {
        return basePath;
    }

    public Path getAvroPath() {
        return avroPath;
    }

    public Path getAvroSchemaPath() {
        return avroSchemaPath;
    }

    public void buildOnFS(final FileSystem fs, FsPermission permission)
            throws IOException, DatasetException {
        buildOnFS(fs,true,permission);
    }

    public void buildOnFS(final FileSystem fs, final boolean makeSubDirs, FsPermission permission )
            throws IOException, DatasetException {
        buildOnFS(fs,makeSubDirs,makeSubDirs,true,permission);
    }

    public void buildOnFS(final FileSystem fs, final boolean makeAvroDir,
                          final boolean makeSchemaDir , final boolean overwrite, FsPermission permission)
            throws IOException, DatasetException{

        boolean datasetExists = existsOnFS(fs,true);
        if (datasetExists && overwrite) {
            fs.delete(basePath, true);
        } else if(datasetExists) {
            throw new DatasetException("Dataset base path already exists!");
        }

        fs.mkdirs(basePath, permission);

        if (makeAvroDir) {
            fs.mkdirs(avroPath, permission);
        }

        if (makeSchemaDir) {
            fs.mkdirs(avroSchemaPath, permission);
        }
    }

    public boolean existsOnFS(final FileSystem fs, final boolean checkOnlyBasePath)
            throws IOException {
        if (checkOnlyBasePath) return fs.exists(basePath);
        else return fs.exists(basePath) &&
                fs.exists(avroPath) &&
                fs.exists(avroSchemaPath);
    }

    public static String toString(final Dataset dataset)
            throws DatasetException {
        if(!dataset.isValid()) throw new DatasetException("Dataset is invalid.");
        return String.format("%s => %s %s %s",
                dataset.getName(), dataset.getBasePath(),
                dataset.getAvroPath(), dataset.getAvroSchemaPath());
    }

    public static Dataset fromString(final String s) throws DatasetException {
        final String[] parts = s.split(" => ");
        if(parts.length != 2) throw new DatasetException("String \"" + s + "\" is invalid dataset string.");
        String name = parts[0];
        final String[] paths = parts[1].split(" ");
        if(paths.length != 3) throw new DatasetException("String \"" + s + "\" is invalid dataset string.");
        Path basePath = new Path(paths[0]);
        Path avroPath = new Path(paths[1]);
        Path avroSchemaPath = new Path(paths[2]);
        return new Dataset(name,basePath,avroPath,avroSchemaPath);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Dataset dataset = (Dataset) o;

        if (!name.equals(dataset.name)) return false;
        if (!basePath.equals(dataset.basePath)) return false;
        if (!avroPath.equals(dataset.avroPath)) return false;
        return avroSchemaPath.equals(dataset.avroSchemaPath);
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + basePath.hashCode();
        result = 31 * result + avroPath.hashCode();
        result = 31 * result + avroSchemaPath.hashCode();
        return result;
    }

    public Schema getSchema(final FileSystem fs)
            throws IOException, DatasetException {
        if(schema == null)  {
            FileStatus[] statuses = fs.listStatus(avroSchemaPath);
            if(statuses.length > 1) {
                for(FileStatus s : statuses) {
                    if(s.isFile() && s.getPath().getName().endsWith(".avsc"))
                        schema = (new Schema.Parser()).parse(fs.open(s.getPath()));
                    if(schema != null) break;
                }
            } else if (statuses.length != 1)
                throw new DatasetException("Empty schema path.");
            FileStatus s = statuses[0];
            if(s.isFile()) schema = (new Schema.Parser()).parse(fs.open(s.getPath()));
        }

        return schema;
    }

    public void writeSchemaOnHdfs(final FileSystem fs) throws IOException {
        if(schema == null) return;
        final Path schemaPath = new Path(avroSchemaPath + "/" + name + ".avsc");
        final FSDataOutputStream fsdos = fs.create(schemaPath, true);
        fsdos.write(schema.toString(true).getBytes());
        fsdos.close();
    }

    public Path getSchemaFile(final FileSystem fs)
            throws IOException, DatasetException {
        FileStatus[] statuses = fs.listStatus(avroSchemaPath);
        if(statuses.length > 1) {
            for(FileStatus s : statuses) {
                if(s.isFile() && s.getPath().getName().endsWith(".avsc")) return s.getPath();
            }
        } else if(statuses.length != 1) throw new DatasetException("Empty schema path.");
        FileStatus s = statuses[0];
        if(s.isFile() && s.getPath().getName().endsWith(".avsc")) return s.getPath();
        else return null;
    }

    public DatasetRecordReader getReader(final FileSystem fs) throws IOException, DatasetException {
        if(schema == null) {
            getSchema(fs);
            if (schema == null) {
                throw new DatasetException("Schema cannot be null");
            }
        }
        return new DatasetRecordReader(fs,schema,avroPath);
    }

    public Path getStatsPath(final int Q) { return new Path(getBasePath() + "/" + String.format("stats_%d",Q)); }

    public Map<String,double[]> getStats(final FileSystem fs, final int Q,final String[] selectedFieldNames)
            throws IOException, DatasetException {

        final Path statsParentPath = getStatsPath(Q);
        if(!fs.exists(statsParentPath))
            throw new DatasetException("Cannot find datasets stats path " + statsParentPath + ".");
        if(fs.listStatus(statsParentPath).length == 0)
            throw new DatasetException("Empty stats path " + statsParentPath + ".");
        final Path statsFile =  fs.listStatus(statsParentPath)[0].getPath();

        SequenceFile.Reader reader = new SequenceFile.Reader(fs.getConf(), SequenceFile.Reader.file(statsFile));
        final Map<String,double[]> stats = new HashMap<String,double[]>();
        Text key = new Text();
        DatasetStatsWritable value = new DatasetStatsWritable();

        while(reader.next(key,value)) {
            if(selectedFieldNames == null ||
                    selectedFieldNames.length == 0 ||
                    Arrays.asList(selectedFieldNames).contains(key.toString()))
                stats.put(key.toString(),value.getStats());
        }
        reader.close();
        return stats;
    }

    public boolean isValid() {
        return (name != null) & (basePath != null) &
                (avroPath != null) & (avroSchemaPath != null);
    }

    public class DatasetRecordReader implements Iterator<GenericRecord>,Closeable {
        private final Logger LOG = LoggerFactory.getLogger(DatasetRecordReader.class);
        private List<FileReader<GenericRecord>> fileReaders;
        private int current;

        public DatasetRecordReader(final FileSystem fs, final Schema schema,final Path parentPath)
                throws IOException {
            final SortedSet<Path> paths = new TreeSet<Path>();
            int count = 0;
            for (FileStatus s : fs.listStatus(parentPath))
                if(s.isFile() && s.getPath().toString().endsWith(".avro")) { count++; paths.add(s.getPath()); }
            LOG.debug("Found {} files to read",count);

            fileReaders = new ArrayList<FileReader<GenericRecord>>();
            for(Path p : paths) {
                LOG.debug("Creating reader for file {}",p);
                fileReaders.add(DataFileReader.openReader(
                        new FsInput(p, fs.getConf()), new GenericDatumReader<GenericRecord>(schema)));
            }
            current = 0;
        }

        public void close() throws IOException {
            if(current < fileReaders.size())
                fileReaders.get(current).close();
            for(FileReader f :fileReaders)
                if(f != null) f.close();
        }

        public boolean hasNext() {
            if(fileReaders.get(current).hasNext()) return true;
            if(current < fileReaders.size()) {
                try {
                    fileReaders.get(current).close();
                } catch (IOException e) {
                    LOG.error("Can't close this!");
                }
                LOG.debug("Moving readers {} -> {}",current,current+1);
                LOG.debug("Before : {}",fileReaders.get(current));
                current++;
                if(current >= fileReaders.size()) return false;
                LOG.debug("After current : {} hasNext ? {} ",fileReaders.get(current),
                        fileReaders.get(current).hasNext());
                return fileReaders.get(current).hasNext();
            }
            return false;
        }

        public GenericRecord next() {
            LOG.debug("Current reader : {}",current);
            return  fileReaders.get(current).next();
        }

        public void remove() {
            throw new UnsupportedOperationException("Not supported");
        }
    }
}