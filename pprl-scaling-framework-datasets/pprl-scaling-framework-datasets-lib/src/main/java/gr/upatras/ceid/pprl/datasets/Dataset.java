package gr.upatras.ceid.pprl.datasets;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.file.SeekableFileInput;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

public class Dataset {

    private static final Logger LOG = LoggerFactory.getLogger(Dataset.class);
    private static final FsPermission ONLY_OWNER_PERMISSION
            = new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE, false);

    private String name;
    private Path basePath;
    private Path avroPath;
    private Path avroSchemaPath;

    private Schema schema;
    private List<FileReader<GenericRecord>> fileReaders;
    private int selectedFR;

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

    public void setName(final String name) {
        this.name = name;
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

    public void buildOnFS(final FileSystem fs)
            throws IOException, DatasetException {
        buildOnFS(fs,true);
    }

    public void buildOnFS(final FileSystem fs, final boolean makeSubDirs )
            throws IOException, DatasetException {
        buildOnFS(fs,makeSubDirs,makeSubDirs,true);
    }

    public void buildOnFS(final FileSystem fs, final boolean makeAvroDir,
                          final boolean makeSchemaDir , final boolean overwrite)
            throws IOException, DatasetException{

        boolean datasetExists = existsOnFS(fs,true);
        if (datasetExists && overwrite) {
            LOG.info("Overwriting dataset found at {}",name,basePath);
            fs.delete(basePath, true);
        } else if(datasetExists) {
            throw new DatasetException("Dataset base path already exists!");
        }

        fs.mkdirs(basePath, ONLY_OWNER_PERMISSION);
        LOG.info("Making base path at {} created with permissions {}.",
                basePath, ONLY_OWNER_PERMISSION);

        if (makeAvroDir) {
            fs.mkdirs(avroPath, ONLY_OWNER_PERMISSION);
            LOG.info("Making data path at {} created with permissions {}.",
                    avroPath, ONLY_OWNER_PERMISSION);
        }

        if (makeSchemaDir) {
            fs.mkdirs(avroSchemaPath, ONLY_OWNER_PERMISSION);
            LOG.info("Making schema path at {} created with permissions {}.",
                    avroSchemaPath, ONLY_OWNER_PERMISSION);
        }
    }

    public boolean existsOnFS(final FileSystem fs, final boolean checkOnlyBasePath)
            throws IOException {
        if (checkOnlyBasePath) return fs.exists(basePath);
        else return fs.exists(basePath) &&
                fs.exists(avroPath) &&
                fs.exists(avroSchemaPath);
    }

    @Override
    public String toString() {
        return String.format("%s => %s %s %s",
                name,basePath,avroPath,avroSchemaPath);
    }

    public static Dataset fromString(final String s) {
        final String[] parts = s.split(" => ");
        final String name = parts[0];
        final String[] partss = parts[1].split(" ");
        final Path basePath = new Path(partss[0]);
        final Path avroPath = new Path(partss[1]);
        final Path avroSchemaPath = new Path(partss[2]);
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

    public Schema getSchema(final FileSystem fs) throws IOException, DatasetException {
        if(schema == null)  {
            FileStatus[] status = fs.listStatus(avroSchemaPath);
            if(status.length != 1) throw new DatasetException("Schema path must contain only one schema file");
            if(!status[0].isFile()) throw new DatasetException("Schema path must contain only one schema file");
            schema = (new Schema.Parser()).parse(fs.open(status[0].getPath()));
        }
        return schema;
    }

    public FileReader<GenericRecord> getReader(final FileSystem fs) throws IOException, DatasetException {
        if(fileReaders == null) {
            fileReaders = new ArrayList<FileReader<GenericRecord>>();
            final SortedSet<Path> paths = new TreeSet<Path>();
            for (FileStatus s : fs.listStatus(avroPath))
                if(s.isFile() && s.getPath().toString().endsWith(".avro")) paths.add(s.getPath());

            LOG.debug("Found {} files",paths.size());
            for(Path p : paths) {
                LOG.debug("Found File {}",p);
                fileReaders.add(DataFileReader.openReader(
                        new FsInput(p, fs.getConf()), new GenericDatumReader<GenericRecord>(getSchema(fs))));
            }


            selectedFR = 0;
            return fileReaders.get(selectedFR);
        }
        if(selectedFR >= fileReaders.size()) throw new DatasetException("Run out of readers");

        return fileReaders.get(selectedFR);
    }

    public GenericRecord getNextRecord() throws DatasetException, IOException {
        // TODO write test for multiple file case
        if(fileReaders == null) throw new DatasetException("Run out of readers");
        if(selectedFR >= fileReaders.size()) throw new DatasetException("Run out of readers");

        if(fileReaders.get(selectedFR).iterator().hasNext())
            return fileReaders.get(selectedFR).iterator().next();
        else
            fileReaders.get(selectedFR).close();

        selectedFR++;
        if(selectedFR >= fileReaders.size()) return null;

        return fileReaders.get(selectedFR).iterator().next();
    }
}