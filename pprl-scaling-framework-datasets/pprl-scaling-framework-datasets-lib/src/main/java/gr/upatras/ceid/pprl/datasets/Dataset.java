package gr.upatras.ceid.pprl.datasets;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class Dataset {

    private static final Logger LOG = LoggerFactory.getLogger(Dataset.class);
    private static final FsPermission ONLY_OWNER_PERMISSION
            = new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE, false);

    private String name;
    private Path basePath;
    private Path avroPath;
    private Path avroSchemaPath;

    public Dataset(final String name, final Path userHomeDirectory) {
        this.name = name;
        basePath = new Path(userHomeDirectory + "/" + name);
        avroPath = new Path(basePath + "/avro");
        avroSchemaPath = new Path(basePath + "/schema");
    }

    public Dataset(final String name, final Path basePath, final Path avroPath, final Path avroSchemaPath) {
        this.name = name;
        this.basePath = basePath;
        this.avroPath = avroPath;
        this.avroSchemaPath = avroSchemaPath;
    }

    public Path getBasePath() {
        return basePath;
    }

    public void setBasePath(final Path basePath) {
        this.basePath = basePath;
    }

    public Path getAvroPath() {
        return avroPath;
    }

    public void setAvroPath(final Path avroPath) {
        this.avroPath = avroPath;
    }

    public Path getAvroSchemaPath() {
        return avroSchemaPath;
    }

    public void setAvroSchemaPath(final Path avroSchemaPath) {
        this.avroSchemaPath = avroSchemaPath;
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
        return String.format("[%s][%s,%s,%s]",
                name,basePath,avroPath,avroSchemaPath);
    }
}