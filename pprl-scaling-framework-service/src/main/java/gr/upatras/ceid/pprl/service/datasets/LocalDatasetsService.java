package gr.upatras.ceid.pprl.service.datasets;

import gr.upatras.ceid.pprl.datasets.DatasetException;
import gr.upatras.ceid.pprl.datasets.DatasetStatistics;
import gr.upatras.ceid.pprl.datasets.DatasetsUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

/**
 * Local Datasets Service class.
 */
@Service
public class LocalDatasetsService implements InitializingBean {

    private static final Logger LOG = LoggerFactory.getLogger(LocalDatasetsService.class);

    public void afterPropertiesSet() {
        LOG.info("Local Dataset service initialized.");
    }

    public static final FsPermission ONLY_OWNER_PERMISSION
            = new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE, false);


    @Autowired
    private FileSystem localFs;

    public FileSystem getLocalFs() {
        return localFs;
    }

    public void setLocalFS(FileSystem localFs) {
        this.localFs = localFs;
    }

    /**
     * Retrieve and returns directories of a datase on the local working directory.
     *
     * @param name dataset name.
     * @return paths used by the dataset.
     * @throws IOException
     */
    public Path[] retrieveDirectories(final String name) throws IOException {
        return retrieveDirectories(name,localFs.getWorkingDirectory());
    }

    /**
     * Retrieve and returns directories of a dataset.
     *
     * @param name dataset name.
     * @param basePath base path (pprl-site).
     * @return paths used by the dataset.
     * @throws IOException
     */
    public Path[] retrieveDirectories(final String name,final Path basePath)
            throws IOException {
        try {
            LOG.info("Retrieving directories of {} name from {}.",name,basePath);
            return DatasetsUtil.retrieveDatasetDirectories(localFs,name,basePath);
        } catch (IOException e) {
            LOG.error(e.getMessage());
            throw e;
        }
    }

    /**
     * Create Dataset Directories on the local working directory.
     *
     * @param name dataset name.
     * @return paths created by the dataset name.
     * @throws IOException
     */
    public Path[] createDirectories(final String name)
            throws IOException {
        return createDirectories(name,localFs.getWorkingDirectory(),ONLY_OWNER_PERMISSION);
    }

    /**
     * Create Dataset Directories.
     *
     * @param name dataset name.
     * @param basePath base path.
     * @return paths created by the dataset name.
     * @throws IOException
     */
    public Path[] createDirectories(final String name,final Path basePath)
            throws IOException {
        LOG.info("Create directories of {} name from {}.",name,basePath);
        return DatasetsUtil.createDatasetDirectories(localFs, name, basePath, ONLY_OWNER_PERMISSION);
    }

    /**
     * Create Dataset Directories on the local working directory.
     *
     * @param name dataset name.
     * @param permission permission.
     * @return paths created by the dataset name.
     * @throws IOException
     */
    public Path[] createDirectories(final String name, final FsPermission permission)
            throws IOException {
        return createDirectories(name,localFs.getWorkingDirectory(),permission);
    }

    /**
     * Create Dataset Directories.
     *
     * @param name dataset name.
     * @param basePath base path.
     * @param permission permission.
     * @return paths created by the dataset name.
     * @throws IOException
     */
    public Path[] createDirectories(final String name,final Path basePath, final FsPermission permission)
            throws IOException {
        LOG.info("Create directories of {} name from {}.",name,basePath);
        return DatasetsUtil.createDatasetDirectories(localFs, name, basePath, permission);
    }

    /**
     * Load dataset records.
     *
     * @param avroPaths paths of avro files.
     * @param schemaPath path of schema file.
     * @return generic avro record array.
     * @throws DatasetException
     * @throws IOException
     */
    public GenericRecord[] loadDatasetRecords(final Path[] avroPaths,
                                              final Path schemaPath)
            throws DatasetException, IOException {
        try{
            final Schema schema = DatasetsUtil.loadSchemaFromFSPath(localFs, schemaPath);
            return loadDatasetRecords(avroPaths,schema);
        } catch (DatasetException e) {
            LOG.error(e.getMessage());
            throw e;
        } catch (IOException e) {
            LOG.error(e.getMessage());
            throw e;
        }
    }

    /**
     * Load dataset records.
     *
     * @param avroPaths paths of avro files.
     * @param schema schema.
     * @return generic avro record array.
     * @throws IOException
     */
    public GenericRecord[] loadDatasetRecords(final Path[] avroPaths,
                                              final Schema schema)
            throws IOException {
        try {
            LOG.info("Loading records from {}.", Arrays.toString(avroPaths));
            return DatasetsUtil.loadAvroRecordsFromFSPaths(localFs,schema,avroPaths);
        } catch (IOException e) {
            LOG.error(e.getMessage());
            throw e;
        }
    }

    /**
     * Save dataset records.
     *
     * @param name a name.
     * @param records generic avro record array.
     * @param schema  schema.
     * @throws DatasetException
     * @throws IOException
     */
    public void saveDatasetRecords(final String name,
                                   final GenericRecord[] records, final Schema schema)
            throws DatasetException, IOException {
        saveDatasetRecords(name, records, schema, localFs.getWorkingDirectory());
    }

    /**
     * Save dataset records.
     *
     * @param name a name.
     * @param records generic avro record array.
     * @param schema  schema.
     * @param partitions count of partitions (avro files).
     * @throws DatasetException
     * @throws IOException
     */
    public void saveDatasetRecords(final String name,
                                   final GenericRecord[] records, final Schema schema,
                                   final int partitions)
            throws DatasetException, IOException {
        saveDatasetRecords(name, records, schema, localFs.getWorkingDirectory(), partitions);
    }

    /**
     * Save dataset records.
     *
     * @param name a name.
     * @param records generic avro record array.
     * @param schema  schema.
     * @param basePath a base path.
     * @throws DatasetException
     * @throws IOException
     */
    public void saveDatasetRecords(final String name,
                                   final GenericRecord[] records, final Schema schema,
                                   final Path basePath)
            throws DatasetException, IOException {
        saveDatasetRecords(name, records, schema, basePath, 1);
    }

    /**
     * Save dataset records.
     *
     * @param name a name.
     * @param records generic avro record array.
     * @param schema  schema.
     * @param basePath a base path.
     * @param partitions count of partitions (avro files).
     * @throws DatasetException
     * @throws IOException
     */
    public void saveDatasetRecords(final String name,
                                   final GenericRecord[] records, final Schema schema,
                                   final Path basePath,
                                   final int partitions)
            throws DatasetException, IOException {
        try {
            LOG.info("Saving records to {} at {}.",name,basePath);
            DatasetsUtil.saveAvroRecordsToFSPath(localFs,records,schema,basePath,name,partitions);
        } catch (IOException e) {
            LOG.error(e.getMessage());
            throw e;
        }
    }

    /**
     * Load schema.
     *
     * @param name a name.
     * @param basePath base path.
     * @return a <code>Schema</code> instance.
     * @throws IOException
     * @throws DatasetException
     */
    public Schema loadSchema(final String name , final Path basePath)
            throws IOException, DatasetException {
        return loadSchema(new Path(basePath,String.format("%s.avsc",name)));
    }

    /**
     * Load schema.
     *
     * @param schemaPath schema path.
     * @return a <code>Schema</code> instance.
     * @throws IOException
     * @throws DatasetException
     */
    public Schema loadSchema(final Path schemaPath)
            throws IOException, DatasetException {
        try {
            LOG.info("Loading schema from {}.",schemaPath);
            return DatasetsUtil.loadSchemaFromFSPath(localFs,schemaPath);
        } catch (DatasetException e) {
            LOG.error(e.getMessage());
            throw e;
        } catch (IOException e) {
            LOG.error(e.getMessage());
            throw e;
        }
    }

    /**
     * Load schema.
     *
     * @param schemaPath schema path.
     * @param schema a <code>Schema</code> instance.
     * @throws IOException
     * @throws DatasetException
     */
    public void saveSchema(final Path schemaPath, final Schema schema)
            throws IOException, DatasetException {
        try {
            LOG.info("Saving schema to {}.",schemaPath);
            DatasetsUtil.saveSchemaToFSPath(localFs, schema, schemaPath);
        } catch (DatasetException e) {
            LOG.error(e.getMessage());
            throw e;
        } catch (IOException e) {
            LOG.error(e.getMessage());
            throw e;
        }
    }

    /**
     * Load schema.
     *
     * @param name a name.
     * @param basePath base path.
     * @param schema a <code>Schema</code> instance.
     * @throws IOException
     * @throws DatasetException
     */
    public void saveSchema(final String name, final Path basePath, final Schema schema)
            throws IOException, DatasetException {
        saveSchema(new Path(basePath,String.format("%s.avsc",name)),schema);
    }

    /**
     * Save dataset statistics
     *
     * @param name a name.
     * @param statistics a <code>DatasetStatistics</code> instance.
     * @return path statistics are saved.
     * @throws IOException
     * @throws DatasetException
     */
    public Path saveStats(final String name,
                          final DatasetStatistics statistics)
            throws IOException, DatasetException {
        return saveStats(name, statistics, localFs.getWorkingDirectory());
    }

    /**
     * Save dataset statistics
     *
     * @param name a name.
     * @param statistics a <code>DatasetStatistics</code> instance.
     * @param basePath a base path.
     * @return path statistics are saved.
     * @throws IOException
     * @throws DatasetException
     */
    public Path saveStats(final String name,
                          final DatasetStatistics statistics,
                          final Path basePath)
            throws IOException, DatasetException {
        try {
            if(!localFs.exists(basePath) && !localFs.mkdirs(basePath,ONLY_OWNER_PERMISSION))
                throw new DatasetException(String.format("Cannot create dir \"%s\"", basePath));
            final Path path = new Path(basePath,name + ".properties");
            LOG.info("Saving stats to {}.",path);
            FSDataOutputStream fsdos = localFs.create(path);
            statistics.toProperties().store(fsdos, "Statistics");
            fsdos.close();
            return path;
        } catch (IOException e) {
            LOG.error(e.getMessage());
            throw e;
        } catch (DatasetException e) {
            LOG.error(e.getMessage());
            throw e;
        }
    }

    /**
     * Load statistics from paths.
     *
     * @param propertiesPaths property file paths.
     * @return a <code>DatasetStatistics</code> instance.
     * @throws IOException
     * @throws DatasetException
     */
    public DatasetStatistics loadStats(final Path... propertiesPaths)
            throws IOException, DatasetException {
        try {
            Properties properties = new Properties();
            for (Path propertiesPath : propertiesPaths) {
                if (!localFs.exists(propertiesPath))
                    throw new DatasetException(String.format("Cannot find file \"%s\"", propertiesPath));
                LOG.info("Loading stats from {}.",propertiesPath);
                FSDataInputStream fsdis = localFs.open(propertiesPath);
                properties.load(fsdis);
                fsdis.close();
            }
            DatasetStatistics statistics = new DatasetStatistics();
            statistics.fromProperties(properties);
            return statistics;
        } catch (IOException e) {
            LOG.error(e.getMessage());
            throw e;
        } catch (DatasetException e) {
            LOG.error(e.getMessage());
            throw e;
        }
    }

    /**
     * Sample a dataset.
     *
     * @param avroPaths avro paths.
     * @param schemaPath schema path.
     * @param sampleSize count of records to be sampled.
     * @return a sample records array of a dataset.
     * @throws IOException
     * @throws DatasetException
     */
    public GenericRecord[] sampleDataset(final Path[] avroPaths,
                                         final Path schemaPath,
                                         final int sampleSize)
            throws IOException, DatasetException {
        try {
            LOG.info("Sampling {} records from {}.",sampleSize, Arrays.toString(avroPaths));
            final Schema schema = DatasetsUtil.loadSchemaFromFSPath(localFs, schemaPath);
            final GenericRecord[] records = DatasetsUtil.loadAvroRecordsFromFSPaths(localFs,schema,avroPaths);
            return DatasetsUtil.sampleDataset(records,sampleSize);
        } catch (DatasetException e) {
            LOG.error(e.getMessage());
            throw e;
        } catch (IOException e) {
            LOG.error(e.getMessage());
            throw e;
        }
    }

    /**
     * Sample a dataset.
     *
     * @param avroPaths avro paths.
     * @param schema schema.
     * @param sampleSize count of records to be sampled.
     * @return a sample records array of a dataset.
     * @throws IOException
     * @throws DatasetException
     */
    public GenericRecord[] sampleDataset(final Path[] avroPaths,
                                         final Schema schema,
                                         final int sampleSize)
            throws IOException, DatasetException {
        try {
            LOG.info("Sampling {} records from {}.",sampleSize, Arrays.toString(avroPaths));
            final GenericRecord[] records = DatasetsUtil.loadAvroRecordsFromFSPaths(localFs,schema,avroPaths);
            return DatasetsUtil.sampleDataset(records,sampleSize);
        } catch (IOException e) {
            LOG.error(e.getMessage());
            throw e;
        }
    }

    /**
     * Sort dataset records.
     *
     * @param records records.
     * @param schema schema.
     * @param addUid if true add ULID field.
     * @param uidFieldName uid field name.
     * @return updated dataset records.
     * @throws DatasetException
     * @throws IOException
     */
    public GenericRecord[] sortDatasetRecords(final GenericRecord[] records,
                                              final Schema schema,
                                              final boolean addUid,
                                              final String uidFieldName,
                                              final String... orderByFieldNames)
            throws DatasetException, IOException {
        if(addUid && uidFieldName == null) throw new IllegalAccessError("Requires a new uid field name.");

        LOG.info("Sorting records.{}.",
                (addUid)?("UID field : " + uidFieldName) : "No adding uid");

        GenericRecord[] sortedRecords =
                DatasetsUtil.updateRecordsWithOrderByFields(records,schema,orderByFieldNames);
        Schema sortedSchema  =
                DatasetsUtil.updateSchemaWithOrderByFields(schema,orderByFieldNames);

        if(addUid) sortedRecords =
                DatasetsUtil.updateRecordsWithUID(sortedRecords,sortedSchema,uidFieldName);
        return sortedRecords;
    }

    /**
     * Update field order in a dataset schema.
     *
     * @param schema schema
     * @param addUid if true add ULID field.
     * @param uidFieldName uidFieldName.
     * @param orderByFieldNames fields to order by (order of fields matters).
     * @return updated dataset schema.
     * @throws DatasetException
     * @throws IOException
     */
    public Schema sortDatasetSchema(final Schema schema,
                                    final boolean addUid,
                                    final String uidFieldName,
                                    final String... orderByFieldNames)
            throws DatasetException, IOException {

        if(addUid && uidFieldName == null) throw new IllegalAccessError("Requires a new uid field name.");

        LOG.info("Updating schema.{}.",
                (addUid)?("UID field : " + uidFieldName) : "No adding uid");

        Schema sortedSchema = DatasetsUtil.updateSchemaWithOrderByFields(schema,orderByFieldNames);

        if(addUid) sortedSchema = DatasetsUtil.updateSchemaWithUID(sortedSchema,uidFieldName);

        return sortedSchema;
    }
}
