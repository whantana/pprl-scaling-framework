package gr.upatras.ceid.pprl.service.datasets;

import gr.upatras.ceid.pprl.avro.dblp.DblpPublication;
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
import org.springframework.data.hadoop.mapreduce.ToolRunner;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.Properties;
import java.util.SortedSet;

/**
 * Datasets Service class.
 */
@Service
public class DatasetsService implements InitializingBean {
    // TODO logging
    protected static final Logger LOG = LoggerFactory.getLogger(DatasetsService.class);

    public void afterPropertiesSet() {
        try {
            basePath = new Path(hdfs.getHomeDirectory(),"pprl");
            if(!hdfs.exists(basePath)) {
                LOG.info("PPRL Base Path : {}",basePath);
                hdfs.mkdirs(basePath, ONLY_OWNER_PERMISSION);
            }

            boolean onlyOwnerPermissionbaseDir = hdfs.getFileStatus(basePath)
                    .getPermission().equals(ONLY_OWNER_PERMISSION);
            LOG.info(String.format("Dataset service initialized [" +
                            " nn=%s, " +
                            " statsBasePath = %s (ONLY_OWNER_PERMISION = %s)," +
                            " Tool#1 = %s, Tool#2 = %s, Tool#3 = %s]",
                    hdfs.getUri(),
                    basePath,onlyOwnerPermissionbaseDir,
                    (dblpXmlToAvroToolRunner != null),
                    (qGramCountingToolRunner != null),
                    (sortAvroToolRunner != null)));
        } catch (IOException e) {
            LOG.error(e.getMessage());
        }
    }

    /**
     * Permissions
     */
    public static final FsPermission ONLY_OWNER_PERMISSION
            = new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE, false);

    public static final FsPermission OTHERS_CAN_READ_PERMISSION
            = new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.READ, false);

    @Autowired
    private FileSystem hdfs;     // HDFS FileSystem reference

    @Autowired
    private FileSystem localFs;  // Local FileSystem reference

    @Autowired
    private ToolRunner dblpXmlToAvroToolRunner;       // DBLP XML TO AVRO ToolRunner

    @Autowired
    private ToolRunner qGramCountingToolRunner;       // Q-Gram Counting Tool

    @Autowired
    private ToolRunner sortAvroToolRunner;

    private Path basePath;                            // PPRL Base Path on the HDFS (pprl-site).

    /**
     * Set only-owner permission on path.
     *
     * @param path hdfs path
     * @throws IOException
     */
    public void setOnlyOwnerPermission(final Path path) throws IOException {
        try {
            if (!hdfs.exists(path))
                throw new IllegalArgumentException("Path \"" + path + "\" does not exist.");
            hdfs.setPermission(path,ONLY_OWNER_PERMISSION);
        } catch (IOException e) {
            LOG.error(e.getMessage());
            throw e;
        }
    }

    /**
     * Set others-can-read permission on path.
     *
     * @param path hdfs path
     * @throws IOException
     */
    public void setOthersCanReadPermission(final Path path) throws IOException {
        try {
            if (!hdfs.exists(path))
                throw new IllegalArgumentException("Path \"" + path + "\" does not exist.");
            hdfs.setPermission(path,OTHERS_CAN_READ_PERMISSION);
        } catch (IOException e) {
            LOG.error(e.getMessage());
            throw e;
        }
    }

    /**
     * Create Datasets Directories on the HDFS pprl-site.
     *
     * @param name dataset name.
     * @param permission permission.
     * @return paths created by the dataset name.
     * @throws IOException
     */
    public Path[] createDirectories(final String name, final FsPermission permission)
            throws IOException {
        return createDirectories(name,basePath,permission);
    }

    /**
     * Create Datasets Directories on the HDFS pprl-site.
     *
     * @param name dataset name.
     * @param basePath base path (pprl-site).
     * @param permission permission.
     * @return paths created by the dataset name.
     * @throws IOException
     */
    public Path[] createDirectories(final String name,final Path basePath, final FsPermission permission)
            throws IOException {
        return DatasetsUtil.createDatasetDirectories(hdfs, name, basePath, permission);
    }

    /**
     * Retrieve and returns directories of a datase on the HDFS pprl-site.
     *
     * @param name dataset name.
     * @return paths used by the dataset.
     * @throws IOException
     */
    public Path[] retrieveDirectories(final String name) throws IOException {
        return retrieveDirectories(name,basePath);
    }

    /**
     * Retrieve and returns directories of a datase on the HDFS pprl-site.
     *
     * @param name dataset name.
     * @param basePath base path (pprl-site).
     * @return paths used by the dataset.
     * @throws IOException
     */
    public Path[] retrieveDirectories(final String name,final Path basePath)
            throws IOException {
        try {
            return DatasetsUtil.retrieveDatasetDirectories(hdfs,name,basePath);
        } catch (IOException e) {
            LOG.error(e.getMessage());
            throw e;
        }
    }

    /**
     * Retrieve and return path containing schema of a dataset.
     *
     * @param basePath datasets base path.
     * @return datasets schema path.
     * @throws IOException
     */
    public Path retrieveSchemaPath(final Path basePath)
            throws IOException {
        try {
            return DatasetsUtil.getSchemaPath(hdfs,basePath);
        } catch (IOException e) {
            LOG.error(e.getMessage());
            throw e;
        }
    }

    /**
     * Upload files as a new dataset.
     *
     * @param avroPaths datasets avro paths (local).
     * @param schemaPath datasets schema path (local).
     * @param name datasets name.
     * @return new dataset base path.
     * @throws IOException
     */
    public Path uploadFiles(final Path[] avroPaths, final Path schemaPath,final  String name)
            throws IOException {
        return uploadFiles(avroPaths,schemaPath,name,ONLY_OWNER_PERMISSION);
    }

    /**
     * Upload files as a new dataset.
     *
     * @param avroPaths datasets avro paths (local).
     * @param schemaPath datasets schema path (local).
     * @param name datasets name.
     * @param permission permission.
     * @return new dataset base path.
     * @throws IOException
     */
    public Path uploadFiles(final Path[] avroPaths, final Path schemaPath,final  String name,
                            final FsPermission permission)
            throws IOException {
        try {
            final Path[] dataset = DatasetsUtil.createDatasetDirectories(hdfs, name, basePath, permission);
            final Path destBasePath = dataset[0];
            final Path destAvroPath = dataset[1];
            final Path destSchemaPath = dataset[2];

            final SortedSet<Path> paths = DatasetsUtil.getAllAvroPaths(localFs, avroPaths);
            LOG.info("Uploading avro {} files.", paths.size());
            for (Path src : paths) {
                final Path dest = new Path(destAvroPath, src.getName());
                LOG.info("\tUploading {} to {}", src, dest);
                hdfs.copyFromLocalFile(src, dest);
                hdfs.setPermission(dest, permission);
            }

            final Path src = schemaPath;
            final Path dest = new Path(destSchemaPath,schemaPath.getName());
            LOG.info("Uploading {} avro schema file.");
            LOG.info("\tUploading {} to {}", src, dest);
            hdfs.copyFromLocalFile(src, dest);
            return destBasePath;
        } catch (IOException e) {
            LOG.error(e.getMessage());
            throw e;
        }
    }

    /**
     * Download files from a HDFS pprl-site dataset to local file system.
     *
     * @param name datasets name.
     * @param downloadName download dataset name
     * @return downloaded dataset base path.
     * @throws DatasetException
     * @throws IOException
     */
    public Path downloadFiles(final String name,
                              final String downloadName) throws DatasetException, IOException {
        final Path uploadedPath = new Path(basePath, name);
        return downloadFiles(uploadedPath,downloadName,localFs.getWorkingDirectory());
    }

    /**
     * Download files from a HDFS pprl-site dataset to local file system.
     *
     * @param name datasets name.
     * @param downloadName download dataset name.
     * @param parent base path for the downloaded dataset.
     * @return downloaded dataset base path.
     * @throws DatasetException
     * @throws IOException
     */
    public Path downloadFiles(final String name,
                              final String downloadName,
                              final Path parent) throws DatasetException, IOException {
        final Path uploadedPath = new Path(basePath, name);
        return downloadFiles(uploadedPath,downloadName,parent);
    }

    /**
     * Download files from a HDFS pprl-site dataset to local file system.
     *
     * @param uploadedPath a dataset base path uploaded on the HDFS pprl-site.
     * @param downloadName download dataset name.
     * @param parent base path for the downloaded dataset.
     * @return downloaded dataset base path.
     * @throws DatasetException
     * @throws IOException
     */
    public Path downloadFiles(final Path uploadedPath,
                              final String downloadName,
                              final Path parent) throws DatasetException, IOException {
        try{
            final Path[] uploadedPaths = DatasetsUtil.retrieveDatasetDirectories(hdfs,uploadedPath);
            final Path baseAvroPath = uploadedPaths[0];
            final Path srcAvroPath = uploadedPaths[1];
            final Path srcSchemaPath = uploadedPaths[2];
            final Path[] dataset = DatasetsUtil.createDatasetDirectories(localFs,downloadName,parent);
            final Path destBasePath = dataset[0];
            final Path destAvroPath = dataset[1];
            final Path destSchemaPath = dataset[2];
            for(Path src: DatasetsUtil.getAllAvroPaths(hdfs,new Path[]{srcAvroPath})) {
                final Path dest = new Path(destAvroPath, src.getName());
                LOG.info("\tDownloading {} to {}", src, dest);
                hdfs.copyToLocalFile(src, dest);
            }

            {
                final Path src = DatasetsUtil.getSchemaPath(hdfs, srcSchemaPath);
                final Path dest = new Path(destSchemaPath, src.getName());
                LOG.info("Downloading avro schema file.");
                LOG.info("\tDownloading {} to {}", src, dest);
                hdfs.copyToLocalFile(src, dest);
            }

            for(Path src : DatasetsUtil.getAllPropertiesPaths(hdfs,new Path[]{baseAvroPath})) {
                final Path dest = new Path(destBasePath, src.getName());
                LOG.info("\tDownloading {} to {}", src, dest);
                hdfs.copyToLocalFile(src, dest);
            }

            return destBasePath;
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
     * Load a dataset schema from a path.
     *
     * @param schemaPath a schema path.
     * @return a dataset schema.
     * @throws IOException
     * @throws DatasetException
     */
    public Schema loadSchema(final Path schemaPath)
            throws IOException, DatasetException {
        try {
            return DatasetsUtil.loadSchemaFromFSPath(hdfs,schemaPath);
        } catch (DatasetException e) {
            LOG.error(e.getMessage());
            throw e;
        } catch (IOException e) {
            LOG.error(e.getMessage());
            throw e;
        }
    }

    /**
     * Save a dataset schema to an HDFS path.
     *
     * @param schemaPath schema hdfs path.
     * @param schema <code>Schema</code> instance.
     * @throws IOException
     * @throws DatasetException
     */
    public void saveSchema(final Path schemaPath, final Schema schema)
            throws IOException, DatasetException {
        try {
            DatasetsUtil.saveSchemaToFSPath(hdfs, schema, schemaPath);
        } catch (DatasetException e) {
            LOG.error(e.getMessage());
            throw e;
        } catch (IOException e) {
            LOG.error(e.getMessage());
            throw e;
        }
    }

    /**
     * Imports the DBLP (dblp.xml) as a new dataset on the HDFS pprl-site.
     *
     * @param xmlPath local path to the dblp xml file.
     * @param name dataset name.
     * @return base path of the imported dblp dataset.
     * @throws Exception
     */
    public Path importDblpXmlDataset(final Path xmlPath, final String name)
            throws Exception {
        return importDblpXmlDataset(xmlPath,name,ONLY_OWNER_PERMISSION);
    }

    /**
     * Imports the DBLP (dblp.xml) as a new dataset on the HDFS pprl-site.
     *
     * @param xmlPath local path to the dblp xml file.
     * @param name dataset name.
     * @param permission permission.
     * @return base path of the imported dblp dataset.
     * @throws Exception
     */
    public Path importDblpXmlDataset(final Path xmlPath, final String name,
                                     final FsPermission permission)
            throws Exception {
        try {
            if(dblpXmlToAvroToolRunner == null) throw new IllegalStateException("tool-runner not set");
            final Path[] dataset = DatasetsUtil.createDatasetDirectories(hdfs, name, basePath, permission);
            final Path datasetPath = dataset[0];
            final Path datasetAvroPath = dataset[1];
            final Path datasetSchemaPath = dataset[2];

            final Path schemaPath = new Path(datasetSchemaPath,String.format("%s.avsc",name));
            LOG.info("Saving dblp schema at : " + schemaPath);
            DatasetsUtil.saveSchemaToFSPath(hdfs,
                    DblpPublication.getClassSchema(),
                    schemaPath);

            final Path inputPath = new Path(datasetPath,"xml");
            LOG.info("Uploading XML path at : " + inputPath);
            hdfs.mkdirs(inputPath,permission);
            hdfs.copyFromLocalFile(xmlPath, inputPath);

            final Path outputPath = datasetAvroPath;
            LOG.info("Runing tool");
            runDblpXmlToAvroTool(inputPath,outputPath);

            hdfs.setPermission(outputPath, permission);

            return datasetPath;
        } catch (Exception e) {
            LOG.error(e.getMessage());
            throw e;
        }
    }

    /**
     * Count Average Q-Grams per field.
     *
     * @param inputPath input datasets data HDFS path.
     * @param inputSchemaPath input datasets schema HDFS path.
     * @param statsBasePath base output path.
     * @param statsFileName stats output file.
     * @param fieldNames field names.
     * @return HDFS path containing the stats.
     * @throws Exception
     */
    public Path countAvgQgrams(final Path inputPath, final Path inputSchemaPath,
                               final Path statsBasePath,
                               final String statsFileName,
                               final String[] fieldNames)
            throws Exception {
        try {
            if(!hdfs.exists(statsBasePath)) hdfs.mkdirs(statsBasePath,ONLY_OWNER_PERMISSION);
            final Path statsPath = new Path(statsBasePath,
                    String.format("%s.properties",statsFileName));
            runQGramCountingTool(inputPath, inputSchemaPath, statsPath,fieldNames);
            hdfs.setPermission(statsPath, ONLY_OWNER_PERMISSION);
            return statsPath;
        } catch (Exception e) {
            LOG.error(e.getMessage());
            throw e;
        }
    }

    /**
     * Run DBLP XML to Avro tool.
     *
     * @param inputPath input path.
     * @param outputPath output path.
     * @throws Exception
     */
    private void runDblpXmlToAvroTool(final Path inputPath, final Path outputPath)
            throws Exception {
        if(dblpXmlToAvroToolRunner == null) throw new IllegalStateException("tool-runner not set");
        LOG.info("input={}", inputPath);
        LOG.info("output={}", outputPath);
        dblpXmlToAvroToolRunner.setArguments(inputPath.toString(), outputPath.toString());
        dblpXmlToAvroToolRunner.call();
    }

    /**
     * Run Q-Gram counting tool.
     *
     * @param inputPath input path.
     * @param inputSchemaPath input schema path.
     * @param propertiesOutputPath output path.
     * @param fieldNames field names.
     * @throws Exception
     */
    private void runQGramCountingTool(final Path inputPath, final Path inputSchemaPath,
                                      final Path propertiesOutputPath,
                                      final String[] fieldNames)
            throws Exception {
        if(qGramCountingToolRunner == null) throw new IllegalStateException("tool-runner not set");
        LOG.info("input={}", inputPath);
        LOG.info("inputSchemaPath={}", inputSchemaPath);
        LOG.info("propertiesOutputPath={}",propertiesOutputPath);
        final StringBuilder fsb = new StringBuilder(fieldNames[0]);
        if(fieldNames.length > 1)
            for (int i = 1; i <fieldNames.length; i++)
                fsb.append(",").append(fieldNames[i]);
        LOG.info("fieldNames={}", fsb.toString());
        qGramCountingToolRunner.setArguments(
                inputPath.toString(),
                inputSchemaPath.toString(),
                propertiesOutputPath.toString(),
                fsb.toString());
        qGramCountingToolRunner.call();
    }



    /**
     * Save statistics instance to a HDFS path.
     *
     * @param name a name.
     * @param basePath base path.
     * @param statistics <code>DatasetStatistics</code> instance.
     * @return return path statistics saved.
     * @throws IOException
     */
    public Path saveStats(final String name, final Path basePath, final DatasetStatistics statistics)
            throws IOException {
        try{
            final Properties properties = statistics.toProperties();
            final Path propertiesPath = new Path(basePath,String.format("%s.properties",name));
            final FSDataOutputStream fsdos = hdfs.create(propertiesPath);
            LOG.info("Saving stats stats from [path={}]", propertiesPath);
            properties.store(fsdos,"Statistics");
            fsdos.close();
            return propertiesPath;
        } catch (IOException e) {
            LOG.error(e.getMessage());
            throw e;
        }
    }

    /**
     * Load dataset statistics.
     *
     * @param propertiesPaths properties paths.
     * @return
     * @throws IOException
     * @throws DatasetException
     */
    public DatasetStatistics loadStats(final Path... propertiesPaths)
            throws IOException, DatasetException {
        try {
            Properties properties = new Properties();
            for (Path propertiesPath : propertiesPaths) {
                if (!hdfs.exists(propertiesPath))
                    throw new DatasetException(String.format("Cannot find file \"%s\"", propertiesPath));
                LOG.info("Loading stats from [path={}]", propertiesPath);
                FSDataInputStream fsdis = hdfs.open(propertiesPath);
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
     * @param datasetName a dataset name.
     * @param sampleName a sample dataset name.
     * @param size size of sample
     * @return base path of the sample dataset
     * @throws IOException
     * @throws DatasetException
     */
    public Path sampleDataset(final String datasetName,
                              final String sampleName,
                              int size)
            throws IOException, DatasetException {
        try {

            final Path[] datasetDirectories = retrieveDirectories(datasetName);
            final Path datasetAvroPath = datasetDirectories[1];
            final Path datasetSchemaPath = retrieveSchemaPath(datasetDirectories[2]);
            final Schema schema = loadSchema(datasetSchemaPath);

            final Path[] sampleDirectories = createDirectories(sampleName, DatasetsService.ONLY_OWNER_PERMISSION);
            final Path sampleBasePath = sampleDirectories[0];
            final Path sampleAvroPath = sampleDirectories[1];
            final Path sampleSchemaPath = new Path(sampleDirectories[2],String.format("%s.avsc",sampleName));
            saveSchema(sampleSchemaPath, schema);

            final DatasetsUtil.DatasetRecordReader reader =
                    new DatasetsUtil.DatasetRecordReader(hdfs, schema, datasetAvroPath);
            final DatasetsUtil.DatasetRecordWriter writer =
                    new DatasetsUtil.DatasetRecordWriter(hdfs, sampleName, schema, sampleAvroPath);
            int sampled = 0;
            final SecureRandom RANDOM = new SecureRandom();

            while (reader.hasNext() && sampled < size) {
                final GenericRecord record = reader.next();
                if(RANDOM.nextBoolean()) {
                    writer.writeRecord(record);
                    sampled++;
                }
            }
            reader.close();
            writer.close();

            return sampleBasePath;
        } catch (IOException e) {
            LOG.error(e.getMessage());
            throw e;
        } catch (DatasetException e) {
            LOG.error(e.getMessage());
            throw e;
        }
    }

    /**
     * Sort a dataset.
     *
     * @param name dataset name.
     * @param sortedName sorted dataset name.
     * @param partitions number of partitions.
     * @param fieldNames fields to sort-by.
     * @return the base path of the sorted dataset.
     */
    public Path sortDataset(final String name, final String sortedName,
                            final int partitions, final String... fieldNames)
            throws Exception {

        try {
            final Path[] inputDirectories = retrieveDirectories(name);
            final Path inputAvroPath = inputDirectories[1];
            final Path inputSchemaPath = retrieveSchemaPath(inputDirectories[2]);

            final Path[] sortedDirectories = createDirectories(sortedName, DatasetsService.ONLY_OWNER_PERMISSION);
            final Path sortedBasePath = sortedDirectories[0];
            final Path sortedAvroPath = sortedDirectories[1];
            final Path sortedSchemaPath = new Path(sortedDirectories[2],String.format("%s.avsc",name));

            runSortAvroTool(inputAvroPath, inputSchemaPath,
                    sortedAvroPath, sortedSchemaPath, partitions, fieldNames);

            return sortedBasePath;
        } catch (IOException e) {
            LOG.error(e.getMessage());
            throw e;
        } catch (Exception e) {
            LOG.error(e.getMessage());
            throw e;
        }
    }

    /**
     * Run sort avro tool.
     *
     * @param inputAvroPath input avro path.
     * @param inputSchemaPath input schema path.
     * @param sortedAvroPath sorted avro path.
     * @param sortedSchemaPath sorted schema path.
     * @param reducersCount number of reducers
     * @param fieldNames field names.
     * @throws Exception
     */
    private void runSortAvroTool(final Path inputAvroPath, final Path inputSchemaPath,
                                 final Path sortedAvroPath, final Path sortedSchemaPath,
                                 final int reducersCount, final  String[] fieldNames)
            throws Exception {
        if(sortAvroToolRunner == null) throw new IllegalStateException("tool-runner not set");
        LOG.info("input={}", inputAvroPath);
        LOG.info("inputSchemaPath={}", inputSchemaPath);
        LOG.info("sortedAvroPath={}",sortedAvroPath);
        LOG.info("sortedSchemaPath={}",sortedSchemaPath);
        LOG.info("Number of Reducers={}", reducersCount);
        final StringBuilder fsb = new StringBuilder(fieldNames[0]);
        if(fieldNames.length > 1)
            for (int i = 1; i <fieldNames.length; i++)
                fsb.append(",").append(fieldNames[i]);
        LOG.info("Sort-By fieldnames={}", fsb.toString());
        sortAvroToolRunner.setArguments(
                inputAvroPath.toString(),inputSchemaPath.toString(),
                sortedAvroPath.toString(),sortedSchemaPath.toString(),
                String.valueOf(reducersCount),fsb.toString()
        );
        sortAvroToolRunner.call();
    }

    /**
     * Add UID to dataset.
     *
     * @param name name of dataset.
     * @param fieldName field name of the UID.
     */
    public void addUIDToDataset(final String name, final String fieldName)
            throws IOException, DatasetException {
        try {
            final Path[] inputDirectories = retrieveDirectories(name);
            final Path inputAvroPath = inputDirectories[1];
            final Path inputSchemaPath = retrieveSchemaPath(inputDirectories[2]);
            final Schema schema = DatasetsUtil.loadSchemaFromFSPath(hdfs,inputSchemaPath);
            final Schema updatedSchema = DatasetsUtil.updateSchemaWithUID(schema,fieldName);

            final Path tmp = new Path(inputDirectories[0],"tmp");
            hdfs.mkdirs(tmp,ONLY_OWNER_PERMISSION);

            final DatasetsUtil.DatasetRecordReader reader = new DatasetsUtil.DatasetRecordReader(
                    hdfs,schema,inputAvroPath);
            int partitions = reader.getReaderCount();
            final DatasetsUtil.DatasetRecordWriter writer =
                    new DatasetsUtil.DatasetRecordWriter(hdfs,name,updatedSchema,tmp,partitions);

            int uid = 0;
            while (reader.hasNext()) {
                GenericRecord record = reader.next();
                GenericRecord updatedRecord =
                        DatasetsUtil.updateRecordWithUID(record,updatedSchema,fieldName,uid);
                uid++;
                writer.writeRecord(updatedRecord,reader.getCurrent());
            }
            reader.close();
            writer.close();

            hdfs.delete(inputSchemaPath,true);
            DatasetsUtil.saveSchemaToFSPath(hdfs,updatedSchema,inputSchemaPath);

            hdfs.delete(inputAvroPath,true);
            hdfs.rename(tmp,inputAvroPath);
        }  catch (IOException e) {
            LOG.error(e.getMessage());
            throw e;
        } catch (DatasetException e) {
            LOG.error(e.getMessage());
            throw e;
        }
    }
}
