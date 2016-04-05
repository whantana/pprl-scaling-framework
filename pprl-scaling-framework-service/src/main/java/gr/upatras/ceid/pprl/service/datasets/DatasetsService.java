package gr.upatras.ceid.pprl.service.datasets;

import gr.upatras.ceid.pprl.datasets.DatasetException;
import gr.upatras.ceid.pprl.datasets.DatasetStatistics;
import gr.upatras.ceid.pprl.datasets.DatasetsUtil;
import gr.upatras.ceid.pprl.avro.dblp.DblpPublication;
import org.apache.avro.Schema;
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

@Service
public class DatasetsService implements InitializingBean {

    protected static final Logger LOG = LoggerFactory.getLogger(DatasetsService.class);

    public void afterPropertiesSet() {
        try {
            basePath = new Path(hdfs.getHomeDirectory(),"pprl");
            if(!hdfs.exists(basePath))
                hdfs.mkdirs(basePath,ONLY_OWNER_PERMISSION);

            boolean onlyOwnerPermissionbaseDir = hdfs.getFileStatus(basePath)
                    .getPermission().equals(ONLY_OWNER_PERMISSION);
            LOG.info(String.format("Dataset service initialized [" +
                            " nn=%s, " +
                            " basePath = %s (ONLY_OWNER_PERMISION = %s)," +
                            " Tool#1 = %s, Tool#2 = %s]",
                    hdfs.getUri(),
                    basePath,onlyOwnerPermissionbaseDir,
                    (dblpXmlToAvroToolRunner != null),
                    (qGramCountingToolRunner != null)));
        } catch (IOException e) {
            LOG.error(e.getMessage());
        }
    }

    private static SecureRandom RANDOM = new SecureRandom();

    public static final FsPermission ONLY_OWNER_PERMISSION
            = new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE, false);

    public static final FsPermission OTHERS_CAN_READ_PERMISSION
            = new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.READ, false);

    @Autowired
    private FileSystem hdfs;

    @Autowired
    private FileSystem localFs;

    @Autowired
    private ToolRunner dblpXmlToAvroToolRunner;

    @Autowired
    private ToolRunner qGramCountingToolRunner;

    public FileSystem getLocalFs() {
        return localFs;
    }

    public void setLocalFs(FileSystem localFs) {
        this.localFs = localFs;
    }

    private Path basePath;

    public Path[] createDirectories(final String name, final FsPermission permission)
            throws IOException {
        return createDirectories(name,basePath,permission);
    }

    public Path[] createDirectories(final String name,final Path basePath, final FsPermission permission)
            throws IOException {
        return DatasetsUtil.createDatasetDirectories(hdfs, name, basePath, permission);
    }

    public Path[] retrieveDirectories(final String name) throws IOException {
        return retrieveDirectories(name,basePath);
    }

    public Path[] retrieveDirectories(final String name,final Path basePath)
            throws IOException {
        try {
            return DatasetsUtil.retrieveDatasetDirectories(hdfs,name,basePath);
        } catch (IOException e) {
            LOG.error(e.getMessage());
            throw e;
        }
    }

    public Schema retrieveSchema(final Path basePath)
            throws IOException, DatasetException {
        try {
            final Path schemaPath = DatasetsUtil.getSchemaPath(hdfs,basePath);
            return DatasetsUtil.loadSchemaFromFSPath(hdfs,schemaPath);
        } catch (IOException e) {
            LOG.error(e.getMessage());
            throw e;
        } catch (DatasetException e) {
            LOG.error(e.getMessage());
            throw e;
        }
    }

    public Path retrieveSchemaPath(final Path basePath)
            throws IOException {
        try {
            return DatasetsUtil.getSchemaPath(hdfs,basePath);
        } catch (IOException e) {
            LOG.error(e.getMessage());
            throw e;
        }
    }

    public Path uploadFiles(final Path[] avroPaths, final Path schemaPath,final  String name)
            throws IOException {
        return uploadFiles(avroPaths,schemaPath,name,ONLY_OWNER_PERMISSION);
    }

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

    public Path downloadFiles(final String name,
                              final String downloadName) throws DatasetException, IOException {
        final Path uploadedPath = new Path(basePath, name);
        return downloadFiles(uploadedPath,downloadName,localFs.getWorkingDirectory());
    }

    public Path downloadFiles(final String name,
                              final String downloadName,
                              final Path parent) throws DatasetException, IOException {
        final Path uploadedPath = new Path(basePath, name);
        return downloadFiles(uploadedPath,downloadName,parent);
    }

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


    public Schema loadSchema(final Path schemaPath)
            throws IOException, DatasetException {
        try {
            LOG.info(String.format("Loading schema [path=%s]",schemaPath));
            return DatasetsUtil.loadSchemaFromFSPath(hdfs,schemaPath);
        } catch (DatasetException e) {
            LOG.error(e.getMessage());
            throw e;
        } catch (IOException e) {
            LOG.error(e.getMessage());
            throw e;
        }
    }

    public Path importDblpXmlDataset(final Path xmlPath, final String name)
            throws Exception {
        return importDblpXmlDataset(xmlPath,name,ONLY_OWNER_PERMISSION);
    }

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

            removeSuccessFile(outputPath);

            hdfs.setPermission(outputPath, permission);

            return datasetPath;
        } catch (Exception e) {
            LOG.error(e.getMessage());
            throw e;
        }
    }

    public Path countAvgQgrams(final Path inputPath, final Path inputSchemaPath,
                               final Path basePath,
                               final String statsFileName,
                               final String[] fieldNames)
            throws Exception {
        try {
            if(!hdfs.exists(basePath)) hdfs.mkdirs(basePath,ONLY_OWNER_PERMISSION);
            final Path statsPath = new Path(basePath,
                    String.format("%s.properties",statsFileName));
            runQGramCountingTool(inputPath, inputSchemaPath, statsPath,fieldNames);
            hdfs.setPermission(statsPath, ONLY_OWNER_PERMISSION);
            return statsPath;
        } catch (Exception e) {
            LOG.error(e.getMessage());
            throw e;
        }
    }

    public void runDblpXmlToAvroTool(final Path inputPath, final Path outputPath)
            throws Exception {
        if(dblpXmlToAvroToolRunner == null) throw new IllegalStateException("tool-runner not set");
        LOG.info("input={}", inputPath);
        LOG.info("output={}", outputPath);
        dblpXmlToAvroToolRunner.setArguments(inputPath.toString(), outputPath.toString());
        dblpXmlToAvroToolRunner.call();
    }

    public void runQGramCountingTool(final Path inputPath, final Path inputSchemaPath,
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


    private void removeSuccessFile(final Path path) throws IOException {
        final Path p = new Path(path + "/_SUCCESS");
        if (hdfs.exists(p)) {
            hdfs.delete(p, false);
        }
    }

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

    // TODO sample files (need spark here?)

    // TODO Add a an int UID field for a sample
}
