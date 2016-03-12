package gr.upatras.ceid.pprl.datasets.service;

import gr.upatras.ceid.pprl.datasets.DatasetException;
import gr.upatras.ceid.pprl.datasets.DatasetsUtil;
import org.apache.avro.Schema;
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
import java.util.SortedSet;

@Service
public class DatasetsService implements InitializingBean {

    protected static final Logger LOG = LoggerFactory.getLogger(DatasetsService.class);

    public void afterPropertiesSet() {
        basePath = hdfs.getHomeDirectory();

        try {
            boolean onlyOwnerPermissionbaseDir = hdfs.getFileStatus(basePath)
                    .getPermission().equals(ONLY_OWNER_PERMISSION);
            LOG.info(String.format("Dataset service initialized [" +
                    " nn=%s, " +
                    " basePath = %s (ONLY_OWNER_PERMISION = %s)," +
                    " toolRunnerSet = %s]",
                    hdfs.getUri(),
                    basePath,onlyOwnerPermissionbaseDir,
                    (dblpXmlToAvroToolRunner != null)));
        } catch (IOException e) {
            LOG.error(e.getMessage());
        }
    }

    private static SecureRandom RANDOM = new SecureRandom();

    protected static final FsPermission ONLY_OWNER_PERMISSION
            = new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE, false);

    @Autowired
    protected FileSystem hdfs;

    @Autowired
    protected FileSystem localFs;

    @Autowired
    private ToolRunner dblpXmlToAvroToolRunner;

    public FileSystem getLocalFs() {
        return localFs;
    }

    public void setLocalFs(FileSystem localFs) {
        this.localFs = localFs;
    }

    private Path basePath;


    // TODO sample files (need spark here)

    // TODO Add a an int UID field for a sample


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
        return downloadFiles(name,downloadName,localFs.getWorkingDirectory());
    }

    public Path downloadFiles(final String name,
                              final String downloadName,
                              final Path parent) throws DatasetException, IOException {
        try{
            final Path uploadedPath = new Path(basePath, name);
            if(!hdfs.exists(uploadedPath)) throw new DatasetException("Cannot find path with name " + name);

            final Path[] dataset = DatasetsUtil.createDatasetDirectories(localFs,downloadName,parent);
            final Path destBasePath = dataset[0];
            final Path destAvroPath = dataset[1];
            final Path destSchemaPath = dataset[2];
            for(Path src: DatasetsUtil.getAllAvroPaths(hdfs,new Path[]{uploadedPath})) {
                final Path dest = new Path(destAvroPath, src.getName());
                LOG.info("\tDownloading {} to {}", src, dest);
                hdfs.copyToLocalFile(src, dest);
            }

            final Path src = DatasetsUtil.getSchemaPath(hdfs,uploadedPath);
            final Path dest = new Path(destSchemaPath,src.getName());
            LOG.info("Downloading avro schema file.");
            LOG.info("\tDownloading {} to {}", src, dest);
            hdfs.copyToLocalFile(src,dest);

            return destBasePath;
        } catch (DatasetException e) {
            LOG.error(e.getMessage());
            throw e;
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

    public Path importDblpXmlDataset(final Path xmlPath, final Path schemaPath, final String name)
            throws Exception {
        return importDblpXmlDataset(xmlPath,schemaPath,name,ONLY_OWNER_PERMISSION);
    }

    public Path importDblpXmlDataset(final Path xmlPath, final Path schemaPath, final String name,
                                     final FsPermission permission)
            throws Exception {
        try {
            if(dblpXmlToAvroToolRunner == null) throw new IllegalStateException("tool-runner not set");
            final Path[] dataset = DatasetsUtil.createDatasetDirectories(hdfs, name, basePath, permission);
            final Path datasetPath = dataset[0];
            final Path datasetAvroPath = dataset[1];
            final Path datasetSchemaPath = dataset[2];
            hdfs.copyFromLocalFile(schemaPath, new Path(datasetSchemaPath,schemaPath.getName()));

            final Path inputPath = new Path(datasetPath,"xml");
            final Path outputPath = datasetAvroPath;
            hdfs.mkdirs(inputPath,permission);
            hdfs.copyFromLocalFile(xmlPath, inputPath);


            runDblpXmlToAvroTool(inputPath,outputPath);

            removeSuccessFile(outputPath);

            hdfs.setPermission(outputPath, permission);

            return datasetPath;
        } catch (Exception e) {
            LOG.error(e.getMessage());
            throw e;
        }
    }

    private void runDblpXmlToAvroTool(final Path inputPath, final Path outputPath)
            throws Exception {
        LOG.info("input={}", inputPath);
        LOG.info("output={}", outputPath);
        dblpXmlToAvroToolRunner.setArguments(inputPath.toString(), outputPath.toString());
        dblpXmlToAvroToolRunner.call();
    }

    private void removeSuccessFile(final Path path) throws IOException {
        final Path p = new Path(path + "/_SUCCESS");
        if (hdfs.exists(p)) {
            hdfs.delete(p, false);
        }
    }
}
