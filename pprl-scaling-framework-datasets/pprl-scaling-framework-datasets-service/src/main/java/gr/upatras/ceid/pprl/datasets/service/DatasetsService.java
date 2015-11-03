package gr.upatras.ceid.pprl.datasets.service;

import gr.upatras.ceid.pprl.datasets.Dataset;
import gr.upatras.ceid.pprl.datasets.DatasetException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.hadoop.hive.HiveOperations;
import org.springframework.data.hadoop.mapreduce.ToolRunner;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
public class DatasetsService {

    @Autowired
    private FileSystem pprlClusterHdfs;

    @Autowired
    private ToolRunner dblpXmlToAvroToolRunner;

    private HiveOperations hiveOperations;

//    private Path usersDatasetsFile;

    private static final Logger LOG = LoggerFactory.getLogger(DatasetsService.class);

    private static final FsPermission ONLY_OWNER_PERMISSION
            = new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE, false);

	private static final Pattern HDFS_URL_PATTERN = Pattern.compile("(.*://).*?(/.*)");

    @Autowired
    public DatasetsService(final HiveOperations hiveOperations) {
        this.hiveOperations = hiveOperations;
    }

//    public void loadDatasets() throws IOException {
//        usersDatasetsFile = new Path(pprlClusterHdfs.getHomeDirectory() + "/.pprl_datasets");
//        if(pprlClusterHdfs.exists(usersDatasetsFile)) {
//            LOG.info("Loading Datasets from file");
//        }
//        else {
//            LOG.info("No datasets file found on cluster. Creating one.");
//            pprlClusterHdfs.createNewFile(usersDatasetsFile);
//        }
//    }

    public void importDataset(final File localAvroFile, final File localAvroSchemaFile, final String datasetName)
            throws IOException, SQLException, DatasetException {
        try {
            final Dataset dataset = new Dataset(datasetName, pprlClusterHdfs.getHomeDirectory());
            dataset.buildOnFS(pprlClusterHdfs);
            uploadFileToHdfs(localAvroFile, dataset.getAvroPath());
            uploadFileToHdfs(localAvroSchemaFile, dataset.getAvroSchemaPath());
//            makeDatasetTable(datasetName, files, schema); TODO : WHAT ABOUT HIVE?
            LOG.info("Dataset : " + datasetName + ", Base path       : " + dataset.getBasePath());
            LOG.info("Dataset : " + datasetName + ", Avro path       : " + dataset.getAvroPath());
            LOG.info("Dataset : " + datasetName + ", AvroSchema Path : " + dataset.getAvroSchemaPath());
//            LOG.info("Dataset : " + datasetName + ", table is ready.");
        } catch (IOException e) {
            LOG.error(e.getMessage());
            throw e;
//        }
//        catch (SQLException e) {
//            LOG.error(e.getMessage());
//            throw e;
        } catch (DatasetException e) {
            LOG.error(e.getMessage());
            throw e;
        }
    }
    public void importDblpXmlDataset(final File localDatasetFile, final File localSchemaFile, final String datasetName)
            throws Exception {
        try {
            final Dataset dataset = new Dataset(datasetName, pprlClusterHdfs.getHomeDirectory());
            dataset.buildOnFS(pprlClusterHdfs,false,true,true);
            final Path input = uploadFileToHdfs(localDatasetFile, new Path(dataset.getAvroPath() + "/xml"));
            uploadFileToHdfs(localSchemaFile, dataset.getAvroSchemaPath());
            final Path output = dataset.getAvroPath();
            LOG.info("Running XML to Avro MapReduce job for dataset : " + datasetName + ".");
            LOG.info("Dataset : " + datasetName + ", Base path       : " + dataset.getBasePath());
            LOG.info("Dataset : " + datasetName + ", XML files Path  : " + input);
            LOG.info("Dataset : " + datasetName + ", AvroSchema Path : " + dataset.getAvroSchemaPath());
            LOG.info("Dataset : " + datasetName + ", Avro Data files Path : " + dataset.getAvroPath());
//            LOG.info("Dataset : " + datasetName + ", table is ready.");
            runDblpXmlToAvroTool(input, output);
            removeSuccessFile(output);
//            makeDatasetTable(datasetName, output, dataset.getAvroSchemaPath());
        } catch (InterruptedException e) {
            LOG.error(e.getMessage());
            throw e;
        } catch (IOException e) {
            LOG.error(e.getMessage());
            throw e;
        } catch (ClassNotFoundException e) {
            LOG.error(e.getMessage());
            throw e;
        } catch (SQLException e) {
            LOG.error(e.getMessage());
            throw e;
        }
    }

    public void listDatasets() {
        // TODO implement me
    }

    public void sampleOfDataset(final String datasetName) {
        // TODO implement me
    }

    public void dropDataset(final String datasetName, final boolean completeDrop) {
        // TODO implement me
    }

    public void getStatsFromDataset(final String datasetName) {
        // TODO implement me
    }

    private Path makeDatasetDirectory(final String datasetName, final boolean makeDirs)
            throws IOException {
        try {
            final Path path = new Path(pprlClusterHdfs.getHomeDirectory() + "/" + datasetName);

            if (pprlClusterHdfs.exists(path)) {
                LOG.debug("Path : " + path + " already exists. Deleting it.");
                pprlClusterHdfs.delete(path, true);
            }

            pprlClusterHdfs.mkdirs(path, ONLY_OWNER_PERMISSION);
            LOG.debug("Path : " + path + " created with permissions " + ONLY_OWNER_PERMISSION);
            if (makeDirs) {
                LOG.debug("Path : " + path + ". Creating subdirectories.");
                final Path dataDirectory = new Path(path + "/avro");
                final Path schemaDirectory = new Path(path + "/schema");
                pprlClusterHdfs.mkdirs(dataDirectory, ONLY_OWNER_PERMISSION);
                pprlClusterHdfs.mkdirs(schemaDirectory, ONLY_OWNER_PERMISSION);
                LOG.debug("Path : " + path + ". Created subdirectories [" +
                        dataDirectory + " " + ONLY_OWNER_PERMISSION  + "," +
                        schemaDirectory + " " + ONLY_OWNER_PERMISSION  + "].");
            }
            return path;
        } catch (IOException e) {
            LOG.error(e.getMessage());
            throw e;
        }
    }

    private Path uploadFileToHdfs(final File file, final Path path)
            throws IOException {
        return uploadFileToHdfs(file, path, ONLY_OWNER_PERMISSION);
    }

    private Path uploadFileToHdfs(final File file, final Path path, final FsPermission permission)
            throws IOException {
        try {
            final Path source = new Path(file.toURI());
            final Path destination;
            if (!pprlClusterHdfs.exists(path)) pprlClusterHdfs.mkdirs(path, permission);
            else pprlClusterHdfs.delete(path, true);

            destination = new Path(path + "/" + file.getName());
            pprlClusterHdfs.copyFromLocalFile(source, destination);
            pprlClusterHdfs.setPermission(destination, permission);
            LOG.info("File : " + file + " copiedFromLocalFile to Path [" + destination + " " + permission + "].");
            return destination;
        } catch (IOException e) {
            LOG.error(e.getMessage());
            throw e;
        }
    }

    private void runDblpXmlToAvroTool(final Path input, final Path output)
            throws Exception {
        try {
            dblpXmlToAvroToolRunner.setArguments(input.toString(),output.toString());
            final Integer result = dblpXmlToAvroToolRunner.call();
            pprlClusterHdfs.setPermission(output, new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE, false));
        } catch (InterruptedException e) {
            LOG.error(e.getMessage());
            throw e;
        } catch (IOException e) {
            LOG.error(e.getMessage());
            throw e;
        } catch (ClassNotFoundException e) {
            LOG.error(e.getMessage());
            throw e;
        } catch (Exception e) {
            LOG.error(e.getMessage());
            throw e;
        }
    }

    private List<String> makeDatasetTable(final String datasetName, final Path avroPath, final Path schemaPath)
            throws SQLException, IOException {

		final String properAvroPath = pprlClusterHdfs.isDirectory(avroPath) ? avroPath.toString() : avroPath.getParent().toString();
		
		final Matcher schemaMatcher = HDFS_URL_PATTERN.matcher(schemaPath.toString());
		final Matcher avroMatcher = HDFS_URL_PATTERN.matcher(properAvroPath);
		
		if (!schemaMatcher.matches() || !avroMatcher.matches()) {
			LOG.error("hdfs path shortening regex does not match on : " + schemaPath.toString() +".");
			LOG.error("hdfs path shortening regex does not match on : " + properAvroPath +".");
			return null;
		}
		
		final String schemaLocation = schemaMatcher.group(1) + schemaMatcher.group(2);
		final String avroLocation = avroMatcher.group(1) + avroMatcher.group(2);

		final Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("tableName", datasetName);
        parameters.put("avroLocation", avroLocation);
        parameters.put("schemaLocation", schemaLocation);
        LOG.debug("Running hive query (import-avro-hive.hql) parameters : " + parameters + ".");
        final List<String> values = hiveOperations.query("classpath:import-avro-hive.hql", parameters);
        LOG.debug("Hive query (import-avro-hive.hql) done. Values : " + values + ".");
        return values;
    }

    private void removeSuccessFile(final Path path) throws IOException {
        final Path p = new Path(path + "/_SUCCESS");
        if(pprlClusterHdfs.exists(p)) pprlClusterHdfs.delete(p,false);
    }
}
