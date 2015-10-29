package gr.upatras.ceid.pprl.datasets.service;

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

    private static final Logger LOG = LoggerFactory.getLogger(DatasetsService.class);

    private static final FsPermission ONLY_OWNER_PERMISSION
            = new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE, false);

	private static final Pattern HDFS_URL_PATTERN = Pattern.compile("(.*://).*?(/.*)");
    
	@Autowired
    public DatasetsService(final HiveOperations hiveOperations) {
        this.hiveOperations = hiveOperations;
    }

    public void importDataset(final File localDatasetFile, final File localSchemaFile, final String datasetName)
            throws IOException {
        try {
            final Path dataset = makeDatasetDirectory(datasetName, true);
            LOG.info("Dataset : " + datasetName + ", Path : " + dataset + ".");
            final Path files = uploadFileToHdfs(localDatasetFile, new Path(dataset + "/avro"));
            LOG.info("Dataset : " + datasetName + ", Avro Data files Path : " + files + ".");
            final Path schema = uploadFileToHdfs(localSchemaFile, new Path(dataset + "/schema"));
            LOG.info("Dataset : " + datasetName + ", Avro Schema Path : " + schema + ".");
//            makeDatasetTable(datasetName, files, schema);
//            LOG.info("Dataset : " + datasetName + ", table is ready.");
        } catch (IOException e) {
            LOG.error(e.getMessage());
            throw e;
        }
//        catch (SQLException e) {
//            LOG.error(e.getMessage());
//            throw e;
//        }
    }
    public void importDblpXmlDataset(final File localDatasetFile, final File localSchemaFile, final String datasetName)
            throws Exception {
        try {
            final Path dataset = makeDatasetDirectory(datasetName, false);
            LOG.info("Dataset : " + datasetName + ", Path : " + dataset + ".");
            final Path input = uploadFileToHdfs(localDatasetFile, new Path(dataset + "/xml"));
            LOG.info("Dataset : " + datasetName + ", XML files Path : " + input + ".");
            final Path schema = uploadFileToHdfs(localSchemaFile, new Path(dataset + "/schema"));
            LOG.info("Dataset : " + datasetName + ", Avro Schema Path : " + schema + ".");
            final Path files = new Path(dataset + "/avro");
            LOG.info("Dataset : " + datasetName + ", Avro Data files Path : " + files + ".");
            runDblpXmlToAvroTool(input, files);
            removeSuccessFile(files);
            LOG.info("Dataset : " + datasetName + ", Running XML to Avro MapReduce job.");
//            makeDatasetTable(datasetName, files, schema);
//            LOG.info("Dataset : " + datasetName + ", table is ready.");
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
    public void listDatasets(){}
    public void sampleOfDataset(final String datasetName){}
    public void dropDataset(final String datasetName, final boolean completeDrop){}

    public Path makeDatasetDirectory(final String datasetName, final boolean makeDirs)
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

    public Path uploadFileToHdfs(final File file, final Path path)
            throws IOException {
        return uploadFileToHdfs(file, path, ONLY_OWNER_PERMISSION);
    }

    public Path uploadFileToHdfs(final File file, final Path path, final FsPermission permission)
            throws IOException {
        try {
            final Path source = new Path(file.toURI());
            final Path destination;
            if (!pprlClusterHdfs.exists(path)) {
                LOG.debug("Path : " + path + " does not exist. Creating it.");
                pprlClusterHdfs.mkdirs(path, permission);
            } else {
                LOG.debug("Path : " + path + " already exists. Deleting it.");
                pprlClusterHdfs.delete(path, true);
            }
            destination = new Path(path + "/" + file.getName());

            pprlClusterHdfs.copyFromLocalFile(source, destination);
            pprlClusterHdfs.setPermission(destination, permission);
            LOG.debug("File : " + file + " copied to Path [" + destination + " " + permission + "].");
            return destination;
        } catch (IOException e) {
            LOG.error(e.getMessage());
            throw e;
        }
    }

    public void runDblpXmlToAvroTool(final Path input, final Path output)
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

    public List<String> makeDatasetTable(final String datasetName, final Path avroPath, final Path schemaPath)
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
