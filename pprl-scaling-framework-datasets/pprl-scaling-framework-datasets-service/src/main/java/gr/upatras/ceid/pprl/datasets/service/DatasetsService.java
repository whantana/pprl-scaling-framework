package gr.upatras.ceid.pprl.datasets.service;

import gr.upatras.ceid.pprl.datasets.Dataset;
import gr.upatras.ceid.pprl.datasets.DatasetException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class DatasetsService implements InitializingBean {

    private static final FsPermission ONLY_OWNER_PERMISSION
            = new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE, false);

    private static final Logger LOG = LoggerFactory.getLogger(DatasetsService.class);

    @Autowired
    private FileSystem pprlClusterHdfs;

    @Autowired
    private ToolRunner dblpXmlToAvroToolRunner;

    private List<Dataset> datasets = new ArrayList<Dataset>();

    public void afterPropertiesSet() throws Exception {
        checkSite();
        loadDatasets();
        LOG.info("Service is now initialized. Found {} datasets on the PPRL site.", datasets.size());
    }

    public void importDataset(final File localAvroFile, final File localAvroSchemaFile, final String datasetName)
            throws IOException, DatasetException {
        try {
            final Dataset dataset = new Dataset(datasetName, pprlClusterHdfs.getHomeDirectory());
            dataset.buildOnFS(pprlClusterHdfs);
            uploadFileToHdfs(localAvroFile, dataset.getAvroPath());
            uploadFileToHdfs(localAvroSchemaFile, dataset.getAvroSchemaPath());
            LOG.info("Dataset : {}, Base path       : {}", datasetName, dataset.getBasePath());
            LOG.info("Dataset : {}, Avro path       : {}", datasetName, dataset.getAvroPath());
            LOG.info("Dataset : {}, Avro Schema Path : {}", datasetName, dataset.getAvroSchemaPath());
            addToDatasets(dataset);
            LOG.info("Dataset : {}, Added to users datasets.", datasetName);
        } catch (IOException e) {
            LOG.error(e.getMessage());
            throw e;
        } catch (DatasetException e) {
            LOG.error(e.getMessage());
            throw e;
        }
    }

    public void importDblpXmlDataset(final File localDatasetFile, final File localSchemaFile, final String datasetName)
            throws Exception {
        try {
            final Dataset dataset = new Dataset(datasetName, pprlClusterHdfs.getHomeDirectory());
            dataset.buildOnFS(pprlClusterHdfs, false, true, true);
            final Path input = uploadFileToHdfs(localDatasetFile, new Path(dataset.getBasePath() + "/xml"));
            uploadFileToHdfs(localSchemaFile, dataset.getAvroSchemaPath());
            final Path output = dataset.getAvroPath();
            LOG.info("Running XML to Avro MapReduce job for dataset : " + datasetName + ".");
            LOG.info("Dataset : {}, Base path       : {}", datasetName, dataset.getBasePath());
            LOG.info("Dataset : {}, XML path        : {}", datasetName, input);
            LOG.info("Dataset : {}, Avro path       : {}", datasetName, output);
            LOG.info("Dataset : {}, Avro Schema Path : {}", datasetName, dataset.getAvroSchemaPath());
            runDblpXmlToAvroTool(input, output);
            removeSuccessFile(output);
            addToDatasets(dataset);
            LOG.info("Dataset : {}, Added to users datasets.", datasetName);
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
        } catch (DatasetException e) {
            LOG.error(e.getMessage());
            throw e;
        }
    }

    public List<String> listDatasets() {
        final List<String> names = new ArrayList<String>();
        for (Dataset dataset : datasets)
            names.add(dataset.getName());
        return names;
    }

    public List<String> sampleOfDataset(final String datasetName, int sampleSize) throws DatasetException, IOException {
        final Dataset dataset = findDatasetByName(datasetName);
        final List<String> sample = new ArrayList<String>(sampleSize);

        dataset.getReader(pprlClusterHdfs);
        GenericRecord record = dataset.getNextRecord();
        do {
            if ((new java.util.Random()).nextBoolean())
                if (!sample.contains(record.toString())) {
                    sample.add(record.toString());
                    sampleSize--;
                    if(sampleSize==0) break;
                }
            record = dataset.getNextRecord();
        } while (record != null);

        return sample;
    }

    public void dropDataset(final String datasetName, final boolean completeDrop)
            throws IOException, DatasetException {
        Dataset dataset = findDatasetByName(datasetName);
        if (completeDrop) pprlClusterHdfs.delete(dataset.getBasePath(), true);
        removeFromDatasets(dataset);
    }

    public void generateDatasetStats(final String datasetName) throws DatasetException {
        Dataset dataset = findDatasetByName(datasetName);

        // TODO implement me
    }

    public Map<String, String> describeDataset(final String datasetName) throws DatasetException,
            IOException {
        final Dataset dataset = findDatasetByName(datasetName);
        final Schema s = dataset.getSchema(pprlClusterHdfs);
        final Map<String, String> description = new HashMap<String, String>();
        for (Schema.Field f : s.getFields())
            description.put(f.name(), f.schema().getType().toString());
        return description;
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
            LOG.debug("File : {} copiedFromLocalFile to path {}", file.getAbsolutePath(), destination);
            return destination;
        } catch (IOException e) {
            LOG.error(e.getMessage());
            throw e;
        }
    }

    private void runDblpXmlToAvroTool(final Path input, final Path output)
            throws Exception {
        try {
            dblpXmlToAvroToolRunner.setArguments(input.toString(), output.toString());
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

    private void removeSuccessFile(final Path path) throws IOException {
        final Path p = new Path(path + "/_SUCCESS");
        if (pprlClusterHdfs.exists(p)) {
            pprlClusterHdfs.delete(p, false);
            LOG.debug("{} removed", p);
        }
    }

    private void checkSite() throws DatasetException, IOException {
        final Path homeDirectory = pprlClusterHdfs.getHomeDirectory();
        if (!pprlClusterHdfs.exists(homeDirectory)) {
            LOG.error("Cannot find Home directory on pprl site");
            throw new DatasetException("Cannot find Home directory on pprl site");
        }
        if (!pprlClusterHdfs.getFileStatus(homeDirectory).getPermission().equals(ONLY_OWNER_PERMISSION)) {
            LOG.warn("Permissions should be ONLY_OWNER_PERMISSION");
        }
    }

    private void loadDatasets() throws IOException {
        final Path userDatasetsFile = new Path(pprlClusterHdfs.getHomeDirectory() + "/.pprl_datasets");
        if (pprlClusterHdfs.exists(userDatasetsFile)) {
            final BufferedReader br =
                    new BufferedReader(new InputStreamReader(pprlClusterHdfs.open(userDatasetsFile)));
            try {
                String line;
                line = br.readLine();
                while (line != null) {
                    LOG.debug("read line : {}", line);
                    datasets.add(Dataset.fromString(line));
                    line = br.readLine();
                }
            } finally {
                br.close();
            }
        } else FileSystem.create(pprlClusterHdfs, userDatasetsFile, ONLY_OWNER_PERMISSION);
    }

    private void saveDatasets() throws IOException {
        final Path userDatasetsFile = new Path(pprlClusterHdfs.getHomeDirectory() + "/.pprl_datasets");
        if (!pprlClusterHdfs.exists(userDatasetsFile)) throw new IOException("users dtatset file does not exist");
        final BufferedWriter bw =
                new BufferedWriter(new OutputStreamWriter(pprlClusterHdfs.create(userDatasetsFile)));
        try {
            for (Dataset dataset : datasets) {
                LOG.debug("Writing line : {}", dataset.toString());
                bw.write(dataset.toString() + "\n");
            }
        } finally {
            bw.close();
        }
    }

    private void addToDatasets(final Dataset d) throws IOException {
        if (!datasets.contains(d)) datasets.add(d);
        saveDatasets();
    }

    private void removeFromDatasets(final Dataset d) throws IOException {
        if (datasets.contains(d)) datasets.remove(d);
        saveDatasets();
    }

    private Dataset findDatasetByName(final String datasetName) throws DatasetException {
        for (Dataset dataset : datasets) if (dataset.getName().equals(datasetName)) return dataset;
        throw new DatasetException("Cannot find dataset with name" + datasetName);
    }
}
