package gr.upatras.ceid.pprl.datasets.service;

import gr.upatras.ceid.pprl.datasets.Dataset;
import gr.upatras.ceid.pprl.datasets.DatasetException;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.hadoop.mapreduce.ToolRunner;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class DatasetsService implements InitializingBean {

    private static final Logger LOG = LoggerFactory.getLogger(DatasetsService.class);

    private static final FsPermission ONLY_OWNER_PERMISSION
            = new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE, false);

    public static final String DATASETS_FILE=".pprl_datasets";

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

    public void importDataset(final String datasetName, final File localAvroSchemaFile, final File... localAvroFiles)
            throws IOException, DatasetException {
        try {
            final Dataset dataset = new Dataset(datasetName, pprlClusterHdfs.getHomeDirectory());
            dataset.buildOnFS(pprlClusterHdfs,ONLY_OWNER_PERMISSION);
            uploadFileToHdfs(dataset.getAvroPath(),localAvroFiles);
            uploadFileToHdfs(dataset.getAvroSchemaPath(),localAvroSchemaFile);
            LOG.info("Dataset : {}, Base path       : {}", datasetName, dataset.getBasePath());
            LOG.info("Dataset : {}, Avro path       : {}", datasetName, dataset.getAvroPath());
            LOG.info("Dataset : {}, Avro Schema Path : {}", datasetName, dataset.getAvroSchemaPath());
            addToDatasets(dataset);
        } catch (IOException e) {
            LOG.error(e.getMessage());
            throw e;
        } catch (DatasetException e) {
            LOG.error(e.getMessage());
            throw e;
        }
    }

    public void importDblpXmlDataset(final String datasetName, final File localAvroSchemaFile, final File... localDblpFiles)
            throws Exception {
        try {
            final Dataset dataset = new Dataset(datasetName, pprlClusterHdfs.getHomeDirectory());
            dataset.buildOnFS(pprlClusterHdfs, false, true, true, ONLY_OWNER_PERMISSION);
            final Path input = new Path(dataset.getBasePath() + "/xml");
            uploadFileToHdfs(input,localDblpFiles);
            uploadFileToHdfs(dataset.getAvroSchemaPath(),localDblpFiles);
            final Path output = dataset.getAvroPath();
            LOG.info("Dataset : {}, Base path       : {}", datasetName, dataset.getBasePath());
            LOG.info("Dataset : {}, XML path        : {}", datasetName, input);
            LOG.info("Dataset : {}, Avro path       : {}", datasetName, output);
            LOG.info("Dataset : {}, Avro Schema Path : {}", datasetName, dataset.getAvroSchemaPath());
            runDblpXmlToAvroTool(input, output);
            removeSuccessFile(output);
            addToDatasets(dataset);
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
        final List<String> sampleStr = new ArrayList<String>(sampleSize);
        final Dataset dataset = findDatasetByName(datasetName);
        final List<GenericRecord> sample = sampleOfDataset(dataset,sampleSize);
        for(GenericRecord record : sample)
            sampleStr.add(record.toString());
        return sampleStr;
    }

    public List<String> saveSampleOfDataset(final String datasetName, int sampleSize,
                                            final File sampleSchemaFile, final File sampleDataFile)
            throws DatasetException, IOException {
        final List<String> sampleStr = new ArrayList<String>(sampleSize);
        final Dataset dataset = findDatasetByName(datasetName);
        final Schema schema = dataset.getSchema(pprlClusterHdfs);
        final List<GenericRecord> sample = sampleOfDataset(dataset, sampleSize);
        saveSampleOfDataset(sample,schema,sampleSchemaFile,sampleDataFile);
        for(GenericRecord record : sample)
            sampleStr.add(record.toString());
        return sampleStr;
    }

    public void saveSampleOfDataset(final List<GenericRecord> sampleOfDataset,final Schema schema,
                                    final File sampleSchemaFile, final File sampleDataFile)
            throws IOException, DatasetException {
        final PrintWriter schemaWriter = new PrintWriter(sampleSchemaFile);
        schemaWriter.print(schema.toString(true));
        schemaWriter.close();

        DataFileWriter<GenericRecord> writer =
                new DataFileWriter<GenericRecord>(new GenericDatumWriter<GenericRecord>(schema));
        DataFileWriter<GenericRecord> sampleWriter = writer.create(schema,sampleDataFile);
        for(GenericRecord record : sampleOfDataset)
            sampleWriter.append(record);
        sampleWriter.close();
    }

    public List<GenericRecord> sampleOfDataset(final Dataset dataset, int sampleSize)
            throws IOException, DatasetException {
        final List<GenericRecord> sample = new ArrayList<GenericRecord>(sampleSize);
        Dataset.DatasetRecordReader reader = dataset.getReader(pprlClusterHdfs);
        while(reader.hasNext()) {
            GenericRecord record = reader.next();
            if ((new java.util.Random()).nextBoolean()) {
                LOG.debug("Adding Record");
                sample.add(record);
                sampleSize--;
                if (sampleSize == 0) break;
            } else LOG.debug("Not adding record");
        }
        reader.close();
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

    private void uploadFileToHdfs(final Path path, final File...files)
            throws IOException {
        uploadFileToHdfs(path, ONLY_OWNER_PERMISSION,files);
    }

    private void uploadFileToHdfs(final Path path, final FsPermission permission, final File...files)
            throws IOException {
        try {
            if(!pprlClusterHdfs.exists(path)) {
                LOG.warn("Path {} does not exist. Creating it", path);
                pprlClusterHdfs.mkdirs(path,ONLY_OWNER_PERMISSION);
            }
            for(File file : files) {
                final Path source = new Path(file.toURI());
                final Path destination = new Path(path + "/" + file.getName());
                pprlClusterHdfs.copyFromLocalFile(source, destination);
                pprlClusterHdfs.setPermission(destination, permission);
                LOG.debug("File : {} copiedFromLocalFile to path {}", file.getAbsolutePath(), destination);
            }
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
            LOG.warn("Home directories permissinos should be {}",ONLY_OWNER_PERMISSION);
        }
    }

    private void loadDatasets() throws IOException {
        final Path userDatasetsFile = new Path(pprlClusterHdfs.getHomeDirectory() + "/" + DATASETS_FILE);
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
        final Path userDatasetsFile = new Path(pprlClusterHdfs.getHomeDirectory() + "/" + DATASETS_FILE);
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
        LOG.info("Dataset : {}, Added to datasets.", d.getName());
    }

    private void removeFromDatasets(final Dataset d) throws IOException {
        if (datasets.contains(d)) datasets.remove(d);
        saveDatasets();
        LOG.info("Dataset : {}, Removed from datasets.", d.getName());
    }

    private Dataset findDatasetByName(final String datasetName) throws DatasetException {
        for (Dataset dataset : datasets) if (dataset.getName().equals(datasetName)) return dataset;
        throw new DatasetException("Cannot find dataset with name" + datasetName);
    }
}
