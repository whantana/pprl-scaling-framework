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
import org.springframework.data.hadoop.mapreduce.ToolRunner;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;

@Service
public class DatasetsService {

    @Autowired
    private FileSystem pprlClusterHdfs;

    @Autowired
    private ToolRunner dblpXmlToAvroToolRunner;

    private static final Logger LOG = LoggerFactory.getLogger(DatasetsService.class);

    private static final FsPermission ONLY_OWNER_PERMISSION
            = new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE, false);

//    public void loadDatasets() throws IOException {
//        private Path usersDatasetsFile = new Path(pprlClusterHdfs.getHomeDirectory() + "/.pprl_datasets");
//        if(pprlClusterHdfs.exists(usersDatasetsFile)) {
//            LOG.info("Loading Datasets from file");
//        }
//        else {
//            LOG.info("No datasets file found on cluster. Creating one.");
//            pprlClusterHdfs.createNewFile(usersDatasetsFile);
//        }
//    }

    public void importDataset(final File localAvroFile, final File localAvroSchemaFile, final String datasetName)
            throws IOException, DatasetException {
        try {
            final Dataset dataset = new Dataset(datasetName, pprlClusterHdfs.getHomeDirectory());
            dataset.buildOnFS(pprlClusterHdfs);
            uploadFileToHdfs(localAvroFile, dataset.getAvroPath());
            uploadFileToHdfs(localAvroSchemaFile, dataset.getAvroSchemaPath());
            LOG.info("Dataset : " + datasetName + ", Base path       : " + dataset.getBasePath());
            LOG.info("Dataset : " + datasetName + ", Avro path       : " + dataset.getAvroPath());
            LOG.info("Dataset : " + datasetName + ", AvroSchema Path : " + dataset.getAvroSchemaPath());
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

    private void removeSuccessFile(final Path path) throws IOException {
        final Path p = new Path(path + "/_SUCCESS");
        if(pprlClusterHdfs.exists(p)) pprlClusterHdfs.delete(p,false);
    }
}
