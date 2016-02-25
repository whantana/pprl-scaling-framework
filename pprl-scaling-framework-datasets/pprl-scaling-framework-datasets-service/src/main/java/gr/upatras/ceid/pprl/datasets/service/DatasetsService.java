package gr.upatras.ceid.pprl.datasets.service;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.hadoop.mapreduce.ToolRunner;
import org.springframework.stereotype.Service;

import java.security.SecureRandom;

@Service
public class DatasetsService implements InitializingBean {

    protected static final Logger LOG = LoggerFactory.getLogger(DatasetsService.class);

    public void afterPropertiesSet() {
        LOG.info("Dataset service initialized.");
    }

    private static SecureRandom RANDOM = new SecureRandom();

    protected static final FsPermission ONLY_OWNER_PERMISSION
            = new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE, false);

    @Autowired
    protected FileSystem pprlClusterHdfs;

//    @Autowired
//    private ToolRunner dblpXmlToAvroToolRunner;
//
//    @Autowired
//    private ToolRunner getDatasetStatsToolRunner;

//        try {
//            if(isPprlClusterHdfsSet()) {
//                checkSite();
//                userDatasetsFile = new Path(pprlClusterHdfs.getHomeDirectory() + "/.pprl_datasets");
//                loadDatasets();
//                LOG.info("Service is now initialized. Found {} datasets on the PPRL site.", datasets.size());
//            } else{
//                LOG.info("Service is now initialized. Not connected to a  PPRL site.");
//            }
//        } catch (DatasetException e) {
//            LOG.error(e.getMessage());
//        } catch (IOException e) {
//            LOG.error(e.getMessage());;
//        }

    //    protected List<Dataset> datasets = new ArrayList<Dataset>();
    //    protected Path userDatasetsFile;
    //
    //    public Path getUserDatasetsFile() {
    //        return userDatasetsFile;
    //    }
    //

//    public void importDataset(final String datasetName, final File localAvroSchemaFile, final File... localAvroFiles)
//            throws IOException, DatasetException {
//        try {
//            final Dataset dataset = new Dataset(datasetName, pprlClusterHdfs.getHomeDirectory());
//            dataset.buildOnFS(pprlClusterHdfs,ONLY_OWNER_PERMISSION);
//            uploadFileToHdfs(dataset.getAvroPath(),localAvroFiles);
//            uploadFileToHdfs(dataset.getAvroSchemaPath(),localAvroSchemaFile);
//            LOG.info("Dataset : {}, Base path        : {}", datasetName, dataset.getBasePath());
//            LOG.info("Dataset : {}, Avro path        : {}", datasetName, dataset.getAvroPath());
//            LOG.info("Dataset : {}, Avro Schema Path : {}", datasetName, dataset.getAvroSchemaPath());
//            addToDatasets(dataset);
//        } catch (IOException e) {
//            LOG.error(e.getMessage());
//            throw e;
//        } catch (DatasetException e) {
//            LOG.error(e.getMessage());
//            throw e;
//        }
//    }
//
//    public void importDblpXmlDataset(final String datasetName, final File localAvroSchemaFile, final File... localDblpFiles)
//            throws Exception {
//        try {
//            final Dataset dataset = new Dataset(datasetName, pprlClusterHdfs.getHomeDirectory());
//            dataset.buildOnFS(pprlClusterHdfs, false, true, true, ONLY_OWNER_PERMISSION);
//            final Path input = new Path(dataset.getBasePath() + "/xml");
//            uploadFileToHdfs(input,localDblpFiles);
//            uploadFileToHdfs(dataset.getAvroSchemaPath(),localAvroSchemaFile);
//            final Path output = dataset.getAvroPath();
//            LOG.info("Dataset : {}, Base path        : {}", datasetName, dataset.getBasePath());
//            LOG.info("Dataset : {}, XML path         : {}", datasetName, input);
//            LOG.info("Dataset : {}, Avro path        : {}", datasetName, output);
//            LOG.info("Dataset : {}, Avro Schema Path : {}", datasetName, dataset.getAvroSchemaPath());
//            runDblpXmlToAvroTool(input, output);
//            removeSuccessFile(output);
//            addToDatasets(dataset);
//        } catch (InterruptedException e) {
//            LOG.error(e.getMessage());
//            throw e;
//        } catch (IOException e) {
//            LOG.error(e.getMessage());
//            throw e;
//        } catch (ClassNotFoundException e) {
//            LOG.error(e.getMessage());
//            throw e;
//        } catch (SQLException e) {
//            LOG.error(e.getMessage());
//            throw e;
//        } catch (DatasetException e) {
//            LOG.error(e.getMessage());
//            throw e;
//        } catch (Exception e) {
//            LOG.error(e.getMessage());
//            throw e;
//        }
//    }
//
//
//    public String calculateDatasetStats(final String datasetName, final int Q)
//            throws Exception {
//        try {
//            final Dataset dataset = findDatasetByName(datasetName);
//            final Path statsPath = dataset.getStatsPath(Q);
//            if(pprlClusterHdfs.exists(statsPath)) pprlClusterHdfs.delete(statsPath,true);
//            LOG.info("Dataset : {}, Base path         : {}", datasetName, dataset.getBasePath());
//            LOG.info("Dataset : {}, Q                 : {}", datasetName, Q);
//            LOG.info("Dataset : {}, Stats path        : {}", datasetName, statsPath);
//            runGetDatasetsStatsTool(dataset.getAvroPath(), dataset.getSchemaFile(pprlClusterHdfs), statsPath, Q);
//            removeSuccessFile(statsPath);
//            return statsPath.toString();
//        } catch (DatasetException e) {
//            LOG.error(e.getMessage());
//            throw e;
//        } catch (IOException e) {
//            LOG.error(e.getMessage());
//            throw e;
//        } catch (Exception e) {
//            LOG.error(e.getMessage());
//            throw e;
//        }
//    }
//
//
//
//    public Map<String,double[]> readDatasetStats(final String datasetName, final int Q,
//                                                 final String[] selectedFieldNames)
//            throws IOException, DatasetException {
//        try {
//            final Dataset dataset = findDatasetByName(datasetName);
//            LOG.info("Dataset : {}, Base path         : {}", datasetName, dataset.getBasePath());
//            LOG.info("Dataset : {}, Q                 : {}", datasetName, Q);
//            LOG.info("Dataset : {}, Stats path        : {}", datasetName, dataset.getStatsPath(Q));
//            if(selectedFieldNames != null)
//                LOG.info("Dataset : {}, Stats path        : {}", datasetName, Arrays.toString(selectedFieldNames));
//            Map<String,double[]> stats = new HashMap<String, double[]>();
//            for(Map.Entry<String,DatasetStatsWritable> entry :
//                    dataset.getStats(pprlClusterHdfs,Q,selectedFieldNames).entrySet()){
//                stats.put(entry.getKey(),entry.getValue().getStats());
//            }
//            return stats;
//        } catch (DatasetException e) {
//            LOG.error(e.getMessage());
//            throw e;
//        } catch (IOException e) {
//            LOG.error(e.getMessage());
//            throw e;
//        }
//    }
//
//
//    public void uploadFileToHdfs(final Path path, final File...files)
//            throws IOException {
//        try{
//            uploadFileToHdfs(path, ONLY_OWNER_PERMISSION,files);
//        } catch (IOException e) {
//            LOG.error(e.getMessage());
//            throw e;
//        }
//    }
//
//    public List<String> listDatasets(final boolean onlyName) {
//        final List<String> strings = new ArrayList<String>();
//        if(onlyName) {
//            for (Dataset dataset : datasets) strings.add(dataset.getName());
//            return strings;
//        }
//        for (Dataset dataset : datasets) {
//            StringBuilder sb = new StringBuilder("name : ");
//            sb.append(dataset.getName());
//            List<String> paths = new ArrayList<String>();
//            paths.add(dataset.getAvroPath().toString());
//            paths.add(dataset.getAvroSchemaPath().toString());
//            sb.append(" | paths : ").append(paths);
//            strings.add(sb.toString());
//        }
//        return strings;
//    }
//
//    public List<String> sampleOfDataset(final String datasetName, int sampleSize)
//            throws IOException, DatasetException {
//        try {
//            final List<String> sampleStr = new ArrayList<String>(sampleSize);
//            final Dataset dataset = findDatasetByName(datasetName);
//            final List<GenericRecord> sample = sampleOfDataset(dataset, sampleSize);
//            for (GenericRecord record : sample)
//                sampleStr.add(record.toString());
//            return sampleStr;
//        } catch (DatasetException e) {
//            LOG.error(e.getMessage());
//            throw e;
//        } catch (IOException e) {
//            LOG.error(e.getMessage());
//            throw e;
//        }
//    }
//
//    public List<String> saveSampleOfDataset(final String datasetName, int sampleSize,
//                                            final String sampleName)
//            throws IOException, DatasetException {
//        try {
//            final List<String> sampleStr = new ArrayList<String>(sampleSize);
//            final Dataset dataset = findDatasetByName(datasetName);
//            final Schema schema = dataset.getSchema(pprlClusterHdfs);
//            final List<GenericRecord> sample = sampleOfDataset(dataset, sampleSize);
//            saveSampleOfDataset(sample, schema, sampleName);
//            for (GenericRecord record : sample)
//                sampleStr.add(record.toString());
//            return sampleStr;
//        } catch (DatasetException e) {
//            LOG.error(e.getMessage());
//            throw e;
//        } catch (IOException e) {
//            LOG.error(e.getMessage());
//            throw e;
//        }
//    }
//
//    public void dropDataset(final String datasetName, final boolean completeDrop)
//            throws IOException, DatasetException {
//        try {
//            final Dataset dataset = findDatasetByName(datasetName);
//            if (completeDrop) {
//                pprlClusterHdfs.delete(dataset.getAvroSchemaPath(), true);
//                pprlClusterHdfs.delete(dataset.getAvroPath(), true);
//                if (pprlClusterHdfs.listStatus(dataset.getBasePath()).length == 0)
//                    pprlClusterHdfs.delete(dataset.getBasePath(), true);
//            }
//            removeFromDatasets(dataset);
//        } catch (FileNotFoundException e) {
//            LOG.error(e.getMessage());
//            throw e;
//        } catch (DatasetException e) {
//            LOG.error(e.getMessage());
//            throw e;
//        } catch (IOException e) {
//            LOG.error(e.getMessage());
//            throw e;
//        }
//    }
//
//    public Map<String, String> describeDataset(final String datasetName)
//            throws IOException, DatasetException {
//        try {
//            final Dataset dataset = findDatasetByName(datasetName);
//            final Schema s = dataset.getSchema(pprlClusterHdfs);
//            final Map<String, String> description = new HashMap<String, String>();
//            for (Schema.Field f : s.getFields())
//                description.put(f.name(), f.schema().getType().toString());
//            return description;
//        } catch (DatasetException e) {
//            LOG.error(e.getMessage());
//            throw e;
//        } catch (IOException e) {
//            LOG.error(e.getMessage());
//            throw e;
//        }
//    }
//
//    public Dataset findDatasetByName(final String datasetName) throws DatasetException {
//        for (Dataset dataset : datasets) if (dataset.getName().equals(datasetName)) return dataset;
//        throw new DatasetException("Cannot find dataset with name " + datasetName);
//    }
//
//    protected void saveSampleOfDataset(final List<GenericRecord> sampleOfDataset,final Schema schema,
//                                       final String sampleName)
//            throws IOException, DatasetException {
//        final File sampleSchemaFile = new File(sampleName + ".avsc");
//        final File sampleDataFile = new File(sampleName + ".avro");
//        sampleSchemaFile.createNewFile();
//        sampleDataFile.createNewFile();
//        final PrintWriter schemaWriter = new PrintWriter(sampleSchemaFile);
//        schemaWriter.print(schema.toString(true));
//        schemaWriter.close();
//        DataFileWriter<GenericRecord> writer =
//                new DataFileWriter<GenericRecord>(new GenericDatumWriter<GenericRecord>(schema));
//        DataFileWriter<GenericRecord> sampleWriter = writer.create(schema,sampleDataFile);
//        for(GenericRecord record : sampleOfDataset)
//            sampleWriter.append(record);
//        sampleWriter.close();
//    }
//
//    protected List<GenericRecord> sampleOfDataset(final Dataset dataset, int sampleSize)
//            throws IOException, DatasetException {
//        final List<GenericRecord> sample = new ArrayList<GenericRecord>(sampleSize);
//        Dataset.DatasetRecordReader reader = dataset.getReader(pprlClusterHdfs);
//        while(reader.hasNext()) {
//            GenericRecord record = reader.next();
//            if ((new java.util.Random()).nextBoolean()) {
//                LOG.debug("Adding Record");
//                sample.add(record);
//                sampleSize--;
//                if (sampleSize == 0) break;
//            } else LOG.debug("Not adding record");
//        }
//        reader.close();
//        return sample;
//    }
//
//    protected void uploadFileToHdfs(final Path path, final FsPermission permission, final File...files)
//            throws IOException {
//        if(!pprlClusterHdfs.exists(path)) {
//            LOG.warn("Path {} does not exist. Creating it", path);
//            pprlClusterHdfs.mkdirs(path,ONLY_OWNER_PERMISSION);
//        }
//        for(File file : files) {
//            final Path source = new Path(file.toURI());
//            final Path destination = new Path(path + "/" + file.getName());
//            pprlClusterHdfs.copyFromLocalFile(source, destination);
//            pprlClusterHdfs.setPermission(destination, permission);
//            LOG.debug("File : {} copiedFromLocalFile to path {}", file.getAbsolutePath(), destination);
//        }
//    }
//
//    private void runGetDatasetsStatsTool(final Path inputPath, final Path inputSchemaPath,
//                                         final Path outputPath, final int Q) throws Exception {
//        LOG.info("input={} , inputSchema={}", inputPath, inputSchemaPath);
//        LOG.info("output={}", outputPath);
//        LOG.info("Q = {}",Q);
//        getDatasetStatsToolRunner.setArguments(inputPath.toString(), inputSchemaPath.toString(),
//                outputPath.toString(),String.valueOf(Q));
//        getDatasetStatsToolRunner.call();
//        pprlClusterHdfs.setPermission(outputPath, new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE, false));
//    }
//
//    private void runDblpXmlToAvroTool(final Path inputPath, final Path outputPath)
//            throws Exception {
//        LOG.info("input={}", inputPath);
//        LOG.info("output={}", outputPath);
//        LOG.info("Q = {}");
//        dblpXmlToAvroToolRunner.setArguments(inputPath.toString(), outputPath.toString());
//        dblpXmlToAvroToolRunner.call();
//        pprlClusterHdfs.setPermission(outputPath, new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE, false));
//    }
//
//    protected void removeSuccessFile(final Path path) throws IOException {
//        final Path p = new Path(path + "/_SUCCESS");
//        if (pprlClusterHdfs.exists(p)) {
//            pprlClusterHdfs.delete(p, false);
//            LOG.debug("{} removed", p);
//        }
//    }
//
//    protected void checkSite() throws DatasetException, IOException {
//        final Path homeDirectory = pprlClusterHdfs.getHomeDirectory();
//
//        if (!pprlClusterHdfs.exists(homeDirectory)) {
//            LOG.error("Cannot find Home directory on pprl site");
//            throw new DatasetException("Cannot find Home directory on pprl site");
//        }
//        if (!pprlClusterHdfs.getFileStatus(homeDirectory).getPermission().equals(ONLY_OWNER_PERMISSION))
//            LOG.warn("Home directories permissions should be {}",ONLY_OWNER_PERMISSION);
//    }
//
//    protected void loadDatasets() throws IOException, DatasetException {
//        LOG.debug("Loading datasets from " + userDatasetsFile);
//        if (pprlClusterHdfs.exists(userDatasetsFile)) {
//            final BufferedReader br =
//                    new BufferedReader(new InputStreamReader(pprlClusterHdfs.open(userDatasetsFile)));
//            try {
//                String line;
//                line = br.readLine();
//                while (line != null) {
//                    LOG.debug("read line : {}", line);
//                    Dataset dataset = Dataset.fromString(line);
//                    datasets.add(dataset);
//                    line = br.readLine();
//                }
//            } finally {
//                br.close();
//            }
//        } else FileSystem.create(pprlClusterHdfs, userDatasetsFile, ONLY_OWNER_PERMISSION);
//    }
//
//    protected void saveDatasets() throws IOException, DatasetException {
//        LOG.debug("Saving datasets to " + userDatasetsFile);
//        if (!pprlClusterHdfs.exists(userDatasetsFile))
//            FileSystem.create(pprlClusterHdfs, userDatasetsFile, ONLY_OWNER_PERMISSION);
//        final BufferedWriter bw =
//                new BufferedWriter(new OutputStreamWriter(pprlClusterHdfs.create(userDatasetsFile)));
//        try {
//            for (Dataset dataset : datasets) {
//                final String line = Dataset.toString(dataset);
//                LOG.debug("Writing line : {}", line);
//                bw.write(line + "\n");
//            }
//        } finally {
//            bw.close();
//        }
//    }
//
//    protected void addToDatasets(final Dataset d) throws IOException, DatasetException {
//        if (!datasets.contains(d)) datasets.add(d);
//        saveDatasets();
//        LOG.debug("Dataset : {}, Added to datasets.", d.getName());
//    }
//
//    protected void removeFromDatasets(final Dataset d) throws IOException, DatasetException {
//        if (datasets.contains(d)) datasets.remove(d);
//        saveDatasets();
//        LOG.debug("Dataset : {}, Removed from datasets.", d.getName());
//    }
}
