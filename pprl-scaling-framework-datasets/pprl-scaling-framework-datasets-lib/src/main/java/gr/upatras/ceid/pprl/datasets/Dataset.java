package gr.upatras.ceid.pprl.datasets;

public class Dataset {
    protected int id;
    protected String name;
    protected String basePath;
    protected String avroPath;
    protected String avroSchemaPath;
    private DatasetStatistics statistics;

    //protected Path basePath;
    //protected Path avroPath;
    //protected Path avroSchemaPath;
    //protected Schema schema;

    public Dataset() {}

    public Dataset(final String name, final String basePath) {
        this(name, basePath + "/" + name,
                basePath + "/" + name + "/avro",
                basePath + "/" + name + "/schema");
    }

    public Dataset(final String name, final String basePath,
                   final String avroPath, final String avroSchemaPath) {
        this.name = name;
        this.basePath = basePath;
        this.avroPath = avroPath;
        this.avroSchemaPath = avroSchemaPath;
    }

    public String getName() {
        return name;
    }

    public void setName(final String name) { this.name = name; }

    public String getBasePath() {
        return basePath;
    }

    public void setBasePath(final String path) { basePath = path; }

    public String getAvroPath() {
        return avroPath;
    }

    public void setAvroPath(final String path) {
            avroPath = path;
        }

    public String getAvroSchemaPath() {
        return avroSchemaPath;
    }

    public void setAvroSchemaPath(final String path) { avroSchemaPath = path; }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public DatasetStatistics getStatistics() {
        return statistics;
    }

    public void setStatistics(DatasetStatistics statistics) {
        this.statistics = statistics;
    }

    //    public void buildOnFS(final FileSystem fs, FsPermission permission)
//            throws IOException, DatasetException {
//        buildOnFS(fs,true,permission);
//    }

//    public void buildOnFS(final FileSystem fs, final boolean makeSubDirs, FsPermission permission )
//            throws IOException, DatasetException {
//        buildOnFS(fs,makeSubDirs,makeSubDirs,true,permission);
//    }
//
//    public void buildOnFS(final FileSystem fs, final boolean makeAvroDir,
//                          final boolean makeSchemaDir , final boolean overwrite, FsPermission permission)
//            throws IOException, DatasetException{
//
//        boolean datasetExists = existsOnFS(fs,true);
//        if (datasetExists && overwrite) {
//            fs.delete(basePath, true);
//        } else if(datasetExists) {
//            throw new DatasetException("Dataset base path already exists!");
//        }
//
//        fs.mkdirs(basePath, permission);
//
//        if (makeAvroDir) {
//            fs.mkdirs(avroPath, permission);
//        }
//
//        if (makeSchemaDir) {
//            fs.mkdirs(avroSchemaPath, permission);
//        }
//    }
//
//    public boolean existsOnFS(final FileSystem fs, final boolean checkOnlyBasePath)
//            throws IOException {
//        if (checkOnlyBasePath) return fs.exists(basePath);
//        else return fs.exists(basePath) &&
//                fs.exists(avroPath) &&
//                fs.exists(avroSchemaPath);
//    }

//    public static String toString(final Dataset dataset)
//            throws DatasetException {
//        if(!dataset.isValid()) throw new DatasetException("Dataset is invalid.");
//        return String.format("%s => %s %s %s",
//                dataset.getName(), dataset.getBasePath(),
//                dataset.getAvroPath(), dataset.getAvroSchemaPath());
//    }

//    public static Dataset fromString(final String s) throws DatasetException {
//        final String[] parts = s.split(" => ");
//        if(parts.length != 2) throw new DatasetException("String \"" + s + "\" is invalid dataset string.");
//        String name = parts[0];
//        final String[] paths = parts[1].split(" ");
//        if(paths.length != 3) throw new DatasetException("String \"" + s + "\" is invalid dataset string.");
//        Path basePath = new Path(paths[0]);
//        Path avroPath = new Path(paths[1]);
//        Path avroSchemaPath = new Path(paths[2]);
//        return new Dataset(name,basePath,avroPath,avroSchemaPath);
//    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Dataset dataset = (Dataset) o;

        if (!name.equals(dataset.name)) return false;
        if (!basePath.equals(dataset.basePath)) return false;
        if (!avroPath.equals(dataset.avroPath)) return false;
        return avroSchemaPath.equals(dataset.avroSchemaPath);
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + basePath.hashCode();
        result = 31 * result + avroPath.hashCode();
        result = 31 * result + avroSchemaPath.hashCode();
        return result;
    }

//    public DatasetsUtil.DatasetRecordReader getReader(final FileSystem fs) throws IOException, DatasetException {
//        if(schema == null) {
//            getSchema(fs);
//            if (schema == null) {
//                throw new DatasetException("Schema cannot be null");
//            }
//        }
//        return new DatasetsUtil.DatasetRecordReader(fs,schema,avroPath);
//    }

//    public Path getStatsPath(final int Q) { return new Path(getBasePath() + "/" + String.format("stats_%d",Q)); }
//
//    public Map<String,DatasetStatistics> getStats(final FileSystem fs, final int Q,final String[] selectedFieldNames)
//            throws IOException, DatasetException {
//
//        final Path statsParentPath = getStatsPath(Q);
//        if(!fs.exists(statsParentPath))
//            throw new DatasetException("Cannot find datasets stats path " + statsParentPath + ".");
//        if(fs.listStatus(statsParentPath).length == 0)
//            throw new DatasetException("Empty stats path " + statsParentPath + ".");
//        final Path statsFile =  fs.listStatus(statsParentPath)[0].getPath();
//
//        SequenceFile.Reader reader = new SequenceFile.Reader(fs.getConf(), SequenceFile.Reader.file(statsFile));
//        final Map<String,DatasetStatistics> stats = new HashMap<String,DatasetStatistics>();
//        Text key = new Text();
//        DatasetStatistics value = new DatasetStatistics();
//
//        while(reader.next(key,value)) {
//            if(selectedFieldNames == null ||
//                    selectedFieldNames.length == 0 ||
//                    Arrays.asList(selectedFieldNames).contains(key.toString()))
//                stats.put(key.toString(),value);
//        }
//        reader.close();
//        return stats;
//    }
}