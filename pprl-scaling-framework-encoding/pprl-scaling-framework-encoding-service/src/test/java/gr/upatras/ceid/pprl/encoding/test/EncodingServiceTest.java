package gr.upatras.ceid.pprl.encoding.test;

//@ContextConfiguration(locations = "classpath:encoding-test-context.xml",loader=HadoopDelegatingSmartContextLoader.class)
//@MiniHadoopCluster(nodes = 1, id = "encoding_service_test")
public class EncodingServiceTest { //  extends AbstractMapReduceTests {

//    private static Logger LOG = LoggerFactory.getLogger(EncodingServiceTest.class);

//    @Autowired
//    private DatasetsService datasetsService;
//
//    @Autowired
//    private EncodingService encodingService;
//
//    private Path datasetsFile;
//    private Path encodingsFile;
//
//    private int datasetsCount = 0;
//    private int encodingsCount = 0;
//
//    @Test
//    public void test0() throws IOException, BloomFilterEncodingException, URISyntaxException {
//        encodeLocalFile();
//    }
//
//    @Test
//    public void test1() throws IOException, DatasetException, URISyntaxException, BloomFilterEncodingException {
//        checkForSavingFiles();
//
//        importLocalDatasets("dblp",new String[]{"dblp/avro/dblp.avro","dblp/schema/dblp.avsc"});
//        importLocalDatasets("random",new String[]{"random/avro/random.avro","random/schema/random.avsc"});
//        for(String s : datasetsService.listDatasets(false)) {
//            LOG.info("\t {}",s);
//        }
//
//        List<String> importedEncodingNames = new ArrayList<String>();
//        for (String enc : new String[]{"static_FBF","dynamic_FBF",
//                "uniform_RBF","weighted_RBF"}) {
//            final String method = enc.split("_")[1];
//            importedEncodingNames.add("enc_orphan_" + enc);
//            importOrphanEncodedDataset("enc_orphan_" + enc,
//                    method, new String[]{
//                            "dblp/" + enc + "/avro/" + enc + ".avro",
//                            "dblp/" + enc + "/schema/" + enc + ".avsc"
//                    });
//            importedEncodingNames.add("enc_non_orphan_" + enc);
//            importNotOrphanEncodedDataset(
//                    "enc_non_orphan_" + enc, "dblp",
//                    method, new String[]{
//                            "dblp/" + enc + "/avro/" + enc + ".avro",
//                            "dblp/" + enc + "/schema/" + enc + ".avsc"
//                    }
//            );
//        }
//
//        for(String s : encodingService.listDatasets(false)) {
//            LOG.info("\t {}",s);
//        }
//        for(String name : importedEncodingNames) {
//            describe(name);
//            getSample(name, 5);
//        }
//    }
//
//    @Test
//    public void test2() throws IOException, DatasetException, BloomFilterEncodingException {
//        checkForSavingFiles();
//        dropEncoding("enc_orphan_static_FBF");
//        for(String s : encodingService.listDatasets(false)) {
//            LOG.info("\t {}",s);
//        }
//    }
//
//
//    @Test
//    public void test3() throws URISyntaxException, BloomFilterEncodingException, DatasetException, IOException {
//        getSampleAndEncodeIt();
//    }
//
//    private void getSample(String name,int size) throws DatasetException, IOException {
//        LOG.info("Sample({}) of {}",size,name);
//        for(String sample : encodingService.sampleOfDataset(name,size)) LOG.info("\t{}", sample);
//    }
//
//    private void describe(String name) throws DatasetException, IOException {
//        Map<String,String> schema = encodingService.describeDataset(name);
//        for(Map.Entry<String,String> field : schema.entrySet())
//            LOG.info("\t {} {}",field.getKey(),field.getValue());
//    }
//
//    private void checkForSavingFiles() throws IOException, EncodedDatasetException, BloomFilterEncodingException {
//        if(datasetsFile == null ) {
//            datasetsFile = datasetsService.getUserDatasetsFile();
//            LOG.info("Setting datasets file : {}",datasetsFile);
//        }
//        if(encodingsFile == null) {
//            encodingsFile = encodingService.getUserDatasetsFile();
//            LOG.info("Setting encodings file : {}",encodingsFile);
//        }
//
//        assertTrue(getFileSystem().exists(datasetsFile));
//        assertTrue(getFileSystem().exists(encodingsFile));
//        long dlen = getFileSystem().getFileStatus(datasetsFile).getLen();
//        LOG.info("Checking datasets file size : {} bytes",dlen);
//        long elen = getFileSystem().getFileStatus(encodingsFile).getLen();
//        LOG.info("Checking encodings file size : {} bytes",elen);
//
//        datasetsCount = datasetsService.listDatasets(true).size();
//        encodingsCount = encodingService.listDatasets(true).size();
//        LOG.info("Found {} datasets.",datasetsCount);
//        LOG.info("Found {} encodings.",encodingsCount);
//    }
//
//    private void importLocalDatasets(final String name, final String[] paths)
//            throws URISyntaxException, IOException, DatasetException {
//        File localAvroFile = new File(paths[0]);
//        File localAvroSchemaFile = new File(paths[1]);
//        datasetsService.importDataset(name,localAvroSchemaFile,localAvroFile);
//        datasetsCount++;
//        assertEquals(datasetsCount,datasetsService.listDatasets(true).size());
//        long len = getFileSystem().getFileStatus(datasetsFile).getLen();
//        LOG.info("After import datasets file size : {} bytes",len);
//    }
//
//    private void importOrphanEncodedDataset(final String name, final String methodName,
//                                            final String[] paths)
//            throws DatasetException, BloomFilterEncodingException, IOException, URISyntaxException {
//        File localAvroFile = new File(paths[0]);
//        File localAvroSchemaFile = new File(paths[1]);
//        encodingService.importOrphanEncodedDataset(name,methodName,localAvroSchemaFile,localAvroFile);
//        encodingsCount++;
//        assertEquals(encodingsCount,encodingService.listDatasets(true).size());
//        long len = getFileSystem().getFileStatus(encodingsFile).getLen();
//        LOG.info("After import encoded datasets file size : {} bytes",len);
//    }
//
//    private void importNotOrphanEncodedDataset(final String name, final String datasetName,
//                                               final String methodName,
//                                               final String[] paths)
//            throws URISyntaxException, DatasetException, BloomFilterEncodingException, IOException {
//        File localAvroFile = new File(paths[0]);
//        File localAvroSchemaFile = new File(paths[1]);
//        encodingService.importEncodedDataset(name, datasetName, methodName, localAvroSchemaFile, localAvroFile);
//        encodingsCount++;
//        assertEquals(encodingsCount,encodingService.listDatasets(true).size());
//        long len = getFileSystem().getFileStatus(encodingsFile).getLen();
//        LOG.info("After import encoded datasets file size : {} bytes",len);
//    }
//
//    private void dropEncoding(String name)
//            throws IOException, DatasetException {
//        encodingService.dropDataset(name,true);
//        encodingsCount--;
//        assertEquals(encodingsCount,encodingService.listDatasets(true).size());
//        long len = getFileSystem().getFileStatus(encodingsFile).getLen();
//        LOG.info("After drop encoded datasets file size : {} bytes",len);
//    }
//
//    private void encodeLocalFile() throws URISyntaxException, IOException, BloomFilterEncodingException {
//        final String[] dblpPaths = new String[]{"dblp/avro/dblp.avro","dblp/schema/dblp.avsc"};
//        final Set<File> files = new TreeSet<File>();
//        files.add(new File(dblpPaths[0]));
//        final File schemaFile = new File(dblpPaths[1]);
//        final String[] SELECTED_FIELDS = new String[]{"author","title"};
//        final String[] REST_FIELDS = new String[]{"key"};
//        final int K = 30;
//        final int Q = 2;
//        for(String method : (new String[]{"FBF","RBF"})) {
//            for(int N : (new int[]{1024,500,-1})) {
//                final String[] paths;
//                final String name = null;
//                if(method.equals("FBF") && N > 0)
//                    paths = encodingService.encodeFBFStaticLocalFile(name,
//                            files,schemaFile,
//                            SELECTED_FIELDS,REST_FIELDS,
//                            N,K,Q);
//                else if(method.equals("RBF") && N > 0) {
//                    paths = encodingService.encodeRBFUniformLocalFile(name,
//                            files, schemaFile,
//                            SELECTED_FIELDS, REST_FIELDS,
//                            N,K,Q);
//                } else if(method.equals("FBF") && N < 0) {
//                    paths = encodingService.encodeFBFDynamicLocalFile(name,
//                            files, schemaFile,
//                            SELECTED_FIELDS, REST_FIELDS,
//                            K,Q);
//                } else if(method.equals("RBF") && N <= 0) {
//                    paths = new String[0];
////                    paths = encodingService.encodeRBFWeightedLocalFile(name,
////                            files, schemaFile,
////                            SELECTED_FIELDS, REST_FIELDS,
////                            new double[0], K, Q);
//                } else {
//                    paths = new String[0];
//                }
//                LOG.info(Arrays.toString(paths));
//            }
//        }
//    }
//
//    private void getSampleAndEncodeIt() throws DatasetException, IOException, URISyntaxException, BloomFilterEncodingException {
//        datasetsService.saveSampleOfDataset("dblp", 5, "dblp_sample");
//        File[] dblpPaths = {
//                new File("dblp_sample.avsc"),
//                new File("dblp_sample.avro")
//        };
//        final Set<File> files = new TreeSet<File>();
//        files.add(dblpPaths[1]);
//        final File schemaFile = dblpPaths[0];
//        final String[] SELECTED_FIELDS = new String[]{"author","title"};
//        final String[] REST_FIELDS = new String[]{"key","year"};
//        final int K = 30;
//        final int Q = 2;
//        for(String method : (new String[]{"FBF","RBF"})) {
//            for(int N : (new int[]{1024,500,-1})) {
//                final String[] paths;
//                final String name = String.format("%s_%s_dblp_sample",method,(
//                        N > 0) ?
//                        ((method.equals("RBF")) ? "uniform" : "static") :
//                        ((method.equals("RBF")) ? "weighted" : "dynamic"));
//                if(method.equals("FBF") && N > 0)
//                    paths = encodingService.encodeFBFStaticLocalFile(name,
//                            files,schemaFile,
//                            SELECTED_FIELDS,REST_FIELDS,
//                            N,K,Q);
//                else if(method.equals("RBF") && N > 0) {
//                    paths = encodingService.encodeRBFUniformLocalFile(name,
//                            files, schemaFile,
//                            SELECTED_FIELDS, REST_FIELDS,
//                            N,K,Q);
//                } else if(method.equals("FBF") && N < 0) {
//                    paths = encodingService.encodeFBFDynamicLocalFile(name,
//                            files, schemaFile,
//                            SELECTED_FIELDS, REST_FIELDS,
//                            K,Q);
//                } else if(method.equals("RBF") && N <= 0) {
//                    paths = new String[0];
////                    paths = encodingService.encodeRBFWeightedLocalFile(name,
////                            files, schemaFile,
////                            SELECTED_FIELDS, REST_FIELDS,
////                            new double[0], K, Q);
//                } else {
//                    paths = new String[0];
//                }
//                LOG.info(Arrays.toString(paths));
//            }
//        }
//    }
}
