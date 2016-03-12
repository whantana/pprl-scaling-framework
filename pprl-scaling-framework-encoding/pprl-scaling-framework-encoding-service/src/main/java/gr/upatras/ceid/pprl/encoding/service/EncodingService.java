package gr.upatras.ceid.pprl.encoding.service;

import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.hadoop.mapreduce.ToolRunner;
import org.springframework.stereotype.Service;

@Service
public class EncodingService implements InitializingBean{

    private static final Logger LOG = LoggerFactory.getLogger(EncodingService.class);

    // @Autowired(required = false)
    // private ToolRunner encodeDatasetToolRunner;

    public void afterPropertiesSet() {
        LOG.info("Encoding service initialized.");
    }

//        try {
//            checkSite();
//            userDatasetsFile = new Path(pprlClusterHdfs.getHomeDirectory() + "/.pprl_encodings");
//            loadDatasets();
//            LOG.info("Service is now initialized. Found {} datasets on the PPRL site.", datasets.size());
//        } catch (EncodedDatasetException e) {
//            LOG.error(e.getMessage());
//        } catch (DatasetException e) {
//            LOG.error(e.getMessage());
//        } catch (IOException e) {
//            LOG.error(e.getMessage());
//        }
//

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
//            if(!dataset.isValid()) continue;
//            if(((EncodedDataset) dataset).isNotOrphan())
//                sb.append(" | source dataset :")
//                        .append(((EncodedDataset) dataset).getDatasetName());
//            sb.append(" | encoding method :")
//                    .append(((EncodedDataset) dataset).getEncoding().getFullName());
//            strings.add(sb.toString());
//        }
//        return strings;
//    }
//
//    public List<String> listDatasets(final String datasetName, final String methodName)
//            throws EncodedDatasetException, BloomFilterEncodingException {
//        try {
//            final List<EncodedDataset> filteredEncodedDatasets;
//            if (datasetName != null && methodName == null) {
//                filteredEncodedDatasets = filterEncodedDatasetsByDatasetName(datasetName);
//            } else if (datasetName == null && methodName != null) {
//                filteredEncodedDatasets = filterEncodedDatasetsByMethodName(methodName);
//            } else if (datasetName != null) {
//                filteredEncodedDatasets = filterEncodedDatasetsByNames(datasetName, methodName);
//            } else throw new EncodedDatasetException("Invalid input datasetName==methodName==null");
//            final List<String> strings = new ArrayList<String>();
//            for (EncodedDataset encodedDataset : filteredEncodedDatasets) {
//                StringBuilder sb = new StringBuilder("name : ");
//                sb.append(encodedDataset.getName());
//                List<String> paths = new ArrayList<String>();
//                paths.add(encodedDataset.getAvroPath().toString());
//                paths.add(encodedDataset.getAvroSchemaPath().toString());
//                sb.append(" | paths : ").append(paths);
//                if (encodedDataset.isNotOrphan()) {
//                    sb.append(" | source dataset :")
//                            .append(encodedDataset.getDatasetName());
//                }
//                sb.append(" | encoding method :")
//                        .append(encodedDataset.getEncoding().getFullName());
//                strings.add(sb.toString());
//            }
//            return strings;
//        } catch (EncodedDatasetException e) {
//            LOG.error(e.getMessage());
//            throw(e);
//        } catch (BloomFilterEncodingException e) {
//            LOG.error(e.getMessage());
//            throw(e);
//        }
//    }
//
//
//    public void importOrphanEncodedDataset(String name, final String methodName,
//                                           final File localAvroSchemaFile, final File... localAvroFiles)
//            throws IOException, DatasetException, BloomFilterEncodingException {
//        try {
//
//            BloomFilterEncoding encoding = BloomFilterEncoding.newInstanceOfMethod(methodName);
//            encoding.setupFromSchema(localDatasetsService.loadAvroSchemaFromFile(localAvroSchemaFile));
//
//            if(name == null) name = encoding.getFullName().toLowerCase();
//
//            final EncodedDataset encodedDataset = new EncodedDataset(name, pprlClusterHdfs.getHomeDirectory());
//            encodedDataset.buildOnFS(pprlClusterHdfs, ONLY_OWNER_PERMISSION);
//            uploadFileToHdfs(encodedDataset.getAvroPath(), localAvroFiles);
//            uploadFileToHdfs(encodedDataset.getAvroSchemaPath(), localAvroSchemaFile);
//            encodedDataset.setEncoding(encoding);
//
//            LOG.info("EncodedDataset : {}, Base path        : {}", name, encodedDataset.getBasePath());
//            LOG.info("EncodedDataset : {}, Avro path        : {}", name, encodedDataset.getAvroPath());
//            LOG.info("EncodedDataset : {}, Avro Schema Path : {}", name, encodedDataset.getAvroSchemaPath());
//            LOG.info("EncodedDataset : {}, Encoding         : {}", name, encoding.getFullName());
//
//            addToDatasets(encodedDataset);
//        } catch (DatasetException e) {
//            LOG.error(e.getMessage());
//            throw e;
//        } catch (IOException e) {
//            LOG.error(e.getMessage());
//            throw e;
//        } catch (BloomFilterEncodingException e) {
//            LOG.error(e.getMessage());
//            throw e;
//        }
//    }
//
//    public void importEncodedDataset(String name, final String sourceDatasetName,
//                                     final String methodName,
//                                     final File localAvroSchemaFile, final File... localAvroFiles)
//            throws IOException, DatasetException, BloomFilterEncodingException {
//        try {
//
//            if(sourceDatasetName == null) {
//                importOrphanEncodedDataset(name, methodName, localAvroSchemaFile, localAvroFiles);
//                return;
//            }
//            final Dataset sourceDataset = datasetsService.findDatasetByName(sourceDatasetName);
//
//            BloomFilterEncoding encoding = BloomFilterEncoding.newInstanceOfMethod(methodName);
//            encoding.setupFromSchema(localDatasetsService.loadAvroSchemaFromFile(localAvroSchemaFile));
//            if(!encoding.isEncodingOfSchema(sourceDataset.getSchema(pprlClusterHdfs)))
//                throw new BloomFilterEncodingException("Encoding does not validate with source dataset.");
//
//            if(name == null) name = encoding.getFullName().toLowerCase();
//
//            final Path basePath = new Path(sourceDataset.getBasePath() + "/" + name);
//            final Path avroPath = new Path(basePath + "/avro");
//            final Path avroSchemaPath = new Path(basePath + "/schema");
//            final EncodedDataset encodedDataset =
//                    new EncodedDataset(name, sourceDatasetName, basePath, avroPath, avroSchemaPath);
//            encodedDataset.buildOnFS(pprlClusterHdfs, ONLY_OWNER_PERMISSION);
//            uploadFileToHdfs(encodedDataset.getAvroPath(), localAvroFiles);
//            uploadFileToHdfs(encodedDataset.getAvroSchemaPath(), localAvroSchemaFile);
//            encodedDataset.setEncoding(encoding);
//
//            LOG.info("EncodedDataset : {}, Base path                       : {}", name, encodedDataset.getBasePath());
//            LOG.info("EncodedDataset : {}, Avro path                       : {}", name, encodedDataset.getAvroPath());
//            LOG.info("EncodedDataset : {}, Avro Schema Path                : {}", name, encodedDataset.getAvroSchemaPath());
//            LOG.info("EncodedDataset : {}, Source dataset name             : {}", name, sourceDataset.getName());
//            LOG.info("EncodedDataset : {}, Source dataset base path        : {}", name, sourceDataset.getBasePath());
//            LOG.info("EncodedDataset : {}, Source dataset avro path        : {}", name, sourceDataset.getAvroPath());
//            LOG.info("EncodedDataset : {}, Source dataset avro schema path : {}", name, sourceDataset.getAvroSchemaPath());
//            LOG.info("EncodedDataset : {}, Encoding                        : {}", name, encoding.getFullName());
//
//            addToDatasets(encodedDataset);
//        } catch (EncodedDatasetException e) {
//            LOG.error(e.getMessage());
//            throw e;
//        } catch (BloomFilterEncodingException e) {
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
//    public void encodeFBFStaticImportedDataset(String name,
//                                               final String sourceDatasetName,
//                                               final String[] selectedFieldNames, String[] restFieldNames,
//                                               final int N, final int K, final int Q) throws Exception {
//        try {
//            final Dataset sourceDataset = datasetsService.findDatasetByName(sourceDatasetName);
//            final FieldBloomFilterEncoding encoding = new FieldBloomFilterEncoding(N,selectedFieldNames.length,K,Q);
//            if(restFieldNames == null) restFieldNames = new String[0];
//            encoding.makeFromSchema(sourceDataset.getSchema(pprlClusterHdfs), selectedFieldNames, restFieldNames);
//            if(!encoding.isEncodingOfSchema(sourceDataset.getSchema(pprlClusterHdfs)))
//                throw new BloomFilterEncodingException("Encoding does not validate with source dataset.");
//            if(name == null) name = encoding.getFullName().toLowerCase();
//
//            final Path basePath = new Path(sourceDataset.getBasePath() + "/" + name);
//            final Path avroPath = new Path(basePath + "/avro");
//            final Path avroSchemaPath = new Path(basePath + "/schema");
//            final EncodedDataset encodedDataset =
//                    new EncodedDataset(name, sourceDatasetName, basePath, avroPath, avroSchemaPath);
//            encodedDataset.buildOnFS(pprlClusterHdfs, false, true, true, ONLY_OWNER_PERMISSION);
//            encodedDataset.setEncoding(encoding);
//            encodedDataset.writeSchemaOnHdfs(pprlClusterHdfs);
//
//            LOG.info("EncodedDataset : {}, Base path                         : {}", name, encodedDataset.getBasePath());
//            LOG.info("EncodedDataset : {}, Avro path                         : {}", name, encodedDataset.getAvroPath());
//            LOG.info("EncodedDataset : {}, Avro Schema Path                  : {}", name, encodedDataset.getAvroSchemaPath());
//            LOG.info("EncodedDataset : {}, Source dataset name               : {}", name, sourceDataset.getName());
//            LOG.info("EncodedDataset : {}, Source dataset base path          : {}", name, sourceDataset.getBasePath());
//            LOG.info("EncodedDataset : {}, Source dataset avro path          : {}", name, sourceDataset.getAvroPath());
//            LOG.info("EncodedDataset : {}, Source dataset avro schema path   : {}", name, sourceDataset.getAvroSchemaPath());
//            LOG.info("EncodedDataset : {}, Selected fields to encode         : {}", name, selectedFieldNames);
//            if(restFieldNames.length > 0)
//                LOG.info("EncodedDataset : {}, Rest of fields in encoded dataset : {}", name, restFieldNames);
//            LOG.info("EncodedDataset : {}, Encoding                          : {}", name, encoding.getFullName());
//
//            runEncodeDatasetTool(
//                    sourceDataset.getAvroPath(), sourceDataset.getSchemaFile(pprlClusterHdfs),
//                    encodedDataset.getAvroPath(), encodedDataset.getSchemaFile(pprlClusterHdfs));
//            removeSuccessFile(encodedDataset.getAvroPath());
//            addToDatasets(encodedDataset);
//        } catch (DatasetException e) {
//            LOG.error(e.getMessage());
//            throw e;
//        } catch (BloomFilterEncodingException e) {
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
//    public void encodeFBFDynamicImportedDataset(String name,
//                                                final String sourceDatasetName,
//                                                final String[] selectedFieldNames, String[] restFieldNames,
//                                                final int K, final int Q) throws Exception {
//        try {
//            final Dataset sourceDataset = datasetsService.findDatasetByName(sourceDatasetName);
//            final Map<String, DatasetStatsWritable> stats =
//                    sourceDataset.getStats(pprlClusterHdfs, Q, selectedFieldNames);
//            final double[] avgQcounts = new double[selectedFieldNames.length];
//            int i =0 ;
//            for(String fieldName : selectedFieldNames)
//                avgQcounts[i] = stats.get(fieldName).getQgramCount();
//
//            final FieldBloomFilterEncoding encoding = new FieldBloomFilterEncoding(avgQcounts,K,Q);
//            if(restFieldNames == null) restFieldNames = new String[0];
//            encoding.makeFromSchema(sourceDataset.getSchema(pprlClusterHdfs), selectedFieldNames, restFieldNames);
//            if(!encoding.isEncodingOfSchema(sourceDataset.getSchema(pprlClusterHdfs)))
//                throw new BloomFilterEncodingException("Encoding does not validate with source dataset.");
//            if(name == null) name = encoding.getFullName().toLowerCase();
//
//            final Path basePath = new Path(sourceDataset.getBasePath() + "/" + name);
//            final Path avroPath = new Path(basePath + "/avro");
//            final Path avroSchemaPath = new Path(basePath + "/schema");
//            final EncodedDataset encodedDataset =
//                    new EncodedDataset(name, sourceDatasetName, basePath, avroPath, avroSchemaPath);
//            encodedDataset.buildOnFS(pprlClusterHdfs, false, true, true, ONLY_OWNER_PERMISSION);
//            encodedDataset.setEncoding(encoding);
//            encodedDataset.writeSchemaOnHdfs(pprlClusterHdfs);
//
//            LOG.info("EncodedDataset : {}, Base path                         : {}", name, encodedDataset.getBasePath());
//            LOG.info("EncodedDataset : {}, Avro path                         : {}", name, encodedDataset.getAvroPath());
//            LOG.info("EncodedDataset : {}, Avro Schema Path                  : {}", name, encodedDataset.getAvroSchemaPath());
//            LOG.info("EncodedDataset : {}, Source dataset name               : {}", name, sourceDataset.getName());
//            LOG.info("EncodedDataset : {}, Source dataset base path          : {}", name, sourceDataset.getBasePath());
//            LOG.info("EncodedDataset : {}, Source dataset avro path          : {}", name, sourceDataset.getAvroPath());
//            LOG.info("EncodedDataset : {}, Source dataset avro schema path   : {}", name, sourceDataset.getAvroSchemaPath());
//            LOG.info("EncodedDataset : {}, Selected fields to encode         : {}", name, selectedFieldNames);
//            if(restFieldNames.length > 0)
//                LOG.info("EncodedDataset : {}, Rest of fields in encoded dataset : {}", name, restFieldNames);
//            LOG.info("EncodedDataset : {}, Encoding                          : {}", name, encoding.getFullName());
//
//            runEncodeDatasetTool(
//                    sourceDataset.getAvroPath(), sourceDataset.getSchemaFile(pprlClusterHdfs),
//                    encodedDataset.getAvroPath(), encodedDataset.getSchemaFile(pprlClusterHdfs));
//            removeSuccessFile(encodedDataset.getAvroPath());
//            addToDatasets(encodedDataset);
//        } catch (DatasetException e) {
//            LOG.error(e.getMessage());
//            throw e;
//        } catch (IOException e) {
//            LOG.error(e.getMessage());
//            throw e;
//        } catch (BloomFilterEncodingException e) {
//            LOG.error(e.getMessage());
//            throw e;
//        } catch (Exception e) {
//            LOG.error(e.getMessage());
//            throw e;
//        }
//    }
//
//    public void encodeRBFUniformImportedDataset(String name, final String sourceDatasetName,
//                                                final String[] selectedFieldNames, String[] restFieldNames,
//                                                final int N, final int K, final int Q)
//            throws Exception {
//        try {
//            final Dataset sourceDataset = datasetsService.findDatasetByName(sourceDatasetName);
//            final Map<String, DatasetStatsWritable> stats =
//                    sourceDataset.getStats(pprlClusterHdfs, Q, selectedFieldNames);
//            final double[] avgQcounts = new double[selectedFieldNames.length];
//            int i =0 ;
//            for(String fieldName : selectedFieldNames)
//                avgQcounts[i] = stats.get(fieldName).getQgramCount();
//
//            final RowBloomFilterEncoding encoding = new RowBloomFilterEncoding(avgQcounts,N,K,Q);
//            if(restFieldNames == null) restFieldNames = new String[0];
//            encoding.makeFromSchema(sourceDataset.getSchema(pprlClusterHdfs), selectedFieldNames, restFieldNames);
//            if(!encoding.isEncodingOfSchema(sourceDataset.getSchema(pprlClusterHdfs)))
//                throw new BloomFilterEncodingException("Encoding does not validate with source dataset.");
//            if(name == null) name = encoding.getFullName().toLowerCase();
//
//            final Path basePath = new Path(sourceDataset.getBasePath() + "/" + name);
//            final Path avroPath = new Path(basePath + "/avro");
//            final Path avroSchemaPath = new Path(basePath + "/schema");
//            final EncodedDataset encodedDataset =
//                    new EncodedDataset(name, sourceDatasetName, basePath, avroPath, avroSchemaPath);
//            encodedDataset.buildOnFS(pprlClusterHdfs, false, true, true, ONLY_OWNER_PERMISSION);
//            encodedDataset.setEncoding(encoding);
//            encodedDataset.writeSchemaOnHdfs(pprlClusterHdfs);
//
//            LOG.info("EncodedDataset : {}, Base path                         : {}", name, encodedDataset.getBasePath());
//            LOG.info("EncodedDataset : {}, Avro path                         : {}", name, encodedDataset.getAvroPath());
//            LOG.info("EncodedDataset : {}, Avro Schema Path                  : {}", name, encodedDataset.getAvroSchemaPath());
//            LOG.info("EncodedDataset : {}, Source dataset name               : {}", name, sourceDataset.getName());
//            LOG.info("EncodedDataset : {}, Source dataset base path          : {}", name, sourceDataset.getBasePath());
//            LOG.info("EncodedDataset : {}, Source dataset avro path          : {}", name, sourceDataset.getAvroPath());
//            LOG.info("EncodedDataset : {}, Source dataset avro schema path   : {}", name, sourceDataset.getAvroSchemaPath());
//            LOG.info("EncodedDataset : {}, Selected fields to encode         : {}", name, selectedFieldNames);
//            if(restFieldNames.length > 0)
//                LOG.info("EncodedDataset : {}, Rest of fields in encoded dataset : {}", name, restFieldNames);
//            LOG.info("EncodedDataset : {}, Encoding                          : {}", name, encoding.getFullName());
//
//            runEncodeDatasetTool(
//                    sourceDataset.getAvroPath(), sourceDataset.getSchemaFile(pprlClusterHdfs),
//                    encodedDataset.getAvroPath(), encodedDataset.getSchemaFile(pprlClusterHdfs));
//            removeSuccessFile(encodedDataset.getAvroPath());
//            addToDatasets(encodedDataset);
//        } catch (DatasetException e) {
//            LOG.error(e.getMessage());
//            throw e;
//        } catch (IOException e) {
//            LOG.error(e.getMessage());
//            throw e;
//        } catch (BloomFilterEncodingException e) {
//            LOG.error(e.getMessage());
//            throw e;
//        } catch (Exception e) {
//            LOG.error(e.getMessage());
//            throw e;
//        }
//    }
//
//    public void encodeRBFWeightedImportedDataset(String name,
//                                                 final String sourceDatasetName,
//                                                 final String[] selectedFieldNames, String[] restFieldNames,
//                                                 double[] weights, int K, int Q)
//            throws Exception {
//        try {
//            if(weights == null || weights.length == 0) {
//                throw new UnsupportedOperationException("Need to retrieve weight from stats.");
//            } else {
//                if(weights.length != selectedFieldNames.length)
//                    throw new BloomFilterEncodingException("weights size is not equal to selected fields size.");
//                double sum=0;
//                for (double w : weights) sum+=w;
//                if(sum != 1.0)
//                    throw new BloomFilterEncodingException("weights must add up to 1.0 . sum(weights)=" + sum + ".");
//            }
//            final Dataset sourceDataset = datasetsService.findDatasetByName(sourceDatasetName);
//            final Map<String, DatasetStatsWritable> stats =
//                    sourceDataset.getStats(pprlClusterHdfs, Q, selectedFieldNames);
//            final double[] avgQcounts = new double[selectedFieldNames.length];
//            int i =0 ;
//            for(String fieldName : selectedFieldNames)
//                avgQcounts[i] = stats.get(fieldName).getQgramCount();
//            // read calcuclated weights
//            final RowBloomFilterEncoding encoding = new RowBloomFilterEncoding(avgQcounts,weights,K,Q);
//            if(restFieldNames == null) restFieldNames = new String[0];
//            encoding.makeFromSchema(sourceDataset.getSchema(pprlClusterHdfs), selectedFieldNames, restFieldNames);
//            if(!encoding.isEncodingOfSchema(sourceDataset.getSchema(pprlClusterHdfs)))
//                throw new BloomFilterEncodingException("Encoding does not validate with source dataset.");
//            if(name == null) name = encoding.getFullName().toLowerCase();
//
//            final Path basePath = new Path(sourceDataset.getBasePath() + "/" + name);
//            final Path avroPath = new Path(basePath + "/avro");
//            final Path avroSchemaPath = new Path(basePath + "/schema");
//            final EncodedDataset encodedDataset =
//                    new EncodedDataset(name, sourceDatasetName, basePath, avroPath, avroSchemaPath);
//            encodedDataset.buildOnFS(pprlClusterHdfs, false, true, true, ONLY_OWNER_PERMISSION);
//            encodedDataset.setEncoding(encoding);
//            encodedDataset.writeSchemaOnHdfs(pprlClusterHdfs);
//
//            LOG.info("EncodedDataset : {}, Base path                         : {}", name, encodedDataset.getBasePath());
//            LOG.info("EncodedDataset : {}, Avro path                         : {}", name, encodedDataset.getAvroPath());
//            LOG.info("EncodedDataset : {}, Avro Schema Path                  : {}", name, encodedDataset.getAvroSchemaPath());
//            LOG.info("EncodedDataset : {}, Source dataset name               : {}", name, sourceDataset.getName());
//            LOG.info("EncodedDataset : {}, Source dataset base path          : {}", name, sourceDataset.getBasePath());
//            LOG.info("EncodedDataset : {}, Source dataset avro path          : {}", name, sourceDataset.getAvroPath());
//            LOG.info("EncodedDataset : {}, Source dataset avro schema path   : {}", name, sourceDataset.getAvroSchemaPath());
//            LOG.info("EncodedDataset : {}, Selected fields to encode         : {}", name, selectedFieldNames);
//            if(restFieldNames.length > 0)
//                LOG.info("EncodedDataset : {}, Rest of fields in encoded dataset : {}", name, restFieldNames);
//            LOG.info("EncodedDataset : {}, Encoding                          : {}", name, encoding.getFullName());
//
//            runEncodeDatasetTool(
//                    sourceDataset.getAvroPath(), sourceDataset.getSchemaFile(pprlClusterHdfs),
//                    encodedDataset.getAvroPath(), encodedDataset.getSchemaFile(pprlClusterHdfs));
//            removeSuccessFile(encodedDataset.getAvroPath());
//            addToDatasets(encodedDataset);
//        } catch (DatasetException e) {
//            LOG.error(e.getMessage());
//            throw e;
//        } catch (IOException e) {
//            LOG.error(e.getMessage());
//            throw e;
//        } catch (BloomFilterEncodingException e) {
//            LOG.error(e.getMessage());
//            throw e;
//        } catch (Exception e) {
//            LOG.error(e.getMessage());
//            throw e;
//        }
//    }
//
//
//    public String[] encodeFBFDynamicLocalFile(String name,
//                                              final Set<File> avroFiles, final File schemaFile,
//                                              final String[] selectedFieldNames,
//                                              final String[] restFieldNames,
//                                              final int K, final int Q)
//            throws IOException, BloomFilterEncodingException {
//        try {
//
//            final Map<String,DatasetStatsWritable> stats =
//                    localDatasetsService.calculateLocalStats(avroFiles, schemaFile, selectedFieldNames, Q);
//
//            final double[] avgQgrams = new double[selectedFieldNames.length];
//            for (int i = 0; i < selectedFieldNames.length; i++)
//                avgQgrams[i] = stats.get(selectedFieldNames[i]).getQgramCount();
//
//            final FieldBloomFilterEncoding encoding = new FieldBloomFilterEncoding(avgQgrams,K,Q);
//
//            final Schema schema = loadAvroSchemaFromFile(schemaFile);
//            encoding.makeFromSchema(schema, selectedFieldNames, restFieldNames);
//            if(!encoding.isEncodingOfSchema(schema))
//                throw new BloomFilterEncodingException("Encoding does not validate with source dataset.");
//            if(name == null) name = encoding.getFullName().toLowerCase();
//
//            LOG.info("Encoding local data : Data files                : {}", avroFiles);
//            LOG.info("Encoding local data : Schema file               : {}", schemaFile);
//            LOG.info("Encoding local data : Selected fields to encode : {}", selectedFieldNames);
//            LOG.info("Encoding local data : Rest of fields to include : {}", restFieldNames);
//            LOG.info("Encoding local data : Encoding name             : {}", name);
//
//            return BloomFilterEncoding.encodeLocalFile(name, avroFiles, schema, encoding);
//        } catch (IOException e) {
//            LOG.error(e.getMessage());
//            throw e;
//        } catch (BloomFilterEncodingException e) {
//            LOG.error(e.getMessage());
//            throw e;
//        }
//
//    }
//
//    public String[] encodeFBFStaticLocalFile(String name,
//                                             final Set<File> avroFiles, final  File schemaFile,
//                                             final String[] selectedFieldNames, final  String[] restFieldNames,
//                                             final int N, final int K, final int Q)
//            throws IOException, BloomFilterEncodingException {
//        try {
//            final FieldBloomFilterEncoding encoding = new FieldBloomFilterEncoding(N,selectedFieldNames.length,K,Q);
//
//            final Schema schema = loadAvroSchemaFromFile(schemaFile);
//            encoding.makeFromSchema(schema, selectedFieldNames, restFieldNames);
//            if(!encoding.isEncodingOfSchema(schema))
//                throw new BloomFilterEncodingException("Encoding does not validate with source dataset.");
//            if(name == null) name = encoding.getFullName().toLowerCase();
//
//            LOG.info("Encoding local data : Data files                : {}", avroFiles);
//            LOG.info("Encoding local data : Schema file               : {}", schemaFile);
//            LOG.info("Encoding local data : Selected fields to encode : {}", selectedFieldNames);
//            LOG.info("Encoding local data : Rest of fields to include : {}", restFieldNames);
//            LOG.info("Encoding local data : Encoding name             : {}", name);
//
//            return BloomFilterEncoding.encodeLocalFile(name, avroFiles, schema, encoding);
//        } catch (IOException e) {
//            LOG.error(e.getMessage());
//            throw e;
//        } catch (BloomFilterEncodingException e) {
//            LOG.error(e.getMessage());
//            throw e;
//        }
//    }
//
//    public String[] encodeRBFUniformLocalFile(String name,
//                                              final Set<File> avroFiles, final  File schemaFile,
//                                              final String[] selectedFieldNames, final String[] restFieldNames,
//                                              final int N, final int K, final int Q)
//            throws IOException, BloomFilterEncodingException {
//        try {
//            final Map<String,DatasetStatsWritable> stats =
//                    calculateLocalStats(avroFiles, schemaFile, selectedFieldNames, Q);
//
//            final double[] avgQgrams = new double[selectedFieldNames.length];
//            for (int i = 0; i < selectedFieldNames.length; i++)
//                avgQgrams[i] = stats.get(selectedFieldNames[i]).getQgramCount();
//
//            final RowBloomFilterEncoding encoding = new RowBloomFilterEncoding(avgQgrams,N,K,Q);
//
//            final Schema schema = loadAvroSchemaFromFile(schemaFile);
//            encoding.makeFromSchema(schema, selectedFieldNames, restFieldNames);
//            if(!encoding.isEncodingOfSchema(schema))
//                throw new BloomFilterEncodingException("Encoding does not validate with source dataset.");
//            if(name == null) name = encoding.getFullName().toLowerCase();
//
//            LOG.info("Encoding local data : Data files                : {}", avroFiles);
//            LOG.info("Encoding local data : Schema file               : {}", schemaFile);
//            LOG.info("Encoding local data : Selected fields to encode : {}", selectedFieldNames);
//            LOG.info("Encoding local data : Rest of fields to include : {}", restFieldNames);
//            LOG.info("Encoding local data : Encoding name             : {}", name);
//
//            return BloomFilterEncoding.encodeLocalFile(name, avroFiles, schema, encoding);
//        } catch (IOException e) {
//            LOG.error(e.getMessage());
//            throw e;
//        } catch (BloomFilterEncodingException e) {
//            LOG.error(e.getMessage());
//            throw e;
//        }
//    }
//
//
//    public String[] encodeRBFWeightedLocalFile(String name,
//                                               final Set<File> avroFiles, final File schemaFile,
//                                               final String[] selectedFieldNames,
//                                               final String[] restFieldNames,
//                                               double[] weights,
//                                               final int K, final int Q)
//            throws IOException, BloomFilterEncodingException {
//        try {
//            if(weights == null || weights.length == 0) {
//                throw new UnsupportedOperationException("Need to retrieve weight from stats.");
//            } else {
//                if(weights.length != selectedFieldNames.length)
//                    throw new BloomFilterEncodingException("weights size is not equal to selected fields size.");
//                double sum=0;
//                for (double w : weights) sum+=w;
//                if(sum != 1.0)
//                    throw new BloomFilterEncodingException("weights must add up to 1.0 . sum(weights)=" + sum + ".");
//            }
//            final Map<String,DatasetStatsWritable> stats =
//                    calculateLocalStats(avroFiles, schemaFile, selectedFieldNames, Q);
//
//            final double[] avgQgrams = new double[selectedFieldNames.length];
//            for (int i = 0; i < selectedFieldNames.length; i++)
//                avgQgrams[i] = stats.get(selectedFieldNames[i]).getQgramCount();
//            // read calcuclated weights
//
//            final RowBloomFilterEncoding encoding = new RowBloomFilterEncoding(avgQgrams,weights,K,Q);
//            final Schema schema = localDatasetsService.loadAvroSchemaFromFile(schemaFile);
//            encoding.makeFromSchema(schema, selectedFieldNames, restFieldNames);
//            if(!encoding.isEncodingOfSchema(schema))
//                throw new BloomFilterEncodingException("Encoding does not validate with source dataset.");
//            if(name == null) name = encoding.getFullName().toLowerCase();
//
//            LOG.info("Encoding local data : Data files                : {}", avroFiles);
//            LOG.info("Encoding local data : Schema file               : {}", schemaFile);
//            LOG.info("Encoding local data : Selected fields to encode : {}", selectedFieldNames);
//            LOG.info("Encoding local data : Rest of fields to include : {}", restFieldNames);
//            LOG.info("Encoding local data : Encoding name             : {}", name);
//
//            return BloomFilterEncoding.encodeLocalFile(name, avroFiles, schema, encoding);
//
//        } catch (IOException e) {
//            LOG.error(e.getMessage());
//            throw e;
//
//        } catch (BloomFilterEncodingException e) {
//            LOG.error(e.getMessage());
//            throw e;
//        }
//    }
//
//    protected void loadDatasets() throws IOException, EncodedDatasetException {
//        LOG.debug("Loading encoded datasets from " + userDatasetsFile);
//        if (pprlClusterHdfs.exists(userDatasetsFile)) {
//            final BufferedReader br =
//                    new BufferedReader(new InputStreamReader(pprlClusterHdfs.open(userDatasetsFile)));
//            try {
//                String line;
//                line = br.readLine();
//                while (line != null) {
//                    LOG.debug("read line : {}", line);
//                    EncodedDataset encodedDataset = EncodedDataset.fromString(line);
//                    encodedDataset.getEncoding().setupFromSchema(encodedDataset.getSchema(pprlClusterHdfs));
//                    datasets.add(encodedDataset);
//                    line = br.readLine();
//                }
//            } catch (BloomFilterEncodingException e) {
//                throw new EncodedDatasetException(e.getMessage());
//            } catch (DatasetException e) {
//                throw new EncodedDatasetException(e.getMessage());
//            } finally {
//                br.close();
//            }
//        } else FileSystem.create(pprlClusterHdfs, userDatasetsFile, ONLY_OWNER_PERMISSION);
//    }
//
//    protected void saveDatasets() throws IOException, EncodedDatasetException {
//        LOG.debug("Saving encoded datasets to " + userDatasetsFile);
//        if (!pprlClusterHdfs.exists(userDatasetsFile))
//            FileSystem.create(pprlClusterHdfs, userDatasetsFile, ONLY_OWNER_PERMISSION);
//        final BufferedWriter bw =
//                new BufferedWriter(new OutputStreamWriter(pprlClusterHdfs.create(userDatasetsFile)));
//        try {
//            for (Dataset encodedDataset : datasets) {
//                if(((EncodedDataset) encodedDataset).isNotOrphan()) {
//                    LOG.debug("{} is not orphan. Checking if true", encodedDataset.getName());
//                    if(((EncodedDataset) encodedDataset).checkIfOrphan(pprlClusterHdfs)) {
//                        ((EncodedDataset) encodedDataset).setOrphan();
//                        LOG.debug("{} is now orphan");
//                    }
//                }
//
//                final String line = EncodedDataset.toString((EncodedDataset) encodedDataset);
//                LOG.debug("Writing line : {}", line);
//                bw.write(line + "\n");
//            }
//        } finally {
//            bw.close();
//        }
//    }
//
//    private void runEncodeDatasetTool(final Path input, final Path inputSchema,
//                                      final Path output, final Path outputSchema) throws Exception {
//        final List<String> argsList = new ArrayList<String>();
//        argsList.add(input.toString()); argsList.add(inputSchema.toString());
//        LOG.info("input={} , inputSchema={}", input, inputSchema);
//        argsList.add(output.toString()); argsList.add(outputSchema.toString());
//        LOG.info("output={} , outputSchema={}", output, outputSchema);
//        String[] args = new String[argsList.size()];
//        args = argsList.toArray(args);
//        LOG.debug("args={}", Arrays.toString(args));
//        encodeDatasetToolRunner.setArguments(args);
//        try {
//            final Integer result = encodeDatasetToolRunner.call();
//        } catch (Exception e) {
//            LOG.error(e.getMessage());
//            throw e;
//        }
//        pprlClusterHdfs.setPermission(output, new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE, false));
//    }
//
//    private List<EncodedDataset> filterEncodedDatasetsByDatasetName(final String datasetName) {
//        final List<EncodedDataset> encodedDatasets = new ArrayList<EncodedDataset>();
//        for(Dataset d : datasets) {
//            if(((EncodedDataset) d).isOrphan()) continue;
//            if(((EncodedDataset) d).getDatasetName().equals(datasetName))
//                encodedDatasets.add(((EncodedDataset) d));
//        }
//        return encodedDatasets;
//    }
//
//    private List<EncodedDataset> filterEncodedDatasetsByMethodName(final String methodName)
//            throws BloomFilterEncodingException {
//        BloomFilterEncoding.belongsInAvailableMethods(methodName);
//
//        final List<EncodedDataset> encodedDatasets = new ArrayList<EncodedDataset>();
//        for(Dataset d : datasets) {
//            if(!d.isValid()) continue;
//            if(((EncodedDataset)d).getEncoding().toString().equals(methodName))
//                encodedDatasets.add((EncodedDataset) d);
//        }
//        return encodedDatasets;
//    }
//
//    private List<EncodedDataset> filterEncodedDatasetsByNames(final String datasetName, final String methodName)
//            throws BloomFilterEncodingException {
//        BloomFilterEncoding.belongsInAvailableMethods(methodName);
//
//        final List<EncodedDataset> encodedDatasets = new ArrayList<EncodedDataset>();
//        for(Dataset d : datasets) {
//            if(((EncodedDataset)d).isOrphan()) continue;
//            if(!((EncodedDataset) d).getDatasetName().equals(datasetName)) continue;
//            if(!d.isValid()) continue;
//            if(((EncodedDataset)d).getEncoding().toString().equals(methodName))
//                encodedDatasets.add((EncodedDataset) d);
//        }
//        return encodedDatasets;
//    }
}
