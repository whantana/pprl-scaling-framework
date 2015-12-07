package gr.upatras.ceid.pprl.encoding.service;

import gr.upatras.ceid.pprl.datasets.Dataset;
import gr.upatras.ceid.pprl.datasets.DatasetException;
import gr.upatras.ceid.pprl.datasets.service.DatasetsService;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncoding;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncodingException;
import gr.upatras.ceid.pprl.encoding.EncodedDataset;
import gr.upatras.ceid.pprl.encoding.EncodedDatasetException;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.hadoop.mapreduce.ToolRunner;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

@Service
public class EncodingService extends DatasetsService {

    private static final Logger LOG = LoggerFactory.getLogger(EncodingService.class);

    @Autowired
    private DatasetsService datasetsService;

    @Autowired
    private ToolRunner encodeDatasetToolRunner;

    public void afterPropertiesSet() throws Exception {
        checkSite();
        userDatasetsFile = new Path(pprlClusterHdfs.getHomeDirectory() + "/.pprl_encodings");
        loadDatasets();
        LOG.info("Service is now initialized. Found {} datasets on the PPRL site.", datasets.size());
    }

    public List<String> listSupportedEncodingMethodsNames() {
        return BloomFilterEncoding.AVAILABLE_METHODS;
    }

    public List<String> listDatasets(final boolean onlyName) {
        final List<String> strings = new ArrayList<String>();
        if(onlyName) {
            for (Dataset dataset : datasets) strings.add(dataset.getName());
            return strings;
        }
        for (Dataset dataset : datasets) {
            StringBuilder sb = new StringBuilder("name : ");
            sb.append(dataset.getName());
            List<String> paths = new ArrayList<String>();
            paths.add(dataset.getAvroPath().toString());
            paths.add(dataset.getAvroSchemaPath().toString());
            sb.append(" | paths : ").append(paths);
            if(!dataset.isValid()) continue;
            if(((EncodedDataset) dataset).isNotOrphan())
                sb.append(" | source dataset :")
                        .append(((EncodedDataset) dataset).getDatasetName());
            sb.append(" | encoding method :")
                    .append(((EncodedDataset) dataset).getEncoding().getName());
            strings.add(sb.toString());
        }
        return strings;
    }

    public List<String> listDatasets(final String datasetName, final String methodName)
            throws EncodedDatasetException, BloomFilterEncodingException {
        try {
            final List<EncodedDataset> filteredEncodedDatasets;
            if (datasetName != null && methodName == null) {
                filteredEncodedDatasets = filterEncodedDatasetsByDatasetName(datasetName);
            } else if (datasetName == null && methodName != null) {
                filteredEncodedDatasets = filterEncodedDatasetsByMethodName(methodName);
            } else if (datasetName != null) {
                filteredEncodedDatasets = filterEncodedDatasetsByNames(datasetName, methodName);
            } else throw new EncodedDatasetException("Invalid input datasetName==methodName==null");
            final List<String> strings = new ArrayList<String>();
            for (EncodedDataset encodedDataset : filteredEncodedDatasets) {
                StringBuilder sb = new StringBuilder("name : ");
                sb.append(encodedDataset.getName());
                List<String> paths = new ArrayList<String>();
                paths.add(encodedDataset.getAvroPath().toString());
                paths.add(encodedDataset.getAvroSchemaPath().toString());
                sb.append(" | paths : ").append(paths);
                if (encodedDataset.isNotOrphan()) {
                    sb.append(" | source dataset :")
                            .append(encodedDataset.getDatasetName());
                }
                sb.append(" | encoding method :")
                        .append(encodedDataset.getEncoding().getName());
                strings.add(sb.toString());
            }
            return strings;
        } catch (EncodedDatasetException e) {
            LOG.error(e.getMessage());
            throw(e);
        } catch (BloomFilterEncodingException e) {
            LOG.error(e.getMessage());
            throw(e);
        }
    }


    public void importOrphanEncodedDataset(String name, final String methodName,
                                           final File localAvroSchemaFile, final File... localAvroFiles)
            throws IOException, DatasetException, BloomFilterEncodingException {
        try {
            if(name == null) name = loadAvroSchemaFromFile(localAvroSchemaFile).getName().toLowerCase();

            final EncodedDataset encodedDataset = new EncodedDataset(name, pprlClusterHdfs.getHomeDirectory());
            encodedDataset.buildOnFS(pprlClusterHdfs, ONLY_OWNER_PERMISSION);
            uploadFileToHdfs(encodedDataset.getAvroPath(), localAvroFiles);
            uploadFileToHdfs(encodedDataset.getAvroSchemaPath(), localAvroSchemaFile);

            LOG.info("EncodedDataset : {}, Base path        : {}", name, encodedDataset.getBasePath());
            LOG.info("EncodedDataset : {}, Avro path        : {}", name, encodedDataset.getAvroPath());
            LOG.info("EncodedDataset : {}, Avro Schema Path : {}", name, encodedDataset.getAvroSchemaPath());

            BloomFilterEncoding encoding = BloomFilterEncoding.newInstanceOfMethod(methodName);
            encoding.makeFromSchema(encodedDataset.getSchema(pprlClusterHdfs));
            if(encoding.isInvalid()) throw new BloomFilterEncodingException("Encoding not valid.");
            encodedDataset.setEncoding(encoding);
            LOG.info("EncodedDataset : {}, Encoding : {}", name, encodedDataset.getEncoding().toString());

            addToDatasets(encodedDataset);
        } catch (DatasetException e) {
            LOG.error(e.getMessage());
            throw e;
        } catch (IOException e) {
            LOG.error(e.getMessage());
            throw e;
        } catch (BloomFilterEncodingException e) {
            LOG.error(e.getMessage());
            throw e;
        }
    }

    public void importEncodedDataset(String name, final String sourceDatasetName,
                                     final String methodName,
                                     final File localAvroSchemaFile, final File... localAvroFiles)
            throws IOException, DatasetException, BloomFilterEncodingException {
        try {

            if(sourceDatasetName == null) {
                importOrphanEncodedDataset(name, methodName, localAvroSchemaFile, localAvroFiles);
                return;
            }

            if(name == null) name = loadAvroSchemaFromFile(localAvroSchemaFile).getName().toLowerCase();

            final Dataset sourceDataset = datasetsService.findDatasetByName(sourceDatasetName);
            final Path basePath = new Path(sourceDataset.getBasePath() + "/" + name);
            final Path avroPath = new Path(basePath + "/avro");
            final Path avroSchemaPath = new Path(basePath + "/schema");
            final EncodedDataset encodedDataset =
                    new EncodedDataset(name, sourceDatasetName, basePath, avroPath, avroSchemaPath);
            encodedDataset.buildOnFS(pprlClusterHdfs, ONLY_OWNER_PERMISSION);
            uploadFileToHdfs(encodedDataset.getAvroPath(), localAvroFiles);
            uploadFileToHdfs(encodedDataset.getAvroSchemaPath(), localAvroSchemaFile);

            LOG.info("EncodedDataset : {}, Base path                       : {}", name, encodedDataset.getBasePath());
            LOG.info("EncodedDataset : {}, Avro path                       : {}", name, encodedDataset.getAvroPath());
            LOG.info("EncodedDataset : {}, Avro Schema Path                : {}", name, encodedDataset.getAvroSchemaPath());
            LOG.info("EncodedDataset : {}, Source dataset name             : {}", name, sourceDataset.getName());
            LOG.info("EncodedDataset : {}, Source dataset base path        : {}", name, sourceDataset.getBasePath());
            LOG.info("EncodedDataset : {}, Source dataset avro path        : {}", name, sourceDataset.getAvroPath());
            LOG.info("EncodedDataset : {}, Source dataset avro schema path : {}", name, sourceDataset.getAvroSchemaPath());

            BloomFilterEncoding encoding = BloomFilterEncoding.newInstanceOfMethod(methodName);
            encoding.makeFromSchema(encodedDataset.getSchema(pprlClusterHdfs));
            if(encoding.isInvalid()) throw new BloomFilterEncodingException("Encoding not valid.");
            encoding.isEncodingOfSchema(sourceDataset.getSchema(pprlClusterHdfs));
            encodedDataset.setEncoding(encoding);
            LOG.info("EncodedDataset : {}, Encoding : {}", name, encodedDataset.getEncoding().toString());

            addToDatasets(encodedDataset);
        } catch (EncodedDatasetException e) {
            LOG.error(e.getMessage());
            throw e;
        } catch (BloomFilterEncodingException e) {
            LOG.error(e.getMessage());
            throw e;
        } catch (DatasetException e) {
            LOG.error(e.getMessage());
            throw e;
        } catch (IOException e) {
            LOG.error(e.getMessage());
            throw e;
        }
    }

    public void encodeImportedDataset(String name, final String sourceDatasetName,
                                      final String[] selectedFieldNames,
                                      final String[] restFieldNames,
                                      final String methodName,final int N, final int K, final int Q)
            throws Exception {
        try {
            final Dataset sourceDataset = datasetsService.findDatasetByName(sourceDatasetName);
            final BloomFilterEncoding encoding;
            if(N <= 0) {
                throw new BloomFilterEncodingException("Not Supported yet!");
                // TODO read stats for each fields put em in double[]
                // instantiate encoding
                //double[] avgQgrams = new double[1];
                //encoding = BloomFilterEncoding.newInstanceOfMethod(methodName, avgQgrams , K, Q);
            } else {
                // static sizing
                LOG.debug("Static FBF size encoding");
                encoding = BloomFilterEncoding.newInstanceOfMethod(methodName, N, K, Q);
            }
            encoding.makeFromSchema(sourceDataset.getSchema(pprlClusterHdfs), selectedFieldNames, restFieldNames);
            if(encoding.isInvalid()) throw new BloomFilterEncodingException("Encoding not valid.");
            encoding.isEncodingOfSchema(sourceDataset.getSchema(pprlClusterHdfs));

            if(name == null) name = encoding.getEncodingSchema().getName().toLowerCase();

            final Path basePath = new Path(sourceDataset.getBasePath() + "/" + name);
            final Path avroPath = new Path(basePath + "/avro");
            final Path avroSchemaPath = new Path(basePath + "/schema");
            final EncodedDataset encodedDataset =
                    new EncodedDataset(name, sourceDatasetName, basePath, avroPath, avroSchemaPath);
            encodedDataset.buildOnFS(pprlClusterHdfs, false, true, true, ONLY_OWNER_PERMISSION);
            encodedDataset.setEncoding(encoding);

            LOG.info("EncodedDataset : {}, Base path                         : {}", name, encodedDataset.getBasePath());
            LOG.info("EncodedDataset : {}, Avro path                         : {}", name, encodedDataset.getAvroPath());
            LOG.info("EncodedDataset : {}, Avro Schema Path                  : {}", name, encodedDataset.getAvroSchemaPath());
            LOG.info("EncodedDataset : {}, Source dataset name               : {}", name, sourceDataset.getName());
            LOG.info("EncodedDataset : {}, Source dataset base path          : {}", name, sourceDataset.getBasePath());
            LOG.info("EncodedDataset : {}, Source dataset avro path          : {}", name, sourceDataset.getAvroPath());
            LOG.info("EncodedDataset : {}, Source dataset avro schema path   : {}", name, sourceDataset.getAvroSchemaPath());
            LOG.info("EncodedDataset : {}, Selected fields to encode         : {}", name, selectedFieldNames);
            LOG.info("EncodedDataset : {}, Rest of fields in encoded dataset : {}", name, restFieldNames);
            LOG.info("EncodedDataset : {}, Encoding : {}", name, encodedDataset.getEncoding().toString());

            int[] Ns = new int[selectedFieldNames.length];
            if(encoding.hasSingleN())
                for (int i = 0; i < Ns.length; i++) Ns[i] = encoding.getN(0);
            else Ns = encoding.getN();

            runEncodeDatasetTool(
                    sourceDataset.getAvroPath(), sourceDataset.getSchemaFile(pprlClusterHdfs),
                    encodedDataset.getAvroPath(), encodedDataset.getSchemaFile(pprlClusterHdfs),
                    selectedFieldNames, restFieldNames,
                    methodName, Ns, K, Q);

            removeSuccessFile(encodedDataset.getAvroPath());
            addToDatasets(encodedDataset);
        } catch (BloomFilterEncodingException e) {
            LOG.error(e.getMessage());
            throw e;
        } catch (DatasetException e) {
            LOG.error(e.getMessage());
            throw e;
        } catch (IOException e) {
            LOG.error(e.getMessage());
            throw e;
        } catch (Exception e) {
            LOG.error(e.getMessage());
            throw e;
        }
    }

    public String[] encodeLocalFile(String name,
                                    final String[] selectedFieldNames,
                                    final String[] restFieldNames,
                                    final String methodName,
                                    final int N, final int K, final int Q,
                                    final Set<File> avroFiles, final File schemaFile) throws BloomFilterEncodingException, IOException {
        try {

            LOG.info("Encoding local file(s)     : {}", avroFiles);
            LOG.info("Encoding local schema file : {}", schemaFile);
            LOG.info("Selected fields to encode : {}", selectedFieldNames);
            LOG.info("Rest of fields to encode : {}", restFieldNames);
            LOG.info("method = {} (" + N + ", " + K + ", " + Q + ")", methodName);

            final Schema schema = loadAvroSchemaFromFile(schemaFile);
            BloomFilterEncoding encoding;
            if(N <= 0 ) {
                throw new BloomFilterEncodingException("Not Supported yet!");
                // TODO read stats for each fields put em in double[]
                // instantiate encoding
                //double[] avgQgrams = new double[1];
                //encoding = BloomFilterEncoding.newInstanceOfMethod(methodName, avgQgrams , K, Q);
            } else {
                LOG.debug("Static FBF size encoding");
                encoding = BloomFilterEncoding.newInstanceOfMethod(methodName, N, K, Q);
            }
            encoding.makeFromSchema(schema, selectedFieldNames, restFieldNames);
            if(encoding.isInvalid()) throw new BloomFilterEncodingException("Encoding not valid.");
            encoding.isEncodingOfSchema(schema);

            if(name == null) name = encoding.getEncodingSchema().getName().toLowerCase();

            int[] Ns = new int[selectedFieldNames.length];
            if(encoding.hasSingleN())
                for (int i = 0; i < Ns.length; i++) Ns[i] = encoding.getN(0);
            else Ns = encoding.getN();

            final File encodedSchemaFile = new File(schemaFile.getParent(),name + ".avsc");
            encodedSchemaFile.createNewFile();
            final PrintWriter schemaWriter = new PrintWriter(encodedSchemaFile);
            schemaWriter.print(encoding.getEncodingSchema().toString(true));
            schemaWriter.close();

            final File encodedFile = new File(avroFiles.iterator().next().getParent(), name + ".avro");
            encodedFile.createNewFile();
            final DataFileWriter<GenericRecord> writer =
                    new DataFileWriter<GenericRecord>(new GenericDatumWriter<GenericRecord>(
                            encoding.getEncodingSchema()));
            writer.create(encoding.getEncodingSchema(), encodedFile);
            for (File avroFile : avroFiles) {
                final DataFileReader<GenericRecord> reader =
                        new DataFileReader<GenericRecord>(avroFile,
                                new GenericDatumReader<GenericRecord>(schema));
                for (GenericRecord record : reader) {
                    writer.append(
                            BloomFilterEncoding.encodeRecord(record, encoding, schema,
                                    selectedFieldNames, restFieldNames, Ns, K, Q));
                }
                reader.close();
            }
            writer.close();

            return new String[]{
                    encodedFile.getAbsolutePath(),
                    encodedSchemaFile.getAbsolutePath()
            };

        } catch (FileNotFoundException e) {
            LOG.error(e.getMessage());
            throw e;
        } catch (BloomFilterEncodingException e) {
            LOG.error(e.getMessage());
            throw e;
        } catch (UnsupportedEncodingException e) {
            LOG.error(e.getMessage());
            throw e;
        } catch (IOException e) {
            LOG.error(e.getMessage());
            throw e;
        }
    }

    protected void loadDatasets() throws IOException, EncodedDatasetException {
        LOG.debug("Loading encoded datasets from " + userDatasetsFile);
        if (pprlClusterHdfs.exists(userDatasetsFile)) {
            final BufferedReader br =
                    new BufferedReader(new InputStreamReader(pprlClusterHdfs.open(userDatasetsFile)));
            try {
                String line;
                line = br.readLine();
                while (line != null) {
                    LOG.debug("read line : {}", line);
                    EncodedDataset encodedDataset = EncodedDataset.fromString(line);
                    encodedDataset.getEncoding().makeFromSchema(encodedDataset.getSchema(pprlClusterHdfs));
                    datasets.add(encodedDataset);
                    line = br.readLine();
                }
            } catch (BloomFilterEncodingException e) {
                throw new EncodedDatasetException(e.getMessage());
            } finally {
                br.close();
            }
        } else FileSystem.create(pprlClusterHdfs, userDatasetsFile, ONLY_OWNER_PERMISSION);
    }

    protected void saveDatasets() throws IOException, EncodedDatasetException {
        LOG.debug("Saving encoded datasets to " + userDatasetsFile);
        if (!pprlClusterHdfs.exists(userDatasetsFile))
            FileSystem.create(pprlClusterHdfs, userDatasetsFile, ONLY_OWNER_PERMISSION);
        final BufferedWriter bw =
                new BufferedWriter(new OutputStreamWriter(pprlClusterHdfs.create(userDatasetsFile)));
        try {
            for (Dataset encodedDataset : datasets) {
                if(((EncodedDataset) encodedDataset).isNotOrphan()) {
                    LOG.debug("{} is not orphan. Checking if true", encodedDataset.getName());
                    if(((EncodedDataset) encodedDataset).checkIfOrphan(pprlClusterHdfs)) {
                        ((EncodedDataset) encodedDataset).setOrphan();
                        LOG.debug("{} is now orphan");
                    }
                }

                final String line = EncodedDataset.toString((EncodedDataset) encodedDataset);
                LOG.debug("Writing line : {}", line);
                bw.write(line + "\n");
            }
        } finally {
            bw.close();
        }
    }

    private void runEncodeDatasetTool(final Path input, final Path inputSchema,
                                      final Path output, final Path outputSchema,
                                      final String[] encodingFieldNames, final String[] restFieldNames,
                                      final String methodName,
                                      final int[] N, final int K, final int Q) throws Exception {
        LOG.info("input={} , inputSchema={}", input, inputSchema);
        LOG.info("output={} , outputSchema={}", output, outputSchema);
        LOG.info("selected column names={}", Arrays.toString(encodingFieldNames));
        LOG.info("rest of =", Arrays.toString(restFieldNames));
        LOG.info("method = {}",methodName);
        LOG.info("N = {}",Arrays.toString(N));
        LOG.info("K = {}",K);
        LOG.info("Q = {}",Q);

        final String[] args = new String[10];
        args[0] = input.toString();
        args[1] = inputSchema.toString();
        args[2] = output.toString();
        args[3] = outputSchema.toString();
        StringBuilder sb = new StringBuilder(encodingFieldNames[0]);
        for (int i = 1; i < encodingFieldNames.length; i++) sb.append(",").append(encodingFieldNames[i]);
        args[4] = sb.toString();
        sb = new StringBuilder(restFieldNames[0]);
        for (int i = 1; i < restFieldNames.length; i++) sb.append(",").append(restFieldNames[i]);
        args[5] = sb.toString();
        args[6] = methodName;
        sb = new StringBuilder(Integer.toString(N[0]));
        for (int i = 1; i < N.length; i++) sb.append(",").append(N[i]);
        args[7] = sb.toString();
        args[8] = Integer.toString(K);
        args[9] = Integer.toString(Q);

        LOG.debug("args=", Arrays.toString(args));

        encodeDatasetToolRunner.setArguments(args);
        try {
            final Integer result = encodeDatasetToolRunner.call();
        } catch (Exception e) {
            LOG.error(e.getMessage());
            throw e;
        }
        pprlClusterHdfs.setPermission(output, new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE, false));
    }

    private Schema loadAvroSchemaFromFile(final File schemaFile) throws IOException {
        FileInputStream fis = new FileInputStream(schemaFile);
        Schema schema = (new Schema.Parser()).parse(fis);
        fis.close();
        return schema;
    }

    private void saveAvroSchemaOnHdfs(final FileSystem fs,final Path schemaPath,final Schema schema) throws IOException {
        FSDataOutputStream fsdofs = fs.create(schemaPath, true);
        fsdofs.write(schema.toString(true).getBytes());
        fsdofs.close();
    }

    private List<EncodedDataset> filterEncodedDatasetsByDatasetName(final String datasetName) {
        final List<EncodedDataset> encodedDatasets = new ArrayList<EncodedDataset>();
        for(Dataset d : datasets) {
            if(((EncodedDataset) d).isOrphan()) continue;
            if(((EncodedDataset) d).getDatasetName().equals(datasetName))
                encodedDatasets.add(((EncodedDataset) d));
        }
        return encodedDatasets;
    }

    private List<EncodedDataset> filterEncodedDatasetsByMethodName(final String methodName)
            throws BloomFilterEncodingException {
        BloomFilterEncoding.belongsInAvailableMethods(methodName);

        final List<EncodedDataset> encodedDatasets = new ArrayList<EncodedDataset>();
        for(Dataset d : datasets) {
            if(!d.isValid()) continue;
            if(((EncodedDataset)d).getEncoding().getName().startsWith(methodName))
                encodedDatasets.add((EncodedDataset) d);
        }
        return encodedDatasets;
    }

    private List<EncodedDataset> filterEncodedDatasetsByNames(final String datasetName, final String methodName)
            throws BloomFilterEncodingException {
        BloomFilterEncoding.belongsInAvailableMethods(methodName);

        final List<EncodedDataset> encodedDatasets = new ArrayList<EncodedDataset>();
        for(Dataset d : datasets) {
            if(((EncodedDataset)d).isOrphan()) continue;
            if(!((EncodedDataset) d).getDatasetName().equals(datasetName)) continue;
            if(!d.isValid()) continue;
            if(((EncodedDataset)d).getEncoding().getName().startsWith(methodName))
                encodedDatasets.add((EncodedDataset) d);
        }
        return encodedDatasets;
    }

    private double[] readAverageQgramsForFieldNames(final Path path) {
        return null; // TODO
    }
}
