package gr.upatras.ceid.pprl.encoding.service;

import gr.upatras.ceid.pprl.datasets.Dataset;
import gr.upatras.ceid.pprl.datasets.DatasetException;
import gr.upatras.ceid.pprl.datasets.service.DatasetsService;
import gr.upatras.ceid.pprl.encoding.BaseBloomFilterEncoding;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncodingException;
import gr.upatras.ceid.pprl.encoding.EncodedDataset;
import gr.upatras.ceid.pprl.encoding.EncodedDatasetException;
import gr.upatras.ceid.pprl.encoding.MultiBloomFilterEncoding;
import gr.upatras.ceid.pprl.encoding.RowBloomFilterEncoding;
import gr.upatras.ceid.pprl.encoding.SimpleBloomFilterEncoding;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
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
        return BaseBloomFilterEncoding.AVAILABLE_METHODS;
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


    public void importEncodedDatasets(final String name, final String methodName,
                                      final int N, final int K, final int Q,
                                      final File localAvroSchemaFile, final File... localAvroFiles)
            throws IOException, DatasetException {
        try {
            final EncodedDataset encodedDataset = new EncodedDataset(name, pprlClusterHdfs.getHomeDirectory());
            encodedDataset.buildOnFS(pprlClusterHdfs, ONLY_OWNER_PERMISSION);
            uploadFileToHdfs(encodedDataset.getAvroPath(), localAvroFiles);
            uploadFileToHdfs(encodedDataset.getAvroSchemaPath(), localAvroSchemaFile);

            LOG.info("EncodedDataset : {}, Base path        : {}", name, encodedDataset.getBasePath());
            LOG.info("EncodedDataset : {}, Avro path        : {}", name, encodedDataset.getAvroPath());
            LOG.info("EncodedDataset : {}, Avro Schema Path : {}", name, encodedDataset.getAvroSchemaPath());

            BaseBloomFilterEncoding encoding;
            if (methodName.equals("SIMPLE")) {
                encoding = new SimpleBloomFilterEncoding(encodedDataset.getSchema(pprlClusterHdfs), N, K, Q);
            } else if (methodName.equals("MULTI")) {
                encoding = new MultiBloomFilterEncoding(encodedDataset.getSchema(pprlClusterHdfs), N, K, Q);
            } else {
                encoding = new RowBloomFilterEncoding(encodedDataset.getSchema(pprlClusterHdfs), N, K, Q);
            }
            encoding.setupFromEncodingSchema();
            encodedDataset.setEncoding(encoding);
            LOG.info("EncodedDataset : {}, Encoding : {}", name, encodedDataset.getEncoding().toString());

            addToDatasets(encodedDataset);
        } catch (DatasetException e) {
            LOG.error(e.getMessage());
            throw e;
        } catch (IOException e) {
            LOG.error(e.getMessage());
            throw e;
        }
    }

    public void importEncodedDatasets(final String name, final String sourceDatasetName,
                                      final List<String> selectedColumnNames,
                                      final String methodName,final int N, final int K, final int Q,
                                      final File localAvroSchemaFile, final File... localAvroFiles)
            throws IOException, DatasetException, BloomFilterEncodingException {
        try {
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
            LOG.info("EncodedDataset : {}, Selected columns to encode      : {}", name, selectedColumnNames);

            BaseBloomFilterEncoding encoding;
            if (methodName.equals("SIMPLE")) {
                encoding = new SimpleBloomFilterEncoding(
                        sourceDataset.getSchema(pprlClusterHdfs),
                        encodedDataset.getSchema(pprlClusterHdfs),
                        selectedColumnNames, N, K, Q);
            } else if (methodName.equals("MULTI")) {
                encoding = new MultiBloomFilterEncoding(
                        sourceDataset.getSchema(pprlClusterHdfs),
                        encodedDataset.getSchema(pprlClusterHdfs),
                        selectedColumnNames, N, K, Q);
            } else {
                encoding = new RowBloomFilterEncoding(
                        sourceDataset.getSchema(pprlClusterHdfs),
                        encodedDataset.getSchema(pprlClusterHdfs),
                        selectedColumnNames, N, K, Q);
            }
            encoding.createEncodingFields();
            if (!encoding.validateEncodingSchema()) throw new EncodedDatasetException("Encoding schema not valid");
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

    public void encodeImportedDataset(final String name, final String sourceDatasetName,
                                      final List<String> selectedColumnNames,
                                      final String methodName,final int N, final int K, final int Q)
            throws Exception {
        try {
            final Dataset sourceDataset = datasetsService.findDatasetByName(sourceDatasetName);
            final Path basePath = new Path(sourceDataset.getBasePath() + "/" + name);
            final Path avroPath = new Path(basePath + "/avro");
            final Path avroSchemaPath = new Path(basePath + "/schema");
            final EncodedDataset encodedDataset =
                    new EncodedDataset(name, sourceDatasetName, basePath, avroPath, avroSchemaPath);
            encodedDataset.buildOnFS(pprlClusterHdfs, false, true, true, ONLY_OWNER_PERMISSION);

            LOG.info("EncodedDataset : {}, Base path                       : {}", name, encodedDataset.getBasePath());
            LOG.info("EncodedDataset : {}, Avro path                       : {}", name, encodedDataset.getAvroPath());
            LOG.info("EncodedDataset : {}, Avro Schema Path                : {}", name, encodedDataset.getAvroSchemaPath());
            LOG.info("EncodedDataset : {}, Source dataset name             : {}", name, sourceDataset.getName());
            LOG.info("EncodedDataset : {}, Source dataset base path        : {}", name, sourceDataset.getBasePath());
            LOG.info("EncodedDataset : {}, Source dataset avro path        : {}", name, sourceDataset.getAvroPath());
            LOG.info("EncodedDataset : {}, Source dataset avro schema path : {}", name, sourceDataset.getAvroSchemaPath());
            LOG.info("EncodedDataset : {}, Selected columns to encode      : {}", name, selectedColumnNames);

            BaseBloomFilterEncoding encoding;
            if (methodName.equals("SIMPLE")) {
                encoding = new SimpleBloomFilterEncoding(
                        sourceDataset.getSchema(pprlClusterHdfs),
                        selectedColumnNames, N, K, Q);
            } else if (methodName.equals("MULTI")) {
                encoding = new MultiBloomFilterEncoding(
                        sourceDataset.getSchema(pprlClusterHdfs),
                        selectedColumnNames, N, K, Q);
            } else {
                encoding = new RowBloomFilterEncoding(
                        sourceDataset.getSchema(pprlClusterHdfs),
                        selectedColumnNames, N, K, Q);
            }
            encoding.createEncodingFields();
            encoding.generateEncodingSchema();
            encodedDataset.setEncoding(encoding);
            saveAvroSchemaOnHdfs(pprlClusterHdfs,
                    new Path(encodedDataset.getAvroSchemaPath() + "/" + encodedDataset.getDatasetName() + ".avsc"),
                    encoding.getEncodingSchema());
            LOG.info("EncodedDataset : {}, Encoding : {}", name, encodedDataset.getEncoding().toString());

            runEncodeDatasetTool(
                    sourceDataset.getAvroPath(), sourceDataset.getSchemaFile(pprlClusterHdfs),
                    encodedDataset.getAvroPath(), encodedDataset.getSchemaFile(pprlClusterHdfs),
                    selectedColumnNames, "key",
                    methodName, N, K, Q);

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

    public void encodeLocalFile(final List<String> selectedColumnNames,
                                final String methodName,
                                final int N, final int K, final int Q,
                                final Set<File> avroFiles, final File schemaFile,
                                final File encodedFile, final File encodedSchemaFile) throws BloomFilterEncodingException, IOException {
        try {
            LOG.info("Encoding local file(s)     : {}", avroFiles);
            LOG.info("Encoding local schema file : {}", schemaFile);
            LOG.info("Selected columns to encode : {}", selectedColumnNames);
            LOG.info("method = {} (" + N + ", " + K + ", " + Q + ")", methodName);

            final Schema schema = loadAvroSchemaFromFile(schemaFile);

            BaseBloomFilterEncoding encoding;
            if (methodName.equals("SIMPLE")) {
                encoding = new SimpleBloomFilterEncoding(
                        schema,
                        selectedColumnNames, N, K, Q);
            } else if (methodName.equals("MULTI")) {
                encoding = new MultiBloomFilterEncoding(
                        schema,
                        selectedColumnNames, N, K, Q);
            } else {
                encoding = new RowBloomFilterEncoding(
                        schema,
                        selectedColumnNames, N, K, Q);
            }
            encoding.createEncodingFields();
            encoding.generateEncodingSchema();

            final PrintWriter schemaWriter = new PrintWriter(encodedSchemaFile);
            schemaWriter.print(encoding.getEncodingSchema().toString(true));
            schemaWriter.close();

            final DataFileWriter<GenericRecord> writer =
                    new DataFileWriter<GenericRecord>(new GenericDatumWriter<GenericRecord>(
                            encoding.getEncodingSchema()));
            writer.create(encoding.getEncodingSchema(), encodedFile);
            for (File avroFile : avroFiles) {
                final DataFileReader<GenericRecord> reader =
                        new DataFileReader<GenericRecord>(avroFile,
                                new GenericDatumReader<GenericRecord>(encoding.getSchema()));
                for (GenericRecord record : reader) {
                    GenericRecord encodedRecord = encodeRecord(record, encoding);
                    writer.append(encodedRecord);
                }
            }
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


    protected void loadDatasets() throws IOException {
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
                    if(encodedDataset != null) {
                        if(encodedDataset.getEncoding() != null) {
                            encodedDataset.getEncoding().setEncodingSchema(encodedDataset.getSchema(pprlClusterHdfs));
                            encodedDataset.getEncoding().setupFromEncodingSchema();
                        }
                        datasets.add(encodedDataset);
                    }
                    line = br.readLine();
                }
            } finally {
                br.close();
            }
        } else FileSystem.create(pprlClusterHdfs, userDatasetsFile, ONLY_OWNER_PERMISSION);
    }

    protected void saveDatasets() throws IOException {
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
                if(line != null) {
                    LOG.debug("Writing line : {}", line);
                    bw.write(line + "\n");
                }
            }
        } finally {
            bw.close();
        }
    }

    private void runEncodeDatasetTool(final Path input, final Path inputSchema,
                                      final Path output, final Path outputSchema,
                                      final List<String> columns, final String uidColumn,
                                      final String methodName,
                                      final int N, final int K, final int Q) throws Exception {
        LOG.info("input={} , inputSchema={}", input, inputSchema);
        LOG.info("output={} , outputSchema={}", output, outputSchema);
        LOG.info("selected column names={}", columns);
        LOG.info("uidColumn=", uidColumn);
        LOG.info("method = {} (" + N + ", " + K + ", " + Q + ")", methodName);

        final String[] args = new String[10];
        args[0] = input.toString();
        args[1] = inputSchema.toString();
        args[2] = output.toString();
        args[3] = outputSchema.toString();
        String columnsStr = columns.get(0);
        for (int i = 1; i < columns.size(); i++) columnsStr += "," + columns.get(i);
        args[4] = columnsStr;
        args[5] = uidColumn;
        args[6] = methodName;
        args[7] = Integer.toString(N);
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

    private GenericRecord encodeRecord(final GenericRecord record, final BaseBloomFilterEncoding encoding)
            throws BloomFilterEncodingException, UnsupportedEncodingException {
        final GenericRecord encodingRecord = new GenericData.Record(encoding.getEncodingSchema());
        if (encoding instanceof SimpleBloomFilterEncoding ||
                encoding instanceof RowBloomFilterEncoding) {

            String fieldName;
            Schema fieldSchema;
            if(encoding instanceof RowBloomFilterEncoding) {
                fieldName = ((RowBloomFilterEncoding) encoding).getEncodingColumnName();
                fieldSchema = ((RowBloomFilterEncoding) encoding).getEncodingColumn().schema();
            }
            else {
                fieldName = ((SimpleBloomFilterEncoding) encoding).getEncodingColumnName();
                fieldSchema = ((SimpleBloomFilterEncoding) encoding).getEncodingColumn().schema();
            }
            LOG.debug("encode record : fieldName {} , fieldSchema {}",fieldName,fieldSchema);

            final Object[] objs = new Object[encoding.getSelectedColumns().size()];
            final Schema.Type[] types = new Schema.Type[encoding.getSelectedColumns().size()];
            int i = 0;
            for (Schema.Field field : encoding.getSelectedColumns()) {
                if(!BaseBloomFilterEncoding.SUPPORTED_TYPES.contains(field.schema().getType())) continue;
                objs[i] = record.get(field.name());
                types[i] = field.schema().getType();
                LOG.debug("obj[ " + i + "]" + objs[i].getClass());
                LOG.debug("type[ " + i + "]" + types[i].getClass());
                i++;
            }
            encodingRecord.put(fieldName, encoding.encode(objs, types, fieldSchema));

            for(Schema.Field field : encoding.getRestColumns()) {
                Object obj = record.get(field.name());
                encodingRecord.put(field.name(), obj);
            }
        } else {
            for(Schema.Field field : encoding.getSelectedColumns()) {
                if(!BaseBloomFilterEncoding.SUPPORTED_TYPES.contains(field.schema().getType())) continue;
                Object obj = record.get(field.name());
                Schema.Type type = field.schema().getType();
                LOG.debug("obj " + obj.getClass());
                LOG.debug("type " + type.getClass());
                Schema.Field encodingField = ((MultiBloomFilterEncoding) encoding).getEncodingColumnForName(field.name());
                String fieldName = encodingField.name();
                Schema fieldSchema = encodingField.schema();
                LOG.debug("encode record : fieldName {} , fieldSchema {}",fieldName,fieldSchema);
                encodingRecord.put(fieldName, encoding.encode(obj,type,fieldSchema));
            }
            for(Schema.Field field : encoding.getRestColumns()) {
                Object obj = record.get(field.name());
                encodingRecord.put(field.name(), obj);
            }
        }
        return encodingRecord;
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
        if(!BaseBloomFilterEncoding.AVAILABLE_METHODS.contains(methodName))
            throw new BloomFilterEncodingException("Unsupported method name : " + methodName);
        final List<EncodedDataset> encodedDatasets = new ArrayList<EncodedDataset>();
        for(Dataset d : datasets) {
            if(!d.isValid()) continue;
            if(((EncodedDataset)d).getEncoding().getName().contains(methodName))
                encodedDatasets.add((EncodedDataset) d);
        }
        return encodedDatasets;
    }

    private List<EncodedDataset> filterEncodedDatasetsByNames(final String datasetName, final String methodName)
            throws BloomFilterEncodingException {

        if(!BaseBloomFilterEncoding.AVAILABLE_METHODS.contains(methodName))
            throw new BloomFilterEncodingException("Unsupported method name : " + methodName);

        final List<EncodedDataset> encodedDatasets = new ArrayList<EncodedDataset>();
        for(Dataset d : datasets) {
            if(((EncodedDataset)d).isOrphan()) continue;
            if(!((EncodedDataset) d).getDatasetName().equals(datasetName)) continue;
            if(!d.isValid()) continue;
            if(((EncodedDataset)d).getEncoding().getName().contains(methodName))
                encodedDatasets.add((EncodedDataset) d);
        }
        return encodedDatasets;
    }
}
