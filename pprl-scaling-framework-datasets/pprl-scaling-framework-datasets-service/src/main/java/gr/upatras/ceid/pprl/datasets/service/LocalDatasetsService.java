package gr.upatras.ceid.pprl.datasets.service;

import gr.upatras.ceid.pprl.datasets.DatasetException;
import gr.upatras.ceid.pprl.datasets.DatasetStatistics;
import gr.upatras.ceid.pprl.datasets.DatasetsUtil;
import gr.upatras.ceid.pprl.datasets.QGramUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class LocalDatasetsService implements InitializingBean {

    private static final Logger LOG = LoggerFactory.getLogger(LocalDatasetsService.class);

    private static SecureRandom RANDOM = new SecureRandom();

    @Autowired
    private FileSystem localFileSystem;

    public FileSystem getLocalFileSystem() {
        return localFileSystem;
    }

    public void setLocalFileSystem(FileSystem localFileSystem) {
        this.localFileSystem = localFileSystem;
    }

    public List<String> sampleOfLocalDataset(final File[] avroFiles, final File avroSchemaFile, int sampleSize)
            throws IOException, DatasetException {
        try {
            LOG.info(String.format("Sampling local dataset [Size=%d,Avro=%s,Schema=%s]", sampleSize,
                    Arrays.toString(avroFiles), avroSchemaFile.toString()));
            final Schema schema = DatasetsUtil.loadSchemaFromFSPath(localFileSystem, new Path(avroSchemaFile.toURI()));
            final Path[] filePaths = new Path[avroFiles.length];
            for (int i = 0; i < avroFiles.length; i++)
                filePaths[i] = new Path(avroFiles[i].toURI());
            final DatasetsUtil.DatasetRecordReader reader = new DatasetsUtil.DatasetRecordReader(
                    localFileSystem, schema, filePaths);
            List<String> sample = new ArrayList<String>();
            while (reader.hasNext()) {
                GenericRecord record = reader.next();
                if (RANDOM.nextBoolean()) {
                    sample.add(record.toString());
                    sampleSize--;
                    if (sampleSize == 0) break;
                }
            }
            return sample;
        } catch (DatasetException e) {
            LOG.error(e.getMessage());
            throw e;
        } catch (IOException e) {
            LOG.error(e.getMessage());
            throw e;
        }
    }

    public Map<String, String> describeLocalDataset(final File avroSchemaFile)
            throws IOException, DatasetException {
        try {
            final Schema schema = DatasetsUtil.loadSchemaFromFSPath(localFileSystem,
                    new Path(avroSchemaFile.toURI()));
            final Map<String, String> description = new HashMap<String, String>();
            for (Schema.Field f : schema.getFields())
                description.put(f.name(), f.schema().getType().toString());
            return description;
        } catch (DatasetException e) {
            LOG.error(e.getMessage());
            throw e;
        } catch (IOException e) {
            LOG.error(e.getMessage());
            throw e;
        }
    }

    public Map<String, double[]> calculateStatisticsLocalDataset(
            final File[] avroFiles, final File avroSchemaFile,
            String[] fieldNames) throws IOException, DatasetException {
        LOG.info(String.format("Calculating dataset statistics [fieldNames=%s,Avro=%s,Schema=%s]",
                Arrays.toString(fieldNames),
                Arrays.toString(avroFiles), avroSchemaFile.toString()));
        try {
            final Schema schema = DatasetsUtil.loadSchemaFromFSPath(localFileSystem, new Path(avroSchemaFile.toURI()));
            final Path[] filePaths = new Path[avroFiles.length];
            for (int i = 0; i < avroFiles.length; i++)
                filePaths[i] = new Path(avroFiles[i].toURI());
            final DatasetsUtil.DatasetRecordReader reader = new DatasetsUtil.DatasetRecordReader(
                    localFileSystem, schema, filePaths);
            List<GenericRecord> records = new ArrayList<GenericRecord>();
            while (reader.hasNext()) records.add(reader.next());
            GenericRecord[] recordArray = records.toArray(new GenericRecord[records.size()]);

            final Map<String, DatasetStatistics> stats = new HashMap<String, DatasetStatistics>();
            if (fieldNames == null) {
                fieldNames = new String[schema.getFields().size()];
                for (int i = 0; i < fieldNames.length; i++)
                    fieldNames[i] = schema.getFields().get(i).name();
            }
            for (String fieldName : fieldNames) stats.put(fieldName,new DatasetStatistics());

            calculateStatistics(recordArray, schema, stats, fieldNames);

            final Map<String, double[]> retStats = new HashMap<String, double[]>();
            for (Map.Entry<String, DatasetStatistics> entry : stats.entrySet())
                retStats.put(entry.getKey(), entry.getValue().getStats());

            return retStats;
        } catch (DatasetException e) {
            LOG.error(e.getMessage());
            throw e;
        }
    }


    private void calculateStatistics(GenericRecord[] records,
                                     final Schema schema,
                                     final Map<String, DatasetStatistics> stats, String[] fieldNames)
            throws IOException {

        int recordCount = records.length;
        int r = 0;
        for (GenericRecord record : records) {
            for (String fieldName : fieldNames) {
                DatasetStatistics fieldStats = stats.get(fieldName);
                Object obj = record.get(fieldName);
                Schema.Type type = schema.getField(fieldName).schema().getType();
                double update = (double) String.valueOf(obj).length() / (double) recordCount;
                double[] updateQgram = new double[DatasetStatistics.Q_GRAMS.length];
                for (int i = 0; i < updateQgram.length; i++) {
                    updateQgram[i] =  ((double) QGramUtil.calcQgramsCount(obj,type, DatasetStatistics.Q_GRAMS[i]) /
                                       (double) recordCount);
                }

                fieldStats.updateFieldLength((update));
                fieldStats.updateFieldQGramCount(updateQgram);
                LOG.info("Field : {}\t{}",fieldName,DatasetStatistics.prettyStats(fieldStats));
            }
            r++;
        }
    }

    public void afterPropertiesSet() throws Exception {
        LOG.info("Local Dataset service initialized.");
    }
}
