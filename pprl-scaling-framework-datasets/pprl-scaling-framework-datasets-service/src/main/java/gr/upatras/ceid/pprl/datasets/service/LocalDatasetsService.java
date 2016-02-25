package gr.upatras.ceid.pprl.datasets.service;

import gr.upatras.ceid.pprl.datasets.DatasetException;
import gr.upatras.ceid.pprl.datasets.DatasetsUtil;
import gr.upatras.ceid.pprl.datasets.statistics.CombinatoricsUtil;
import gr.upatras.ceid.pprl.datasets.statistics.DatasetStatistics;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
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
import java.util.List;

@Service
public class LocalDatasetsService implements InitializingBean {
    // TODO Load FULL dataset in memory service

    private static final Logger LOG = LoggerFactory.getLogger(LocalDatasetsService.class);

    public void afterPropertiesSet() {
        LOG.info("Local Dataset service initialized.");
    }

    public static final FsPermission ONLY_OWNER_PERMISSION
            = new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE, false);

    private static SecureRandom RANDOM = new SecureRandom();

    @Autowired
    private FileSystem localFS;

    public FileSystem getLocalFS() {
        return localFS;
    }

    public void setLocalFS(FileSystem localFS) {
        this.localFS = localFS;
    }

    public GenericRecord[] sampleOfLocalDataset(final Path[] avroPaths,
                                                final Path schemaPath,
                                                final int sampleSize)
            throws IOException, DatasetException {
        try {
            LOG.info(String.format("Sampling local dataset [Size=%d,Avro=%s,Schema=%s]", sampleSize,
                    Arrays.toString(avroPaths), schemaPath));
            final Schema schema = DatasetsUtil.loadSchemaFromFSPath(localFS, schemaPath);
            final DatasetsUtil.DatasetRecordReader reader = new DatasetsUtil.DatasetRecordReader(
                    localFS, schema, avroPaths);
            final GenericRecord[] sample = new GenericRecord[sampleSize];
            int size = sampleSize;
            while (reader.hasNext()) {
                GenericRecord record = reader.next();
                if (RANDOM.nextBoolean()) {
                    sample[sampleSize - size] = record;
                    size--;
                    if (size== 0) break;
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

    public GenericRecord[] loadLocalDataset(final Path[] avroPaths,
                                            final Path schemaPath)
            throws DatasetException, IOException {
        try{
            LOG.info(String.format("Loading local dataset [Avro=%s,Schema=%s]",
                    Arrays.toString(avroPaths), schemaPath));
            final Schema schema = DatasetsUtil.loadSchemaFromFSPath(localFS, schemaPath);
            final List<GenericRecord> rlist = new ArrayList<GenericRecord>();
            final DatasetsUtil.DatasetRecordReader reader = new DatasetsUtil.DatasetRecordReader(
                    localFS, schema, avroPaths);
            int i = 0;
            while (reader.hasNext()) {
                rlist.add(i,reader.next());
                i++;
            }
            GenericRecord[] records = new GenericRecord[rlist.size()];
            records = rlist.toArray(records);
            return records;
        } catch (DatasetException e) {
            LOG.error(e.getMessage());
            throw e;
        } catch (IOException e) {
            LOG.error(e.getMessage());
            throw e;
        }
    }

    public void localSaveOfSample(final String name,
                                  final GenericRecord[] records, final Schema schema)
            throws DatasetException, IOException {
        localSaveOfSample(name,records,schema, localFS.getWorkingDirectory());
    }

    public void localSaveOfSample(final String name,
                                  final GenericRecord[] records, final Schema schema,
                                  final Path parent)
            throws DatasetException, IOException {
        try {
            final Path basePath = new Path(parent,name);
            if(!localFS.mkdirs(basePath, ONLY_OWNER_PERMISSION))
                throw new DatasetException(String.format("Cannot create base path \"%s\".",basePath));
            LOG.info("Saving sample at {}",basePath);
            final Path baseSchemaPath = new Path(basePath,"schema");
            if(!localFS.mkdirs(baseSchemaPath,ONLY_OWNER_PERMISSION))
                throw new DatasetException(String.format("Cannot create base schema path \"%s\".",basePath));
            final Path schemaPath = new Path(baseSchemaPath,name + ".avsc");
            DatasetsUtil.saveSchemaToFSPath(localFS, schema, schemaPath);
            final Path baseAvroPath = new Path(basePath,"avro");
            if(!localFS.mkdirs(baseAvroPath,ONLY_OWNER_PERMISSION))
                throw new DatasetException(String.format("Cannot create base avro path \"%s\".",basePath));
            final DatasetsUtil.DatasetRecordWriter writer =
                    new DatasetsUtil.DatasetRecordWriter(localFS,name,schema,baseAvroPath);
            writer.writeRecords(records);
            writer.close();
        } catch (DatasetException e) {
            LOG.error(e.getMessage());
            throw e;
        } catch (IOException e) {
            LOG.error(e.getMessage());
            throw e;
        }
    }

    public Schema schemaOfLocalDataset(final Path schemaPath)
            throws IOException, DatasetException {
        try {
            return DatasetsUtil.loadSchemaFromFSPath(localFS,schemaPath);
        } catch (DatasetException e) {
            LOG.error(e.getMessage());
            throw e;
        } catch (IOException e) {
            LOG.error(e.getMessage());
            throw e;
        }
    }

    public DatasetStatistics calculateStatisticsLocalDataset(final Path[] avroPaths,
                                                             final Path schemaPath,
                                                             String[] fieldNames)
            throws IOException, DatasetException {
        try {
            final Schema schema = DatasetsUtil.loadSchemaFromFSPath(localFS, schemaPath);
            if (fieldNames == null || fieldNames.length == 0) {
                fieldNames = new String[schema.getFields().size()];
                for (int i = 0; i < fieldNames.length; i++)
                    fieldNames[i] = schema.getFields().get(i).name();
            }
            DatasetsUtil.DatasetRecordReader reader = new DatasetsUtil.DatasetRecordReader(
                    localFS, schema, avroPaths);
            List<GenericRecord> rlist = new ArrayList<GenericRecord>();
            while (reader.hasNext()) rlist.add(reader.next());
            reader.close();
            GenericRecord[] records = rlist.toArray(new GenericRecord[rlist.size()]);

            return calculateStatisticsLocalDataset(records, schema, fieldNames);
        } catch (DatasetException e) {
            LOG.error(e.getMessage());
            throw e;
        }
    }

    private DatasetStatistics calculateStatisticsLocalDataset(final GenericRecord[] records,
                                                              final Schema schema,
                                                              final String[] fieldNames)
            throws IOException {
        final DatasetStatistics statistics = new DatasetStatistics();
        statistics.setRecordCount(records.length);
        statistics.setPairCount(CombinatoricsUtil.twoCombinationsCount(records.length));
        statistics.setFieldNames(fieldNames);
        statistics.calculateFieldStatistics(records, schema, fieldNames);
        return statistics;
    }

    public void localSaveOfStatsReport(final String name,
                                       final String report)
            throws IOException, DatasetException {
        localSaveOfStatsReport(name, report , localFS.getWorkingDirectory());
    }

    public void localSaveOfStatsReport(final String name,
                                       final String report,
                                       final Path parent)
            throws IOException, DatasetException {
        try {
            if(!localFS.exists(parent) && !localFS.mkdirs(parent,ONLY_OWNER_PERMISSION))
                    throw new DatasetException(String.format("Cannot create dir \"%s\"",parent));
            final Path reportPath = new Path(parent,name + ".txt");
            FSDataOutputStream fsdos;
            fsdos = localFS.create(reportPath);
            fsdos.write(report.getBytes());
            fsdos.close();
        } catch (IOException e) {
            LOG.error(e.getMessage());
            throw e;
        } catch (DatasetException e) {
            LOG.error(e.getMessage());
            throw e;
        }
    }

    public void localSaveOfStatsReport(final DatasetStatistics statistics) {
        localSaveOfStatsReport(statistics, localFS.getWorkingDirectory());
    }

    public void localSaveOfStatsReport(final DatasetStatistics statistics, final Path parent) {
        // might export them as properties... who knows
    }
}
