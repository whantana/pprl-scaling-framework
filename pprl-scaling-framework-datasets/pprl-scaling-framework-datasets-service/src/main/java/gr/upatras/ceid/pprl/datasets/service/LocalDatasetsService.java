package gr.upatras.ceid.pprl.datasets.service;

import gr.upatras.ceid.pprl.datasets.DatasetException;
import gr.upatras.ceid.pprl.datasets.DatasetStatistics;
import gr.upatras.ceid.pprl.datasets.DatasetsUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FSDataInputStream;
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

import java.io.IOException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

@Service
public class LocalDatasetsService implements InitializingBean {

    private static final Logger LOG = LoggerFactory.getLogger(LocalDatasetsService.class);

    public void afterPropertiesSet() {
        LOG.info("Local Dataset service initialized.");
    }

    public static final FsPermission ONLY_OWNER_PERMISSION
            = new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE, false);

    private static SecureRandom RANDOM = new SecureRandom();

    @Autowired
    private FileSystem localFs;

    public FileSystem getLocalFs() {
        return localFs;
    }

    public void setLocalFS(FileSystem localFs) {
        this.localFs = localFs;
    }

    public GenericRecord[] sample(final Path[] avroPaths,
                                  final Path schemaPath,
                                  final int sampleSize)
            throws IOException, DatasetException {
        try {
            LOG.info(String.format("Sampling local dataset [size=%d,avroPath=%s,schemaPath=%s]", sampleSize,
                    Arrays.toString(avroPaths), schemaPath));
            final Schema schema = DatasetsUtil.loadSchemaFromFSPath(localFs, schemaPath);
            final DatasetsUtil.DatasetRecordReader reader = new DatasetsUtil.DatasetRecordReader(
                    localFs, schema, avroPaths);
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

    public GenericRecord[] loadRecords(final Path[] avroPaths,
                                       final Path schemaPath)
            throws DatasetException, IOException {
        try{
            LOG.info(String.format("Loading local dataset [avroPath=%s,schemaPath=%s]",
                    Arrays.toString(avroPaths), schemaPath));
            final Schema schema = DatasetsUtil.loadSchemaFromFSPath(localFs, schemaPath);
            final List<GenericRecord> rlist = new ArrayList<GenericRecord>();
            final DatasetsUtil.DatasetRecordReader reader = new DatasetsUtil.DatasetRecordReader(
                    localFs, schema, avroPaths);
            int i = 0;
            while (reader.hasNext()) {
                rlist.add(i,reader.next());
                i++;
                LOG.info("Loading Record #{}",i);
            }
            GenericRecord[] records = new GenericRecord[rlist.size()];
            LOG.info("Loaded records count = {}",records.length);
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

    public Path saveRecords(final String name,
                            final GenericRecord[] records, final Schema schema)
            throws DatasetException, IOException {
        return saveRecords(name, records, schema, localFs.getWorkingDirectory());
    }

    public Path saveRecords(final String name,
                            final GenericRecord[] records, final Schema schema,
                            final Path parent)
            throws DatasetException, IOException {
        try {
            final Path[] dataset = DatasetsUtil.createDatasetDirectories(localFs,name,parent);
            final Path basePath = dataset[0];
            final Path avroPath = dataset[1];
            final Path schemaPath = dataset[2];
            DatasetsUtil.saveSchemaToFSPath(localFs, schema, schemaPath);
            final DatasetsUtil.DatasetRecordWriter writer =
                    new DatasetsUtil.DatasetRecordWriter(localFs,name,schema,avroPath);
            writer.writeRecords(records);
            writer.close();
            return basePath;
        } catch (DatasetException e) {
            LOG.error(e.getMessage());
            throw e;
        } catch (IOException e) {
            LOG.error(e.getMessage());
            throw e;
        }
    }

    public Schema loadSchema(final Path schemaPath)
            throws IOException, DatasetException {
        try {
            LOG.info(String.format("Loading schema [path=%s]",schemaPath));
            return DatasetsUtil.loadSchemaFromFSPath(localFs,schemaPath);
        } catch (DatasetException e) {
            LOG.error(e.getMessage());
            throw e;
        } catch (IOException e) {
            LOG.error(e.getMessage());
            throw e;
        }
    }

    public Path saveStats(final String name,
                          final DatasetStatistics statistics)
            throws IOException, DatasetException {
        return saveStats(name, statistics, localFs.getWorkingDirectory());
    }

    public Path saveStats(final String name,
                          final DatasetStatistics statistics,
                          final Path parent)
            throws IOException, DatasetException {
        try {
            if(!localFs.exists(parent) && !localFs.mkdirs(parent,ONLY_OWNER_PERMISSION))
                    throw new DatasetException(String.format("Cannot create dir \"%s\"",parent));
            final Path path = new Path(parent,name + ".properties");
            LOG.info("Saving stats to [path={}]",path);
            FSDataOutputStream fsdos = localFs.create(path);
            statistics.toProperties().store(fsdos, "statistics");
            fsdos.close();
            return path;
        } catch (IOException e) {
            LOG.error(e.getMessage());
            throw e;
        } catch (DatasetException e) {
            LOG.error(e.getMessage());
            throw e;
        }
    }

    public DatasetStatistics loadStats(final Path propertiesPath)
            throws IOException, DatasetException {
        try {
            if (!localFs.exists(propertiesPath))
                throw new DatasetException(String.format("Cannot find file \"%s\"", propertiesPath));
            LOG.info("Loading stats from [path={}]",propertiesPath);
            Properties properties = new Properties();
            FSDataInputStream fsdis = localFs.open(propertiesPath);
            properties.load(fsdis);
            fsdis.close();
            DatasetStatistics statistics = new DatasetStatistics();
            statistics.fromProperties(properties);
            return statistics;
        } catch (IOException e) {
            LOG.error(e.getMessage());
            throw e;
        } catch (DatasetException e) {
            LOG.error(e.getMessage());
            throw e;
        }
    }
}
