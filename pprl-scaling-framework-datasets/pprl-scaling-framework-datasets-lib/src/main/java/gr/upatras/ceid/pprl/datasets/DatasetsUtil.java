package gr.upatras.ceid.pprl.datasets;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

public class DatasetsUtil {

    private static final Logger LOG = LoggerFactory.getLogger(DatasetsUtil.class);

    private static boolean fsIsLocal(final FileSystem fs) {
        return fs.getScheme().equals("file");
    }

    public static Schema loadSchemaFromFSPath(final FileSystem fs, final Path schemaPath)
            throws DatasetException, IOException {
        LOG.info(String.format("Loading path [FileSystem=%s,Path=%s]",
                fsIsLocal(fs) ? "local" : fs.getUri(), schemaPath));
        FileStatus fss = fs.getFileStatus(schemaPath);
        if (fss.isFile() && fss.getPath().getName().endsWith(".avsc"))
            return (new Schema.Parser()).parse(fs.open(fss.getPath()));
        else throw new DatasetException("Path provided not a schema file.");
    }

    public static void saveSchemaToFSPath(final FileSystem fs, final Schema schema, final Path schemaPath)
            throws DatasetException, IOException {
        LOG.info(String.format("Loading path [FileSystem=%s,Path=%s]",
                fsIsLocal(fs) ?"local":fs.getUri(),schemaPath));
        final FSDataOutputStream fsdos = fs.create(schemaPath, true);
        fsdos.write(schema.toString(true).getBytes());
        fsdos.close();
    }

    public static class DatasetRecordReader implements Iterator<GenericRecord>, Closeable {
        private static final Logger LOG = LoggerFactory.getLogger(DatasetRecordReader.class);
        private List<FileReader<GenericRecord>> fileReaders;
        private int current;

        public DatasetRecordReader(final FileSystem fs, final Schema schema, final Path parentPath)
                throws IOException {
            List<Path> filePaths = new ArrayList<Path>();
            for (FileStatus s : fs.listStatus(parentPath))
                if (s.isFile() && s.getPath().toString().endsWith(".avro")) {
                    filePaths.add(s.getPath());
                }
            final SortedSet<Path> paths = new TreeSet<Path>(filePaths);
            int count = paths.size();
            LOG.info(String.format("Found %d files to read [FileSystem=%s,Path=%s]",
                    count, fsIsLocal(fs) ?"local":fs.getUri(),parentPath));
            for (Path p : paths) {
                LOG.debug("Creating reader for file {}", p);
                fileReaders.add(DataFileReader.openReader(
                        new FsInput(p, fs.getConf()), new GenericDatumReader<GenericRecord>(schema)));
            }
            current = 0;
        }

        public DatasetRecordReader(final FileSystem fs, final Schema schema, final Path[] filePaths)
                throws IOException {
            final SortedSet<Path> paths = new TreeSet<Path>(Arrays.asList(filePaths));
            int count = paths.size();
            LOG.info(String.format("Found %d files to read [FileSystem=%s,Paths=%s]",
                    count, fsIsLocal(fs) ? "local" : fs.getUri(), Arrays.toString(filePaths)));
            fileReaders = new ArrayList<FileReader<GenericRecord>>();
            for (Path p : paths) {
                LOG.debug("Creating reader for file {}", p);
                fileReaders.add(DataFileReader.openReader(
                        new FsInput(p, fs.getConf()), new GenericDatumReader<GenericRecord>(schema)));
            }
            current = 0;
        }


        public void close() throws IOException {
            if (current < fileReaders.size())
                fileReaders.get(current).close();
            for (FileReader f : fileReaders)
                if (f != null) f.close();
        }

        public boolean hasNext() {
            if (fileReaders.get(current).hasNext()) return true;
            if (current < fileReaders.size()) {
                try {
                    fileReaders.get(current).close();
                } catch (IOException e) {
                    LOG.error("Can't close this!");
                }
                LOG.debug("Moving readers {} -> {}", current, current + 1);
                LOG.debug("Before : {}", fileReaders.get(current));
                current++;
                if (current >= fileReaders.size()) return false;
                LOG.debug("After current : {} hasNext ? {} ", fileReaders.get(current),
                        fileReaders.get(current).hasNext());
                return fileReaders.get(current).hasNext();
            }
            return false;
        }

        public GenericRecord next() {
            LOG.debug("Current reader : {}", current);
            return fileReaders.get(current).next();
        }

        public void remove() {
            throw new UnsupportedOperationException("Not supported");
        }
    }
}
