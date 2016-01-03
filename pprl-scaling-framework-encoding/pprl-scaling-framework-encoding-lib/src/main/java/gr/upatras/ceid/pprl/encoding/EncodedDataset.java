package gr.upatras.ceid.pprl.encoding;

import gr.upatras.ceid.pprl.datasets.Dataset;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class EncodedDataset extends Dataset {

    private String datasetName;
    private BloomFilterEncoding encoding;

    public EncodedDataset(String name,Path userHomeDirectory) {
        super(name, userHomeDirectory);
        this.datasetName = null;
        this.encoding = null;
    }

    public EncodedDataset(String name, Path basePath, Path avroPath, Path avroSchemaPath) {
        super(name, basePath, avroPath, avroSchemaPath);
        this.datasetName = null;
        this.encoding = null;
    }

    public EncodedDataset(String name, String datasetName,
                          Path basePath, Path avroPath, Path avroSchemaPath) {
        super(name, basePath, avroPath, avroSchemaPath);
        this.datasetName = datasetName;
    }

    public String getDatasetName() {
        return datasetName;
    }

    public BloomFilterEncoding getEncoding() {
        return encoding;
    }

    public void setEncoding(BloomFilterEncoding encoding) {
        this.encoding = encoding;
        this.schema = encoding.getEncodingSchema();
    }

    public boolean isOrphan() { return datasetName == null;}

    public boolean isNotOrphan() {
        return !isOrphan();
    }

    public boolean checkIfOrphan(final FileSystem fs) throws IOException {
        boolean hasDataDir = false;
        boolean hasSchemaDir= false;
        for(FileStatus status : fs.listStatus(basePath.getParent())) {
            if(status.isDirectory() && status.getPath().getName().endsWith("avro"))
                hasDataDir = true;
            if(status.isDirectory() && status.getPath().getName().endsWith("schema"))
                hasSchemaDir = true;
        }
        return (!hasDataDir || !hasSchemaDir);
    }

    public void setOrphan() {
        datasetName = null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        EncodedDataset that = (EncodedDataset) o;

        return datasetName.equals(that.datasetName) &&
                !(encoding != null ? !encoding.equals(that.encoding) : that.encoding != null);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (datasetName != null ? datasetName.hashCode() : 0);
        return result;
    }

    public boolean isValid() {
        return super.isValid() & (encoding != null);
    }

    public static String toString(final EncodedDataset encodedDataset)
            throws EncodedDatasetException {
        if(!encodedDataset.isValid()) throw new EncodedDatasetException("Encoded Dataset is not valid.");
        final String datasetName = (encodedDataset.isNotOrphan()) ? encodedDataset.getDatasetName() : null;
        final String name = encodedDataset.getName();
        final Path basePath = encodedDataset.getBasePath();
        final Path avroPath = encodedDataset.getAvroPath();
        final Path avroSchemaPath = encodedDataset.getAvroSchemaPath();
        final BloomFilterEncoding encoding = encodedDataset.getEncoding();
        return String.format("%s => %s %s %s => %s",
                (datasetName !=null )? datasetName + "#" + name : name,
                basePath,avroPath,avroSchemaPath,encoding.toString());
    }

    public static EncodedDataset fromString(final String s) throws EncodedDatasetException {
        final String[] parts = s.split(" => ");

        if(parts.length != 3)
            throw new EncodedDatasetException("String \"" + s + "\" is invalid encoded dataset string.");

        final String namePart = parts[0];
        final String pathsPart = parts[1];
        final String encPart = parts[2];

        final String datasetName = namePart.contains("#") ? namePart.split("#")[0] : null;
        final String name = namePart.contains("#") ? namePart.split("#")[1] : namePart;
        final Path basePath = new Path(pathsPart.split(" ")[0]);
        final Path avroPath = new Path(pathsPart.split(" ")[1]);
        final Path avroSchemaPath = new Path(pathsPart.split(" ")[2]);

        final EncodedDataset encodedDataset = (datasetName == null) ?
                new EncodedDataset(name,basePath,avroPath,avroSchemaPath) :
                new EncodedDataset(name,datasetName,basePath,avroPath,avroSchemaPath);

        try {
            final BloomFilterEncoding encoding;
            encoding = BloomFilterEncoding.fromString(encPart);
            encodedDataset.setEncoding(encoding);
        } catch (BloomFilterEncodingException e) {
            throw new EncodedDatasetException(e.getMessage());
        }

        return encodedDataset;
    }
}
