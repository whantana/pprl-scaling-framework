package gr.upatras.ceid.pprl.encoding;

import gr.upatras.ceid.pprl.datasets.Dataset;

public class EncodedDataset extends Dataset{
    private Dataset dataset;
    private BloomFilterEncoding encoding;

    public EncodedDataset() {}

    public EncodedDataset(final String name, final String basePath) {
        this(name, basePath + "/" + name,
                basePath + "/" + name + "/avro",
                basePath + "/" + name + "/schema");
    }

    public EncodedDataset(final String name, final String basePath,
                   final String avroPath, final String avroSchemaPath) {
        this.name = name;
        this.basePath = basePath;
        this.avroPath = avroPath;
        this.avroSchemaPath = avroSchemaPath;
    }

    public Dataset getDataset() {
        return dataset;
    }

    public void setDataset(Dataset dataset) {
        this.dataset = dataset;
    }

    public BloomFilterEncoding getEncoding() {
        return encoding;
    }

    public void setEncoding(BloomFilterEncoding encoding) {
        this.encoding = encoding;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        EncodedDataset that = (EncodedDataset) o;

        return dataset.equals(that.dataset) &&
                !(encoding != null ? !encoding.equals(that.encoding) : that.encoding != null);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (dataset != null ? dataset.hashCode() : 0);
        return result;
    }

//    public boolean isValid() {
//        return super.isValid() & (encoding != null);
//    }
//
//    public static String toString(final EncodedDataset encodedDataset)
//            throws EncodedDatasetException {
//        if(!encodedDataset.isValid()) throw new EncodedDatasetException("Encoded Dataset is not valid.");
//        final String datasetName = (encodedDataset.isNotOrphan()) ? encodedDataset.getDatasetName() : null;
//        final String name = encodedDataset.getName();
//        final Path basePath = encodedDataset.getBasePath();
//        final Path avroPath = encodedDataset.getAvroPath();
//        final Path avroSchemaPath = encodedDataset.getAvroSchemaPath();
//        final BloomFilterEncoding encoding = encodedDataset.getEncoding();
//        return String.format("%s => %s %s %s => %s",
//                (datasetName !=null )? datasetName + "#" + name : name,
//                basePath,avroPath,avroSchemaPath,encoding.toString());
//    }
//
//    public static EncodedDataset fromString(final String s) throws EncodedDatasetException {
//        final String[] parts = s.split(" => ");
//
//        if(parts.length != 3)
//            throw new EncodedDatasetException("String \"" + s + "\" is invalid encoded dataset string.");
//
//        final String namePart = parts[0];
//        final String pathsPart = parts[1];
//        final String encPart = parts[2];
//
//        final String datasetName = namePart.contains("#") ? namePart.split("#")[0] : null;
//        final String name = namePart.contains("#") ? namePart.split("#")[1] : namePart;
//        final Path basePath = new Path(pathsPart.split(" ")[0]);
//        final Path avroPath = new Path(pathsPart.split(" ")[1]);
//        final Path avroSchemaPath = new Path(pathsPart.split(" ")[2]);
//
//        final EncodedDataset encodedDataset = (datasetName == null) ?
//                new EncodedDataset(name,basePath,avroPath,avroSchemaPath) :
//                new EncodedDataset(name,datasetName,basePath,avroPath,avroSchemaPath);
//
//        try {
//            final BloomFilterEncoding encoding;
//            encoding = BloomFilterEncoding.fromString(encPart);
//            encodedDataset.setEncoding(encoding);
//        } catch (BloomFilterEncodingException e) {
//            throw new EncodedDatasetException(e.getMessage());
//        }
//
//        return encodedDataset;
//    }
}
