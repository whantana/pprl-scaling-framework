package gr.upatras.ceid.pprl.encoding;

import gr.upatras.ceid.pprl.datasets.Dataset;
import org.apache.hadoop.fs.Path;


public class EncodedDataset extends Dataset {

    public EncodedDataset(String name, Path basePath, Path avroPath, Path avroSchemaPath) {
        super(name, basePath, avroPath, avroSchemaPath);
    }

    // todo more! (Refactore encodings as well)
}
