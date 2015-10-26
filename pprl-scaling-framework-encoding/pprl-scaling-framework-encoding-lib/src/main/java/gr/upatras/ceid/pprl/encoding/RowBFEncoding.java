package gr.upatras.ceid.pprl.encoding;

import org.apache.avro.Schema;

import java.util.Collections;
import java.util.List;

public class RowBFEncoding extends BaseBFEncoding {


    public RowBFEncoding(Schema schema, String uidColumnName, List<String> selectedColumnNames,
                         int N, int K, int Q) throws BFEncodingException {
        super(schema, uidColumnName, selectedColumnNames, N, K, Q);
    }

    public RowBFEncoding(Schema schema, Schema encodingSchema, String uidColumnName, List<String> selectedColumnNames,
                         int n, int k, int q) throws BFEncodingException {
        super(schema, encodingSchema, uidColumnName, selectedColumnNames, n, k, q);
    }

    protected String getName() {
        return "_RBF" + super.getName();
    }

    @Override
    public void generateEncodingColumnNames() throws BFEncodingException {
        super.generateEncodingColumnNames();
        StringBuilder sb = new StringBuilder("enc");
        sb.append(getName());
        for(String column : selectedColumnNames) sb.append(column).append("_");
        sb.deleteCharAt(sb.lastIndexOf("_"));
        encodingColumnNames = Collections.singletonList(sb.toString());
    }

    @Override
    public Object encode(Object obj, Class<?> clz) {
        return null;
    }

    @Override
    public String toString() {
        return "RBFEncoding" + super.toString();
    }
}
