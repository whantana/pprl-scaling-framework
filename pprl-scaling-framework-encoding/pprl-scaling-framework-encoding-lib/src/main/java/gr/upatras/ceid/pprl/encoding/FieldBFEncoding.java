package gr.upatras.ceid.pprl.encoding;

import org.apache.avro.Schema;

import java.util.ArrayList;
import java.util.List;

public class FieldBFEncoding extends BaseBFEncoding{


    public FieldBFEncoding(Schema schema, String uidColumnName, List<String> selectedColumnNames, int N, int K, int Q)
            throws BFEncodingException {
        super(schema, uidColumnName, selectedColumnNames, N, K, Q);
    }

    public FieldBFEncoding(Schema schema, Schema encodingSchema, String uidColumnName, List<String> selectedColumnNames,
                           int n, int k, int q) throws BFEncodingException {
        super(schema, encodingSchema, uidColumnName, selectedColumnNames, n, k, q);
    }

    protected String getName() {
        return "_FBF" + super.getName();
    }

    @Override
    public void generateEncodingColumnNames() throws BFEncodingException {
        super.generateEncodingColumnNames();
        encodingColumnNames = new ArrayList<String>();
        final StringBuilder sb = new StringBuilder("enc");
        sb.append(getName());
        for(String column : selectedColumnNames) {
            final StringBuilder nsb = new StringBuilder(sb.toString());
            nsb.append(column).append("_");
            nsb.deleteCharAt(nsb.lastIndexOf("_"));
            encodingColumnNames.add(nsb.toString());
        }
    }

    @Override
    public Object encode(Object obj, Class<?> clz) {
        return null;
    }

    @Override
    public String toString() {
        return "FBFEncoding" + super.toString();
    }
}
