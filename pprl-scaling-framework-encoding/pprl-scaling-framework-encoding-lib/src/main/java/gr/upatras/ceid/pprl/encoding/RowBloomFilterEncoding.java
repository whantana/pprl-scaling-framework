package gr.upatras.ceid.pprl.encoding;

public class RowBloomFilterEncoding extends FieldBloomFilterEncoding {

    public RowBloomFilterEncoding(){}  // TODO a lot

    public String getName() {
        return "RBF_" + String.format("%d_%d",K,Q);
    }

    public String toString() { return "RBF"; }
}
