package gr.upatras.ceid.pprl.encoding;

public class RowBloomFilterEncoding extends FieldBloomFilterEncoding {

    public RowBloomFilterEncoding(){}  // TODO a lot

    public String getName() {
        return "RBF_" + (hasMultiN() ? String.format("%d_%d",K,Q) : String.format("%d_%d_%d",N[0],K,Q));
    }

    public String toString() { return getName(); }
}
