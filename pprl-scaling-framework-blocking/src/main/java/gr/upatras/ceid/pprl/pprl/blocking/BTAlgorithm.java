package gr.upatras.ceid.pprl.pprl.blocking;

import java.util.BitSet;

public interface BTAlgorithm {
    public static final int UNDEFINED = -1;
    public static final int IGNORE_BIT = -1;

    public void prepare(final BitSet[] bitSets);
    public void run();
    public java.util.List<Bucket> getBUCKETS();
}
