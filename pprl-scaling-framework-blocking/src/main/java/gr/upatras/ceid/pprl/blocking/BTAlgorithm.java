package gr.upatras.ceid.pprl.blocking;

import java.util.BitSet;

public interface BTAlgorithm {
    int UNDEFINED = -1;
    int IGNORE_BIT = -1;

    void prepare(final BitSet[] bitSets);

    void run();

    java.util.List<Bucket> getBUCKETS();
}
