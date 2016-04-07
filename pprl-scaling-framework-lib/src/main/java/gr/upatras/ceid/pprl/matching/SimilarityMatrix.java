package gr.upatras.ceid.pprl.matching;

import java.util.BitSet;

/**
 * Similarity Matrix class.
 */
public class SimilarityMatrix {
    protected final int totalBits;  // Total number of bits in the bitset.
    protected final int fieldCount; // Number of fields.
    protected final int pairCount;  // Number of pairs.
    protected final int minIndex;   // minimum index = 0;
    protected final int maxIndex;   // max index = totalBits - 1;
    protected int nnzCount;         // Non zero count;
    protected BitSet bits;          // a BitSet

    /**
     * Constructor.
     *
     * @param pairCount count of pairs.
     * @param fieldCount count of fields.
     */
    public SimilarityMatrix(final int pairCount, final int fieldCount) {
        this.fieldCount = fieldCount;
        this.pairCount = pairCount;
        totalBits = ((int)pairCount)*fieldCount;
        bits = new BitSet(totalBits);
        nnzCount = 0;
        minIndex = 0;
        maxIndex = totalBits - 1;
    }

    /**
     * Sets 1 for true value, 0 for false value, at position [rank,field]
     *
     * @param rank rank of pair.
     * @param field field index.
     * @param value true sets 1 , false sets 0.
     */
    private void set(int rank,int field,boolean value) {
        int index = rank*fieldCount + field;
        assert index >= minIndex && index <= maxIndex;
        bits.set(index,value);
    }

    /**
     * Sets 1 at position [rank,field]. Increase count of non-zero elemetns.
     *
     * @param rank rank of pair.
     * @param field field index.
     */
    public void set(int rank,int field) {
        set(rank,field,true);
        nnzCount++;
    }

    /**
     * Returns value at position [rank,field].
     *
     * @param rank rank of pair.
     * @param field field index.
     * @return true if value is 1 , false if value is 0.
     */
    public boolean get(int rank,int field) {
        int index = rank*fieldCount + field;
        assert index >= minIndex && index <= maxIndex;
        return bits.get(index);
    }

    /**
     * Returns field count.
     *
     * @return field count.
     */
    public int getFieldCount() {
        return fieldCount;
    }

    /**
     * Returns pair count.
     *
     * @return pair count.
     */
    public int getPairCount() {
        return pairCount;
    }

    @Override
    public String toString() {
        return String.format("NaiveSimilarity Matrix [pairs=%d, fields= %d, minIndex = %d, maxIndex = %d, nnz= %d, nz = %d]",
                pairCount,fieldCount,minIndex,maxIndex,nnzCount,totalBits - nnzCount);
    }
}
