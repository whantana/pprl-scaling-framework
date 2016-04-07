package gr.upatras.ceid.pprl.matching;

import java.util.Arrays;
import java.util.Properties;

/**
 * Similarity vector frequencies class.
 */
public class SimilarityVectorFrequencies {
    private int fieldCount;           // field count.
    private long[] vectorFrequencies; // similarity vector frequencies.

    /**
     * Constructor.
     */
    public SimilarityVectorFrequencies() {}

    /**
     * Constructor.
     *
     * @param fieldCount field count.
     */
    public SimilarityVectorFrequencies(int fieldCount) {
        this.fieldCount = fieldCount;
        vectorFrequencies = new long[1 << fieldCount];
    }

    /**
     * Set row. Increase the frequeny of row.
     *
     * @param row boolean array (a similarity vector).
     */
    public void set(final boolean[] row) {
        assert row.length == fieldCount;
        vectorFrequencies[vector2Index(row)]++;
    }

    /**
     * Returns index of the vector in the frequency array.
     *
     * @param row boolean array (a similarity vector).
     * @return index of the vector in the frequency array.
     */
    public static int vector2Index(final boolean[] row) {
        int index = 0;
        for(int i = 0; i < row.length; i ++) if(row[i]) index |= (1 << i);
        return index;
    }

    /**
     * Returns the vector for a given index in the frequency array.
     *
     * @param index of the vector in the frequency array.
     * @param fieldCount field count.
     * @return boolean array (a similarity vector).
     */
    public static boolean[] index2Vector(final int index,final int fieldCount) {
        assert index < (1 << fieldCount);
        final boolean[] row =  new boolean[fieldCount];
        for (int i = 0; i < fieldCount; i++) {
            row[i] = ((1 << i) & index) > 0;
        }
        return row;
    }

    /**
     * Returns indexes of vectors that j-th field matches.
     *
     * @param j index of field.
     * @param fieldCount field count.
     * @return indexes of vectors that j-th field matches.
     */
    public static int[] indexesWithJset(final int j, final int fieldCount) {
        assert j < fieldCount;
        final int[] indexes = new int[1 << (fieldCount-1)];
        for (int i = 0; i < (1 << (fieldCount-1)); i++) {
            indexes[i] = (i < (1 << j)) ?
                    (1 << j) | i :
                    (((j ==0 ? i : (i >> j)) << (j +1))  |
                            (1 << j) |
                            (j ==0 ? 0: (i & (1 << j)-1)));
        }
        return indexes;
    }

    /**
     * Returns the vector frequencies array.
     *
     * @return the vector frequencies array.
     */
    public long[] getVectorFrequencies() {
        return vectorFrequencies;
    }

    /**
     * Returns the i-th vector frequency.
     *
     * @param i index
     * @return the i-th vector frequency.
     */
    public long getVectorFrequency(final int i) {
        assert i > 0 && i < vectorFrequencies.length;
        return vectorFrequencies[i];
    }

    @Override
    public String toString() {
        return "Similarity Matrix , vectorFrequencies=" + Arrays.toString(vectorFrequencies) + "]";
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SimilarityVectorFrequencies matrix = (SimilarityVectorFrequencies) o;

        if (fieldCount != matrix.fieldCount) return false;
        return Arrays.equals(vectorFrequencies, matrix.vectorFrequencies);

    }

    @Override
    public int hashCode() {
        int result = fieldCount;
        result = 31 * result + Arrays.hashCode(vectorFrequencies);
        return result;
    }

    /**
     * Return field count.
     *
     * @return field count.
     */
    public int getFieldCount() {
        return fieldCount;
    }

    /**
     * Return a <code>Properties</code> instance from this instance.
     *
     * @return a <code>Properties</code> instance from this instance.
     */
    public Properties toProperties() {
        final Properties properties = new Properties();
        properties.setProperty("field.count",String.valueOf(fieldCount));
        for (int i = 0; i < vectorFrequencies.length; i++)
            properties.setProperty(String.format("vec.%d",i),String.valueOf(vectorFrequencies[i]));
        return properties;
    }

    /**
     * Instantiates this instance from a <code>Properties</code> instance.
     *
     * @param properties a <code>Properties</code> instance.
     */
    public void fromProperties(final Properties properties) {
        if(properties.getProperty("field.count") == null)
            throw new IllegalStateException("Requires field.count");

        fieldCount = Integer.valueOf(properties.getProperty("field.count"));
        vectorFrequencies = new long[1 << fieldCount];
        for (int i = 0; i < vectorFrequencies.length; i++)
            vectorFrequencies[i] = Long.valueOf(properties.getProperty(String.format("vec.%d",i)));
    }
}
