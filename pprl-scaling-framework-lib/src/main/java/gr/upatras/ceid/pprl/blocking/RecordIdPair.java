package gr.upatras.ceid.pprl.blocking;

/**
 * Record ID pair class.
 */
public class RecordIdPair {
    public String aliceId; // ids are essentially indexes of GenericRecord arrays
    public String bobId;


    /**
     * Constructor.
     *
     * @param aliceId alice record id.
     * @param bobId bob record id.
     */
    public RecordIdPair(final String aliceId, final String bobId) {
        this.aliceId = aliceId;
        this.bobId = bobId;
    }

    /**
     * Constructor.
     *
     * @param aliceId alice record id.
     * @param bobId bob record id.
     */
    public RecordIdPair(final String aliceId, final String bobId,
                        final double h, final double j, final double d
                        ) {
        this.aliceId = aliceId;
        this.bobId = bobId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RecordIdPair idPair = (RecordIdPair) o;


        return aliceId.equals(idPair.aliceId) &&
               bobId.equals(idPair.bobId);

    }

    @Override
    public int hashCode() {
        int result = aliceId.hashCode();
        result = 31 * result + bobId.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "[id.a=" +
                aliceId +
                ", id.b=" +
                bobId +
                ']';
    }
}

