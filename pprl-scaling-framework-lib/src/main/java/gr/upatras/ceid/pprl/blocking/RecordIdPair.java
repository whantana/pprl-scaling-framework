package gr.upatras.ceid.pprl.blocking;

/**
 * Record ID pair class.
 */
public class RecordIdPair {
    public int aliceId; // ids are essentially indexes of GenericRecord arrays
    public int bobId;

    /**
     * Constructor.
     *
     * @param aliceId alice record id.
     * @param bobId bob record id.
     */
    public RecordIdPair(int aliceId, int bobId) {
        this.aliceId = aliceId;
        this.bobId = bobId;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RecordIdPair idPair = (RecordIdPair) o;

        if (aliceId != idPair.aliceId) return false;
        return bobId == idPair.bobId;

    }

    @Override
    public int hashCode() {
        int result = aliceId;
        result = 31 * result + bobId;
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

