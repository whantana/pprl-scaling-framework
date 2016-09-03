package gr.upatras.ceid.pprl.blocking;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * Hamming LSH Blocking Result class.
 */
public class HammingLSHBlockingResult {
    private final List<RecordIdPair> matchedPairs;
    private long bobBlockingTime;
    private long fpsTime;
    private int matchedPairsCount;
    private int frequentPairsCount;
    private long bobBlockingSize;
    private int trullyMatchedCount;

    public HammingLSHBlockingResult() {
        matchedPairs = new LinkedList<RecordIdPair>();
        matchedPairsCount = 0;
        frequentPairsCount = 0;
        bobBlockingSize = 0;
        trullyMatchedCount = 0;
    }

    /**
     * Constructor
     *
     * @param matchedPairs       matched pair list.
     * @param matchedPairsCount  matched pair count.
     * @param frequentPairsCount frequent pair count.
     */
    public HammingLSHBlockingResult(final List<RecordIdPair> matchedPairs,
                                    final int matchedPairsCount,
                                    final int frequentPairsCount) {
        this.matchedPairs = matchedPairs;
        this.matchedPairsCount = matchedPairsCount;
        this.frequentPairsCount = frequentPairsCount;
    }

    /**
     * Save record id pairs in path.
     *
     * @param fs a <code>FileSystem</code> reference.
     * @param outputPath output path (file).
     * @param result a blocking result.
     */
    public static void saveBlockingResult(final FileSystem fs, final Path outputPath,
                                          final HammingLSHBlockingResult result)
            throws IOException {
        final FSDataOutputStream fsdos = fs.create(outputPath, true);
        fsdos.writeBytes("Bob buckets generation time :" + timeToStr(result.getBobBlockingTime()) + "\n" );
        fsdos.writeBytes("Bob buckets size in bytes :" + result.getBobBlockingSize() + "\n" );
        fsdos.writeBytes("FPS execution time :" + timeToStr(result.getFpsTime()) + "\n" );
        fsdos.writeBytes("Frequent Pairs : " + result.getFrequentPairsCount() + "\n");
        fsdos.writeBytes("Trully matched Pairs : " + result.getTrullyMatchedCount() + "\n");
        fsdos.writeBytes("Matched Pairs : " + result.getMatchedPairsCount() + "\n");
        fsdos.writeBytes("Pairs:\n");

        if(result.getMatchedPairsCount() <= 100) {
            for (RecordIdPair pair : result.getMatchedPairs()) {
                String ps = String.format("%s , %s\n", pair.aliceId, pair.bobId);
                fsdos.writeBytes(ps);
            }
        } else {
            for (int i = 0 ; i <= 25; i++) {
                RecordIdPair pair = result.getMatchedPairs().get(i);
                String ps = String.format("%s , %s\n", pair.aliceId, pair.bobId);
                fsdos.writeBytes(ps);
            }
            fsdos.writeBytes("...\n...\n...\n");
            for (int i = result.getMatchedPairsCount()-25 ; i < result.getMatchedPairsCount(); i++) {
                RecordIdPair pair = result.getMatchedPairs().get(i);
                String ps = String.format("%s , %s\n", pair.aliceId, pair.bobId);
                fsdos.writeBytes(ps);
            }
        }

        fsdos.close();
    }

    private static String timeToStr(final long ms) {
        if(ms >= 1000) {
            double s = (double) ms/ (double) 1000;
            return String.format("%.2f seconds",s);
        } else {
            return String.format("%d milliseconds",ms);
        }
    }

    public void addPair(final String idA, final String idB) {
        matchedPairs.add(new RecordIdPair(idA,idB));
    }

    /**
     * Returns matched pair list.
     *
     * @return matched pair list.
     */
    public List<RecordIdPair> getMatchedPairs() {
        return matchedPairs;
    }

    /**
     * Returns matched pairs count.
     *
     * @return matched pair count.
     */
    public int getMatchedPairsCount() {
        return matchedPairsCount;
    }

    /**
     * Returns frequent pair count.
     *
     * @return frequent pair count.
     */
    public int getFrequentPairsCount() {
        return frequentPairsCount;
    }

    public void increaseMatchedPairsCount() {
        this.matchedPairsCount++;
    }

    public void increaseTrullyMatchedCount() { this.trullyMatchedCount++; }

    public void increaseFrequentPairsCount() {
        this.frequentPairsCount++;
    }

    public long getBobBlockingTime() {
        return bobBlockingTime;
    }

    public void setBobBlockingTime(long time) {
        bobBlockingTime= time;
    }

    public long getFpsTime() {
        return fpsTime;
    }

    public void setFpsTime(long time) {
        fpsTime = time;
    }

    public long getBobBlockingSize() {
        return bobBlockingSize;
    }

    public void setBobBlockingSize(long bobBlockingSize) {
        this.bobBlockingSize = bobBlockingSize;
    }

    public int getTrullyMatchedCount() {
        return trullyMatchedCount;
    }

    /**
     * Record ID pair class.
     */
    public static class RecordIdPair {
        public String aliceId;
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
}
