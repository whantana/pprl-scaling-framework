package gr.upatras.ceid.pprl.blocking;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Blocking Utiltity class.
 */
public class BlockingUtil {

    public static final List<String> SCHEME_NAMES = new ArrayList<String>(); // Available Encoding Schemes
    static {
        SCHEME_NAMES.add("HLSH_FPS");
        SCHEME_NAMES.add("HLSH_MR");
        SCHEME_NAMES.add("HLSH_FPS_MR");
    }

    /**
     * Does nothing if scheme name is supported, throws exception othewise.
     *
     * @param scheme blocking scheme name.
     */
    public static void schemeNameSupported(final String scheme) {
        if(!SCHEME_NAMES.contains(scheme))
            throw new UnsupportedOperationException("Scheme name \"" + scheme +"\" does not belong in available schemes.");
    }

    /**
     * Save record id pairs in path.
     *
     * @param fs a <code>FileSystem</code> reference.
     * @param outputPath output path (file).
     * @param result a blocking result.
     */
    public static void saveBlockingResult(final FileSystem fs, final Path outputPath,
                                          final HammingLSHBlocking.HammingLSHBlockingResult result)
            throws IOException {
        final FSDataOutputStream fsdos = fs.create(outputPath, true);
        fsdos.writeBytes("Frequent Pairs : " + result.getFrequentPairsCount() + "\n");
        fsdos.writeBytes("Matched Pairs : " + result.getMatchedPairsCount() + "\n");
        for (RecordIdPair pair : result.getMatchedPairs()){
            String ps = String.format("%s , %s\n", pair.aliceId, pair.bobId);
            fsdos.writeBytes(ps + "\n");
        }

        fsdos.close();
    }
}
