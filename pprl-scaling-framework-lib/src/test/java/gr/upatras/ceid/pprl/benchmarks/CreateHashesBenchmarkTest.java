package gr.upatras.ceid.pprl.benchmarks;

import gr.upatras.ceid.pprl.encoding.BloomFilter;
import gr.upatras.ceid.pprl.qgram.QGramUtil;
import org.apache.avro.Schema;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class CreateHashesBenchmarkTest {
    private static Logger LOG = LoggerFactory.getLogger(CreateHashesBenchmarkTest.class);
    private static final int N = 1024;
    private static final int K = 30;
    private static final int minQ = 2;
    private static final int maxQ = 4;
    private static final int ITERATIONS = 20;
    private static float FILL_FACTOR = 0.75f;


    private static final char[] CHARSET_AZ_09_ = "_ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789".toCharArray();
    private static final Random random = new SecureRandom();

    private static final Mac HMAC_MD5;
    private static final Mac HMAC_SHA1;
    private static final String SECRET_KEY = "MYZIKRETQI";
    private static DescriptiveStatistics statsTime =  new DescriptiveStatistics();
    private static DescriptiveStatistics statsCollisions =  new DescriptiveStatistics();
    private static DescriptiveStatistics statsDictionary =  new DescriptiveStatistics();

    private static final long[] SIZES = {10*1024,20*1024,50*1024}; // 10 kbytes,20 kbytes,50 kbytes

    long[][][] millisV3 = new long [SIZES.length][maxQ-minQ + 1][K];
    long[][][] millisV3backed = new long [SIZES.length][maxQ-minQ + 1][K];
    long[][][] mapSizeV3 = new long [SIZES.length][maxQ-minQ + 1][K];
    long[][][] collisionsV3 = new long [SIZES.length][maxQ-minQ + 1][K];

    static {
        Mac tmp;
        Mac tmp1;
        try {
            tmp = Mac.getInstance("HmacMD5");
            tmp.init(new SecretKeySpec(SECRET_KEY.getBytes(), "HmacMD5"));
            tmp1 = Mac.getInstance("HmacSHA1");
            tmp1.init(new SecretKeySpec(SECRET_KEY.getBytes(), "HmacSHA1"));
        } catch (NoSuchAlgorithmException e) {
            tmp = null;
            tmp1 = null;
        } catch (InvalidKeyException e) {
            tmp = null;
            tmp1 = null;
        }
        HMAC_MD5 = tmp;
        HMAC_SHA1 = tmp1;
    }

    @Test
    public void test0() throws IOException {
        LOG.info("Running benchmarks (iterations : {}):",ITERATIONS);
        benchmarkCreateHashesV3();
        benchmarkCreateHashesV3MapBacked();

        FileSystem fs = FileSystem.getLocal(new Configuration());
        LOG.info("\nSaving benchmarks to CSV files.");
        final String header = "k,createHashesV3,MBcreateHashesV3,collisions,dictionary_size\n";
        for (int j = 0 ; j < SIZES.length; j++) {
            for (int q = minQ, i = 0; q <= maxQ; q++, i++) {
                final String fileName = String.format("benchmark_%d_%d.csv",j,q);
                FSDataOutputStream fsdos = fs.create(new Path("data/benchmarks", fileName));
                fsdos.writeBytes(header);
                for (int k=0; k<K; k++)
                    fsdos.writeBytes(String.format("%d,%d,%d,%d,%d\n",
                            k + 1,
                            millisV3[j][i][k],
                            millisV3backed[j][i][k],
                            collisionsV3[j][i][k],
                            mapSizeV3[j][i][k]));
                fsdos.close();
            }
        }
    }

    @Test
    public void test1() throws IOException {
        LOG.info("Running 1MB benchmark:");
        OneMBBenchmark();
        LOG.info("Running 1 million names benchmark:");
        OneMNamesBenchmark();
    }

    private static String randomQgram(final int q) {
        final char[] chars = new char[q];
        for (int i = 0; i < q; i++) {
            chars[i] = CHARSET_AZ_09_[random.nextInt(CHARSET_AZ_09_.length)];
        }
        return new String(chars);
    }

    private void benchmarkCreateHashesV3() throws UnsupportedEncodingException {
        long totalBytesRead = 0;
        long start = System.currentTimeMillis();
        for (int j = 0 ; j < SIZES.length; j++) {
            long size = SIZES[j];
            for(int q = minQ,i = 0 ; q <= maxQ ; q++,i++) {
                for(int k = 1 ; k <= K; k++) {
                    statsTime.clear();
                    for (int it = 0; it < ITERATIONS / 4; it++) {
                        BloomFilter.createHashesV3(randomQgram(q).getBytes("UTF-8"), N, k, HMAC_MD5, HMAC_SHA1);
                    }
                    LOG.info("Benchmarking : BloomFilter.createHashesV3() ({})",
                            String.format("input-size : %d, K : %d, Q : %d",size,k,q));
                    for (int it = 0; it < ITERATIONS; it++) {
                        long bytesRead = 0;
                        long before = System.currentTimeMillis();
                        while (bytesRead < size) {
                            byte[] b = randomQgram(q).getBytes("UTF-8");
                            BloomFilter.createHashesV3(b, N, k, HMAC_MD5, HMAC_SHA1);
                            bytesRead += b.length;
                        }
                        long after = System.currentTimeMillis();
                        long diff = after - before;
                        statsTime.addValue(diff);
                        totalBytesRead += bytesRead;
                    }
                    millisV3[j][i][k-1] = (long) getCorrectMean(statsTime);
                }
            }
        }
        long end = System.currentTimeMillis();
        long totalTime = (end-start)/1000;
        LOG.info("Benchmarking : BloomFilter.createHashesV3() ({})",
                String.format("totalBytes : %d , totalTime : %d",totalBytesRead,totalTime));
    }



    private void benchmarkCreateHashesV3MapBacked() throws UnsupportedEncodingException {
        long totalBytesRead = 0;
        long start = System.currentTimeMillis();

        for (int j = 0 ; j < SIZES.length; j++) {
            long size = SIZES[j];
            for(int q = minQ,i = 0 ; q <= maxQ ; q++,i++) {
                for(int k = 1 ; k <= K; k++) {
                    statsTime.clear();
                    statsCollisions.clear();
                    statsDictionary.clear();
                    final int capacity = (int) ((int)Math.pow(CHARSET_AZ_09_.length,q) / FILL_FACTOR + 1);
                    final Map<String,int[]> map = new HashMap<String,int[]>(capacity, FILL_FACTOR);
                    for (int it = 0; it < ITERATIONS / 4; it++) {
                        final String qgram = randomQgram(q);
                        if(!map.containsKey(qgram)) {
                            map.put(qgram,
                                    BloomFilter.createHashesV3(randomQgram(q).getBytes("UTF-8"),
                                            N, k, HMAC_MD5, HMAC_SHA1));
                        }
                    }
                    map.clear();
                    LOG.info("Benchmarking : BloomFilter.createHashesV3() backed by dictionary ({})",
                            String.format("input-size : %d, K : %d, Q : %d", size, k, q));
                    for (int it = 0; it < ITERATIONS; it++) {
                        map.clear();
                        long bytesRead = 0;
                        long collisions = 0;
                        long before = System.currentTimeMillis();
                        while (bytesRead < size) {
                            final String qgram = randomQgram(q);
                            byte[] b = qgram.getBytes("UTF-8");
                            if(!map.containsKey(qgram)) {
                                map.put(qgram,
                                        BloomFilter.createHashesV3(b, N, k, HMAC_MD5, HMAC_SHA1));
                            } else collisions++;
                            bytesRead += b.length;
                        }
                        long after = System.currentTimeMillis();
                        long diff = after - before;
                        statsDictionary.addValue(map.keySet().size());
                        statsCollisions.addValue(collisions);
                        statsTime.addValue(diff);
                        totalBytesRead += bytesRead;
                    }
                    millisV3backed[j][i][k-1] = (long) getCorrectMean(statsTime);
                    mapSizeV3[j][i][k-1] = (long) getCorrectMean(statsDictionary);
                    collisionsV3[j][i][k-1] = (long) getCorrectMean(statsCollisions);
                }
            }
        }
        long end = System.currentTimeMillis();
        long totalTime = (end-start)/1000;
        LOG.info("Benchmarking : BloomFilter.createHashesV1() backed by dictionary ({}).",
                String.format("totalBytes : %d , totalTime : %d",totalBytesRead,totalTime));
    }

    private void OneMBBenchmark() throws UnsupportedEncodingException {

        long maxBytes = 1024*1024; //1MB
        int K = 30;
        {
            for(int q = minQ; q<= maxQ; q++) {
                long bytesRead = 0;
                long start = System.currentTimeMillis();
                while (bytesRead < maxBytes) {
                    byte[] b = randomQgram(q).getBytes("UTF-8");
                    BloomFilter.createHashesV3(b, N, K, HMAC_MD5, HMAC_SHA1);
                    bytesRead += b.length;
                }
                long end = System.currentTimeMillis();
                long millis = end - start;
                LOG.info("q = " + q + " Time took " + millis + " milliseconds. Read " + bytesRead + " bytes.");
            }
        }

        {
            for (int q = minQ; q <= maxQ; q++) {
                long bytesRead = 0;
                long collisions = 0;
                final int capacity = (int) ((int)Math.pow(CHARSET_AZ_09_.length,q) / FILL_FACTOR + 1);
                final Map<String,int[]> map = new HashMap<String,int[]>(capacity, FILL_FACTOR);
                long start = System.currentTimeMillis();
                while (bytesRead < maxBytes) {
                    String qGram = randomQgram(q);
                    byte[] b = randomQgram(q).getBytes("UTF-8");
                    if (!map.containsKey(qGram)) {
                        map.put(qGram, BloomFilter.createHashesV3(b, N, K, HMAC_MD5, HMAC_SHA1));

                    } else collisions++;
                    bytesRead += b.length;
                }
                LOG.info("Map backed createHashesV3 : ");
                long end = System.currentTimeMillis();
                long millis = end - start;
                LOG.info("q = " + q + " Time took " + millis + " milliseconds. Read " + bytesRead + " bytes." +
                        " Collisions :" + collisions + " Dictionary size : " + map.keySet().size() + " keys");
            }
        }
    }


    private void OneMNamesBenchmark() throws IOException {
        int K = 30;
        {
            for(int q = minQ; q<= maxQ; q++) {
                long bytesRead = 0;
                FileSystem fs = FileSystem.get(new Configuration());
                FSDataInputStream fsdis = fs.open(new Path("data", "names.txt"));
                BufferedReader reader = new BufferedReader(new InputStreamReader(fsdis));
                String line;
                long start = System.currentTimeMillis();
                do {
                    line = reader.readLine();
                    if(line == null) break;
                    for(String qgram  : QGramUtil.generateQGrams(line, Schema.Type.STRING,q)) {
                        byte[] b = qgram.getBytes("UTF-8");
                        BloomFilter.createHashesV3(b, N, K, HMAC_MD5, HMAC_SHA1);
                        bytesRead += b.length;
                    }
                }while (true);
                reader.close();
                long end = System.currentTimeMillis();
                long millis = end - start;
                LOG.info("q = " + q + "createHashesV3 : Time took " + millis + " milliseconds. Read " + bytesRead + " bytes.");
            }
        }

        {
            for (int q = minQ; q <= maxQ; q++) {
                long bytesRead = 0;
                long collisions = 0;
                final int capacity = (int) ((int)Math.pow(CHARSET_AZ_09_.length,q) / FILL_FACTOR + 1);
                final Map<String,int[]> map = new HashMap<String,int[]>(capacity, FILL_FACTOR);
                FileSystem fs = FileSystem.get(new Configuration());
                FSDataInputStream fsdis = fs.open(new Path("data/benchmarks", "names.txt"));
                BufferedReader reader = new BufferedReader(new InputStreamReader(fsdis));
                String line;
                long start = System.currentTimeMillis();
                do {
                    line = reader.readLine();
                    if(line == null) break;
                    for(String qgram  : QGramUtil.generateQGrams(line, Schema.Type.STRING,q)) {
                        byte[] b = qgram.getBytes("UTF-8");
                        if(!map.containsKey(qgram)) {
                            map.put(qgram,BloomFilter.createHashesV3(b, N, K, HMAC_MD5, HMAC_SHA1));
                        } else collisions++;
                        bytesRead += b.length;
                    }
                }while (true);
                reader.close();
                long end = System.currentTimeMillis();
                long millis = end - start;
                LOG.info("q = " + q + "createHashesV3 : Time took " + millis + " milliseconds. Read " + bytesRead + " bytes." +
                        " Collisions :" + collisions + " Dictionary size : " + map.keySet().size() + " keys");
            }
        }
    }


    public double getCorrectMean(final DescriptiveStatistics stats) {
        double q1 = stats.getPercentile(25);
        double q3 = stats.getPercentile(75);
        assert q1 <= q3;
        double iqr = q3 - q1;
        double upper = q3 + 1.5*iqr;
        double lower = q1 - 1.5*iqr;
        final DescriptiveStatistics noOutliersStats = new DescriptiveStatistics();
        for (double v : stats.getValues())
            if(v >= lower && v <= upper) noOutliersStats.addValue(v);
        return noOutliersStats.getMean();
    }
}
