package gr.upatras.ceid.pprl.encoding.test;

import gr.upatras.ceid.pprl.encoding.BloomFilter;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

public class BloomFilterHashingBenchmarkTest {
    private static Logger LOG = LoggerFactory.getLogger(BloomFilterHashingBenchmarkTest.class);
    private static final int N = 1024;
    private static final int K = 30;
    private static final int Q = 2;
    private static final int ITERATIONS = 4;
    private static final long BYTES_LIMIT = 100*1024; // 100K limit for each iteration for each K
    private static final int MAX_PROGRESS = 3*K*ITERATIONS;
    private static final char[] CHARSET_AZ_09_ = "_ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789".toCharArray();
    private static final Random random = new SecureRandom();

    private static final Mac HMAC_MD5;
    private static final Mac HMAC_SHA1;
    private static final String SECRET_KEY = "MYZIKRETQI";
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
        LOG.info(String.format("Running benchmarks (ITERATIONS=%d,N=%d,K={1:%d},Q=%d)\n", ITERATIONS, N, K, Q));
        long[][] millisV1 = benchmarkCreateHashesV1();
        long[][] millisV2 = benchmarkCreateHashesV2();
        long[][] millisV1backed = benchmarkCreateHashesV1MapBacked();
        long[][] millisV2backed = benchmarkCreateHashesV2MapBacked();

        LOG.info("\nSaving benchmarks to CSV files.");
        final String header = "k,createHashesV1,createHashesV2,MBcreateHashesV1,MBcreateHashesV2";
        for (int i = 10,j=0; j<3; i=i/2,j++) {
            final String fileName = String.format("benchmark_scale_%d.csv",j+1);
            final File file = new File(fileName);
            file.createNewFile();
            final PrintWriter writer = new PrintWriter(file);
            writer.append(header).append("\n");
            for (int k=0; k<K; k++)
                writer.append(
                        String.format("%d,%d,%d,%d,%d",
                                k+1, millisV1[j][k], millisV2[j][k], millisV1backed[j][k], millisV2backed[j][k]))
                        .append("\n");
            LOG.info("Iteration input size : " + BYTES_LIMIT/i + " bytes." +
                    "Benchmark timings saved at " + file.getAbsolutePath());
            writer.close();
        }

        //LOG.info("Running 64MB benchmark:");
        //hdfsBlkSizeBenchmark();
    }


    private static String randomQgram() {
        final char[] chars = new char[Q];
        for (int i = 0; i < Q; i++) {
            chars[i] = CHARSET_AZ_09_[random.nextInt(CHARSET_AZ_09_.length)];
        }
        return new String(chars);
    }


    private static long[][] benchmarkCreateHashesV1() throws UnsupportedEncodingException {
        int progress = 0;
        long totalBytesRead = 0;

        long millis[][] = new long[3][K];
        long start = System.currentTimeMillis();
        for (int i = 10,j=0; j < 3; i=i/2,j++) {
            long maxSize = BYTES_LIMIT/i;
            for(int k = 0 ; k < K; k++) {
                for (int l = 0; l < ITERATIONS; l++)
                    BloomFilter.createHashesV1(randomQgram().getBytes("UTF-8"), N, k, HMAC_MD5, HMAC_SHA1);
                long millisSum = 0;
                for (int l = 0; l < ITERATIONS; l++) {
                    long bytesRead = 0;
                    long before = System.currentTimeMillis();
                    while (bytesRead < maxSize) {
                        BloomFilter.createHashesV1(randomQgram().getBytes("UTF-8"), N, k, HMAC_MD5, HMAC_SHA1);
                        bytesRead += Q;
                    }
                    long after = System.currentTimeMillis();
                    millisSum += after - before;
                    LOG.info("\rBenchmarking : BloomFilter.createHashesV1() " + (100 * progress) / (MAX_PROGRESS) + "%.");
                    progress++;totalBytesRead += bytesRead;
                }
                millis[j][k] = millisSum / ITERATIONS;
            }
        }
        long end = System.currentTimeMillis();
        long totalTime = (end-start)/1000;
        LOG.info("\rBenchmarking : BloomFilter.createHashesV1() 100%.");
        LOG.info("Total bytes Read = " + totalBytesRead + " Total time " + totalTime + " seconds.");
        return millis;
    }

    private static long[][] benchmarkCreateHashesV2() throws UnsupportedEncodingException {
        int progress = 0;
        long totalBytesRead = 0;

        long millis[][] = new long[3][K];
        long start = System.currentTimeMillis();
        for (int i = 10,j=0; j < 3; i=i/2,j++) {
            long maxSize = BYTES_LIMIT/i;
            for(int k = 0 ; k < K; k++) {
                for (int l = 0; l < ITERATIONS; l++)
                    BloomFilter.createHashesV2(randomQgram().getBytes("UTF-8"), N, k, HMAC_MD5);
                long millisSum = 0;
                for (int l = 0; l < ITERATIONS; l++) {
                    long bytesRead = 0;
                    long before = System.currentTimeMillis();
                    while (bytesRead < maxSize) {
                        BloomFilter.createHashesV2(randomQgram().getBytes("UTF-8"), N, k, HMAC_MD5);
                        bytesRead += Q;
                    }
                    long after = System.currentTimeMillis();
                    millisSum += after - before;
                    LOG.info("\rBenchmarking : BloomFilter.createHashesV2() " + (100 * progress) / (MAX_PROGRESS) + "%.");
                    progress++;totalBytesRead += bytesRead;
                }
                millis[j][k] = millisSum / ITERATIONS;
            }
        }
        long end = System.currentTimeMillis();
        long totalTime = (end-start)/1000;
        LOG.info("\rBenchmarking : BloomFilter.createHashesV2() 100%.");
        LOG.info(String.format("Read %d bytes , Time %d seconds\n",totalBytesRead,totalTime));
        return millis;
    }


    private static long[][] benchmarkCreateHashesV1MapBacked() throws UnsupportedEncodingException {
        Map<String,int[]> map = new TreeMap<String,int[]>();
        int progress = 0;
        long totalBytesRead = 0;
        long totalBytesHashed = 0;

        long millis[][] = new long[3][K];
        long start = System.currentTimeMillis();
        for (int i = 10,j=0; j < 3; i=i/2,j++) {
            long maxSize = BYTES_LIMIT/i;
            for(int k = 0 ; k < K; k++) {
                map.clear();
                for (int l = 0; l < ITERATIONS; l++)
                    BloomFilter.createHashesV1(randomQgram().getBytes("UTF-8"), N, l, HMAC_MD5, HMAC_SHA1);
                long millisSum = 0;
                for (int l = 0; l < ITERATIONS; l++) {
                    long bytesRead = 0;
                    long before = System.currentTimeMillis();
                    while (bytesRead < maxSize) {
                        final String qgram = randomQgram();
                        if(!map.containsKey(qgram)) {
                            map.put(qgram,BloomFilter.createHashesV1(qgram.getBytes("UTF-8"), N, k, HMAC_MD5, HMAC_SHA1));
                            totalBytesHashed += Q;
                        }
                        bytesRead += Q;
                    }
                    long after = System.currentTimeMillis();
                    millisSum += after - before;
                    LOG.info("\rBenchmarking : BloomFilter.createHashesV1() backed by dictionary " +
                            (100 * progress) / (MAX_PROGRESS) + "%.");
                    progress++;totalBytesRead += bytesRead;
                }
                millis[j][k] = millisSum / ITERATIONS;
            }
        }
        long end = System.currentTimeMillis();
        long totalTime = (end-start)/1000;
        LOG.info("\rBenchmarking : BloomFilter.createHashesV1() backed by dictionary 100%.");
        LOG.info("Read %d bytes, Hashed %d bytes, Time %d seconds\n", totalBytesRead, totalBytesHashed, totalTime);
        LOG.info("Final dictionary keys size : " + map.keySet().size());
        return millis;
    }

    private static long[][] benchmarkCreateHashesV2MapBacked() throws UnsupportedEncodingException {

        Map<String,int[]> map = new TreeMap<String,int[]>();

        int progress = 0;
        long totalBytesRead = 0;
        long totalBytesHashed = 0;

        long millis[][] = new long[3][K];
        long start = System.currentTimeMillis();
        for (int i = 10,j=0; j < 3; i=i/2,j++) {
            long maxSize = BYTES_LIMIT/i;
            for(int k = 0 ; k < K; k++) {
                map.clear();
                for (int l = 0; l < ITERATIONS; l++)
                            BloomFilter.createHashesV2(randomQgram().getBytes("UTF-8"), N, k, HMAC_MD5);
                long millisSum = 0;
                for (int l = 0; l < ITERATIONS; l++) {
                    long bytesRead = 0;
                    long before = System.currentTimeMillis();
                    while (bytesRead < maxSize) {
                        final String qgram = randomQgram();
                        if(!map.containsKey(qgram)) {
                            map.put(qgram,BloomFilter.createHashesV2(qgram.getBytes("UTF-8"), N, k, HMAC_MD5));
                            totalBytesHashed += Q;
                        }
                        bytesRead += Q;
                    }
                    long after = System.currentTimeMillis();
                    millisSum += after - before;
                    LOG.info("\rBenchmarking : BloomFilter.createHashesV2() backed by dictionary " +
                            (100*progress)/(MAX_PROGRESS) + "%.");
                    progress++;totalBytesRead += bytesRead;
                }
                millis[j][k] = millisSum / ITERATIONS;
            }
        }
        long end = System.currentTimeMillis();
        long totalTime = (end-start)/1000;
        LOG.info("\rBenchmarking : BloomFilter.createHashesV2() backed by dictionary 100%.");
        LOG.info("Read %d bytes, Hashed %d bytes, Time %d seconds\n", totalBytesRead, totalBytesHashed, totalTime);
        LOG.info("Final dictionary keys size : " +  map.keySet().size() );
        return millis;
    }

    private static void hdfsBlkSizeBenchmark() throws UnsupportedEncodingException {

        long maxBytes = 64*BYTES_LIMIT; //64 MB

        {
            long bytesRead = 0;
            long start = System.currentTimeMillis();
            while (bytesRead < maxBytes) {
                BloomFilter.createHashesV1(randomQgram().getBytes("UTF-8"), N, K, HMAC_MD5, HMAC_SHA1);
                bytesRead += Q;
            }
            long end = System.currentTimeMillis();
            long millis = end - start;
            LOG.info("createHashesV1 : ");
            LOG.info("Time took " + millis/1000 + " seconds. Read " + bytesRead + " bytes.");
        }
        {
            long bytesRead = 0;
            long start = System.currentTimeMillis();
            while (bytesRead < maxBytes) {
                BloomFilter.createHashesV2(randomQgram().getBytes("UTF-8"), N, K, HMAC_MD5);
                bytesRead += Q;
            }
            long end = System.currentTimeMillis();
            long millis = end - start;
            LOG.info("createHashesV2 : ");
            LOG.info("Time took " + millis/1000 + " seconds. Read " + bytesRead + " bytes.");
        }
        {
            long bytesRead = 0;
            long bytesHashed = 0;
            Map<String, int[]> map = new TreeMap<String,int[]>();
            long start = System.currentTimeMillis();
            while (bytesRead < maxBytes) {
                String qGram = randomQgram();
                if(!map.containsKey(qGram)) {
                    map.put(qGram,BloomFilter.createHashesV1(qGram.getBytes("UTF"), N, K, HMAC_MD5, HMAC_SHA1));
                    bytesHashed += Q;
                }
                bytesRead += Q;
            }
            LOG.info("Map backed createHashesV1 : ");
            long end = System.currentTimeMillis();
            long millis = end - start;
            LOG.info("Time took " + millis/1000 + " seconds. Read " + bytesRead + " bytes." +
                    " Hashed " + bytesHashed + " bytes");
        }
        {
            long bytesRead = 0;
            long bytesHashed = 0;
            long start = System.currentTimeMillis();
            Map<String, int[]> map = new TreeMap<String,int[]>();
            while (bytesRead < maxBytes) {
                String qGram = randomQgram();
                if(!map.containsKey(qGram)) {
                    map.put(qGram,BloomFilter.createHashesV2(qGram.getBytes("UTF"), N, K, HMAC_MD5));
                    bytesHashed += Q;
                }
                bytesRead += Q;
            }
            long end = System.currentTimeMillis();
            long millis = end - start;
            LOG.info("Map backed createHashesV2 : ");
            LOG.info("Time took " + millis / 1000 + " seconds. Read " + bytesRead + " bytes." +
                    " Hashed " + bytesHashed + " bytes");
        }
    }
}
