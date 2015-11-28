package gr.upatras.ceid.pprl.encoding;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.security.SecureRandom;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

public class BenchmarkHashingMethods {

    private static final int N = 1024;
    private static final int K = 30;
    private static final int Q = 2;
    private static final int ITERATIONS = 4;
    private static final long BYTES_LIMIT = 1024*1024; // 1MB limit for each iteration for each K
    private static final int MAX_PROGRESS = 3*K*ITERATIONS;
    private static final char[] CHARSET_AZ_09_ = "_ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789".toCharArray();
    private static final Random random = new SecureRandom();

    public static void main(String[] args) throws IOException {
        System.out.format("Running benchmarks (ITERATIONS=%d,N=%d,K={1:%d},Q=%d)\n", ITERATIONS, N, K, Q);
        long[][] millisV1 = benchmarkCreateHashesV1();
        long[][] millisV2 = benchmarkCreateHashesV2();
        long[][] millisV1backed = benchmarkCreateHashesV1MapBacked();
        long[][] millisV2backed = benchmarkCreateHashesV2MapBacked();

        System.out.println("\nSaving benchmarks to CSV files.");
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
            System.out.println("Benchmark timings saved at " + file.getAbsolutePath());
            writer.close();
        }

        hdfsBlkSizeBenchmark();
    }

    private static String randomQgram() {
        final char[] chars = new char[Q];
        for (int i = 0; i < Q; i++) {
            chars[i] = CHARSET_AZ_09_[random.nextInt(CHARSET_AZ_09_.length)];
        }
        return new String(chars);
    }


    private static long[][] benchmarkCreateHashesV1() throws UnsupportedEncodingException {
        for (int i = 0; i < 3; i++) BloomFilter.createHashesV1(randomQgram().getBytes("UTF-8"), N, 2);

        int progress = 0;
        long totalBytesRead = 0;

        long millis[][] = new long[3][K];
        long start = System.currentTimeMillis();
        for (int i = 10,j=0; j < 3; i=i/2,j++) {
            long maxSize = BYTES_LIMIT/i;
            for(int k = 0 ; k < K; k++) {
                long millisSum = 0;
                for (int l = 0; l < ITERATIONS; l++) {
                    long bytesRead = 0;
                    long before = System.currentTimeMillis();
                    while (bytesRead < maxSize) {
                        BloomFilter.createHashesV1(randomQgram().getBytes("UTF-8"), N, k);
                        bytesRead += Q;
                    }
                    long after = System.currentTimeMillis();
                    millisSum += after - before;
                    System.out.print("\rBenchmarking : BloomFilter.createHashesV1() " + (100*progress)/(MAX_PROGRESS) + "%.");
                    progress++;totalBytesRead += bytesRead;
                }
                millis[j][k] = millisSum / ITERATIONS;
            }
        }
        long end = System.currentTimeMillis();
        long totalTime = (end-start)/1000;
        System.out.println("\rBenchmarking : BloomFilter.createHashesV1() 100%.");
        System.out.println("Total bytes Read = " + totalBytesRead + " Total time " + totalTime + " seconds.");
        return millis;
    }

    private static long[][] benchmarkCreateHashesV2() throws UnsupportedEncodingException {
        for (int i = 0; i < 3; i++) BloomFilter.createHashesV2(randomQgram().getBytes("UTF-8"), N, 2);

        int progress = 0;
        long totalBytesRead = 0;

        long millis[][] = new long[3][K];
        long start = System.currentTimeMillis();
        for (int i = 10,j=0; j < 3; i=i/2,j++) {
            long maxSize = BYTES_LIMIT/i;
            for(int k = 0 ; k < K; k++) {
                long millisSum = 0;
                for (int l = 0; l < ITERATIONS; l++) {
                    long bytesRead = 0;
                    long before = System.currentTimeMillis();
                    while (bytesRead < maxSize) {
                        BloomFilter.createHashesV2(randomQgram().getBytes("UTF-8"), N, k);
                        bytesRead += Q;
                    }
                    long after = System.currentTimeMillis();
                    millisSum += after - before;
                    System.out.print("\rBenchmarking : BloomFilter.createHashesV2() " + (100*progress)/(MAX_PROGRESS) + "%.");
                    progress++;totalBytesRead += bytesRead;
                }
                millis[j][k] = millisSum / ITERATIONS;
            }
        }
        long end = System.currentTimeMillis();
        long totalTime = (end-start)/1000;
        System.out.println("\rBenchmarking : BloomFilter.createHashesV2() 100%.");
        System.out.format("Read %d bytes , Time %d seconds\n",totalBytesRead,totalTime);
        return millis;
    }


    private static long[][] benchmarkCreateHashesV1MapBacked() throws UnsupportedEncodingException {

        for (int i = 0; i < 3; i++) BloomFilter.createHashesV1(randomQgram().getBytes("UTF-8"), N, 2);

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
                long millisSum = 0;
                for (int l = 0; l < ITERATIONS; l++) {
                    long bytesRead = 0;
                    long before = System.currentTimeMillis();
                    while (bytesRead < maxSize) {
                        final String qgram = randomQgram();
                        if(!map.containsKey(qgram)) {
                            map.put(qgram,BloomFilter.createHashesV1(qgram.getBytes("UTF-8"), N, k));
                            totalBytesHashed += Q;
                        }
                        bytesRead += Q;
                    }
                    long after = System.currentTimeMillis();
                    millisSum += after - before;
                    System.out.print("\rBenchmarking : BloomFilter.createHashesV1() backed by dictionary " +
                            (100*progress)/(MAX_PROGRESS) + "%.");
                    progress++;totalBytesRead += bytesRead;
                }
                millis[j][k] = millisSum / ITERATIONS;
            }
        }
        long end = System.currentTimeMillis();
        long totalTime = (end-start)/1000;
        System.out.println("\rBenchmarking : BloomFilter.createHashesV1() backed by dictionary 100%.");
        System.out.format("Read %d bytes, Hashed %d bytes, Time %d seconds\n", totalBytesRead, totalBytesHashed, totalTime);
        System.out.println("Final dictionary keys size : " +  map.keySet().size() );
        return millis;
    }

    private static long[][] benchmarkCreateHashesV2MapBacked() throws UnsupportedEncodingException {

        for (int i = 0; i < 3; i++) BloomFilter.createHashesV2(randomQgram().getBytes("UTF-8"), N, 2);

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
                long millisSum = 0;
                for (int l = 0; l < ITERATIONS; l++) {
                    long bytesRead = 0;
                    long before = System.currentTimeMillis();
                    while (bytesRead < maxSize) {
                        final String qgram = randomQgram();
                        if(!map.containsKey(qgram)) {
                            map.put(qgram,BloomFilter.createHashesV2(qgram.getBytes("UTF-8"), N, k));
                            totalBytesHashed += Q;
                        }
                        bytesRead += Q;
                    }
                    long after = System.currentTimeMillis();
                    millisSum += after - before;
                    System.out.print("\rBenchmarking : BloomFilter.createHashesV2() backed by dictionary " +
                            (100*progress)/(MAX_PROGRESS) + "%.");
                    progress++;totalBytesRead += bytesRead;
                }
                millis[j][k] = millisSum / ITERATIONS;
            }
        }
        long end = System.currentTimeMillis();
        long totalTime = (end-start)/1000;
        System.out.println("\rBenchmarking : BloomFilter.createHashesV2() backed by dictionary 100%.");
        System.out.format("Read %d bytes, Hashed %d bytes, Time %d seconds\n", totalBytesRead, totalBytesHashed, totalTime);
        System.out.println("Final dictionary keys size : " +  map.keySet().size() );
        return millis;
    }

    private static void hdfsBlkSizeBenchmark() throws UnsupportedEncodingException {

        long maxBytes = 64*BYTES_LIMIT; //64 MB

        BloomFilter.createHashesV1(randomQgram().getBytes("UTF-8"), N, K);
        BloomFilter.createHashesV1(randomQgram().getBytes("UTF-8"), N, K);
        {
            long bytesRead = 0;
            long start = System.currentTimeMillis();
            while (bytesRead < maxBytes) {
                BloomFilter.createHashesV1(randomQgram().getBytes("UTF-8"), N, K);
                bytesRead += Q;
            }
            long end = System.currentTimeMillis();
            long millis = end - start;
            System.out.print("createHashesV1 : ");
            System.out.println("Time took " + millis/1000 + " seconds. Read " + bytesRead + " bytes.");
        }
        {
            long bytesRead = 0;
            long start = System.currentTimeMillis();
            while (bytesRead < maxBytes) {
                BloomFilter.createHashesV2(randomQgram().getBytes("UTF-8"), N, K);
                bytesRead += Q;
            }
            long end = System.currentTimeMillis();
            long millis = end - start;
            System.out.print("createHashesV2 : ");
            System.out.println("Time took " + millis/1000 + " seconds. Read " + bytesRead + " bytes.");
        }
        {
            long bytesRead = 0;
            long bytesHashed = 0;
            long start = System.currentTimeMillis();
            Map<String, int[]> map = new TreeMap<String,int[]>();
            while (bytesRead < maxBytes) {
                String qGram = randomQgram();
                if(!map.containsKey(qGram)) {
                    map.put(qGram,BloomFilter.createHashesV1(qGram.getBytes("UTF"), N, K));
                    bytesHashed += Q;
                }
                bytesRead += Q;
            }
            System.out.print("Map backed createHashesV1 : ");
            long end = System.currentTimeMillis();
            long millis = end - start;
            System.out.println("Time took " + millis/1000 + " seconds. Read " + bytesRead + " bytes." +
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
                    map.put(qGram,BloomFilter.createHashesV2(qGram.getBytes("UTF"), N, K));
                    bytesHashed += Q;
                }
                bytesRead += Q;
            }
            long end = System.currentTimeMillis();
            long millis = end - start;
            System.out.print("Map backed createHashesV2 : ");
            System.out.println("Time took " + millis / 1000 + " seconds. Read " + bytesRead + " bytes." +
                    " Hashed " + bytesHashed + " bytes");
        }
    }
}