package gr.upatras.ceid.pprl.encoding;

import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

public class BenchmarkHashingMethods {

    private static final int N = 1024;
    private static final int K = 1;
    private static final int Q = 2;
    private static final int ITERATIONS = 4;
    private static final long BYTES_LIMIT = 128*1024 * 1024;

    public static void main(String[] args) throws UnsupportedEncodingException {
        System.out.println("Running benchmarks : ");
        System.out.println("\nBenchmarking : BloomFilter.createHashesV1()");
        long[][] millisV1 = benchmarkCreateHashesV1();
        System.out.println("\nBenchmarking : BloomFilter.createHashesV2()");
        long[][] millisV2 = benchmarkCreateHashesV2();
        System.out.println("\nBenchmarking : BloomFilter.createHashesV1() backed by dictionary.");
        long[][] millisV1backed = benchmarkCreateHashesV1MapBacked();
        System.out.println("\nBenchmarking : BloomFilter.createHashesV2() backed by dictionary.");
        long[][] millisV2backed = benchmarkCreateHashesV2MapBacked();

        for (int i = 10,j=0; j<3; i=i/2,j++) {
            System.out.format("\nFor problem size = %d bytes :\n\tv1,v2,mbv1,mb2\n",BYTES_LIMIT/i);
            for (int k=0; k<K; k++) {
                System.out.format("k=%d\t%d, %d,%d, %d\n",k,millisV1[j][k],millisV2[j][k],
                        millisV1backed[j][k],millisV2backed[j][k]);
            }
            System.out.format("\n\n");
        }
    }

    private static long[][] benchmarkCreateHashesV1() throws UnsupportedEncodingException {
        long millis[][] = new long[3][K];
        for (int i = 10,j=0; j < 3; i=i/2,j++) {
            long maxSize = BYTES_LIMIT/i;
            for(int k = 0 ; k < K; k++) {
                long millisSum = 0;
                for (int l = 0; l < ITERATIONS; l++) {
                    long total = 0;
                    long before = System.currentTimeMillis();
                    while (total < maxSize) {
                        String randomBigram =
                                UUID.randomUUID().toString().substring(0,Q-1);
                        BloomFilter.createHashesV1(randomBigram.getBytes("UTF-8"), N, k);
                        total += 2;
                    }
                    long after = System.currentTimeMillis();
                    millisSum += after - before;
                }
                millis[j][k] = millisSum / ITERATIONS;
            }
        }
        return millis;
    }

    private static long[][] benchmarkCreateHashesV2() throws UnsupportedEncodingException {
        long millis[][] = new long[3][K];
        for (int i = 10,j=0; j < 3; i=i/2,j++) {
            long maxSize = BYTES_LIMIT/i;
            for(int k = 0 ; k < K; k++) {
                long millisSum = 0;
                for (int l = 0; l < ITERATIONS; l++) {
                    long total = 0;
                    long before = System.currentTimeMillis();
                    while (total < maxSize) {
                        String randomBigram =
                                UUID.randomUUID().toString().substring(0,Q-1);
                        BloomFilter.createHashesV2(randomBigram.getBytes("UTF-8"), N, k);
                        total += 2;
                    }
                    long after = System.currentTimeMillis();
                    millisSum += after - before;
                }
                millis[j][k] = millisSum / ITERATIONS;
            }
        }
        return millis;
    }


    private static long[][] benchmarkCreateHashesV1MapBacked() throws UnsupportedEncodingException {
        Map<String,int[]> map = new TreeMap<String,int[]>();
        long millis[][] = new long[3][K];
        for (int i = 10,j=0; j < 3; i=i/2,j++) {
            long maxSize = BYTES_LIMIT/i;
            for(int k = 0 ; k < K; k++) {
                long millisSum = 0;
                for (int l = 0; l < ITERATIONS; l++) {
                    long total = 0;
                    long before = System.currentTimeMillis();
                    while (total < maxSize) {
                        String randomBigram =
                                UUID.randomUUID().toString().substring(0,Q-1);
                        if(!map.containsKey(randomBigram)) {
                            int[] hashes = BloomFilter.createHashesV1(randomBigram.getBytes("UTF-8"), N, k);
                            map.put(randomBigram,hashes);
                        }
                        total += 2;
                    }
                    long after = System.currentTimeMillis();
                    millisSum += after - before;
                }
                millis[j][k] = millisSum / ITERATIONS;
            }
        }
        return millis;
    }

    private static long[][] benchmarkCreateHashesV2MapBacked() throws UnsupportedEncodingException {
        Map<String,int[]> map = new TreeMap<String,int[]>();
        long millis[][] = new long[3][K];
        for (int i = 10,j=0; j < 3; i=i/2,j++) {
            long maxSize = BYTES_LIMIT/i;
            for(int k = 0 ; k < K; k++) {
                long millisSum = 0;
                for (int l = 0; l < ITERATIONS; l++) {
                    long total = 0;
                    long before = System.currentTimeMillis();
                    while (total < maxSize) {
                        String randomBigram =
                                UUID.randomUUID().toString().substring(0,Q-1);
                        if(!map.containsKey(randomBigram)) {
                            int[] hashes = BloomFilter.createHashesV2(randomBigram.getBytes("UTF-8"), N, k);
                            map.put(randomBigram,hashes);
                        }
                        total += 2;
                    }
                    long after = System.currentTimeMillis();
                    millisSum += after - before;
                }
                millis[j][k] = millisSum / ITERATIONS;
            }
        }
        return millis;
    }
}
