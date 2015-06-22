package gr.upatras.ceid.pprl.pprlsf.test;

import java.util.BitSet;
import java.util.Random;

public class BitSets {

    public static BitSet[] randomBitSets(int N, int L) {

        BitSet[] bitSets = new BitSet[N];
        final int NUM_BYTES = (int) Math.ceil((double) L/Byte.SIZE);
        final Random rnd = new Random();

        for (int i = 0; i < N; i++) {
            byte[] randomBytes = new byte[NUM_BYTES];
            rnd.nextBytes(randomBytes);
            bitSets[i] = BitSet.valueOf(randomBytes);

        }
        return bitSets;
    }

    public static void print(BitSet bitset,int L) {
        StringBuilder sb = new StringBuilder("");
        for (int j = 0; j < L; j++) {
            sb.append(String.format("%3s", bitset.get(j) ? "1" : "0"));
        }
        System.out.println(sb.toString());
    }

    public static void printAll(BitSet[] bitsets,int L) {
        StringBuilder sb = new StringBuilder("");
        for (int i = 0; i < L; i++) {
            sb.append(String.format("%3s",i));
        }
        System.out.printf("%-4s%s%n","",sb.toString());
        for (int i = 0; i < bitsets.length; i++) {
            sb = new StringBuilder("");
            for (int j = 0; j < L; j++) {
                sb.append(String.format("%3s", bitsets[i].get(j) ? "1" : "0"));
            }
            System.out.printf("%-4s%s%n", i, sb.toString());
        }
    }
}
