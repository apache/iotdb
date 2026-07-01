package org.apache.iotdb.tsfile.encoding;

import org.junit.Test;

import java.util.Arrays;

public class SubcolumnPruneNewBenchmarkTest {

    private static int[] syntheticData(int size, int seed) {
        int[] data = new int[size];
        int state = seed;
        for (int i = 0; i < size; i++) {
            state = state * 1103515245 + 12345;
            data[i] = (state >>> 16) & 0x7FFF;
            if (i % 17 == 0) {
                data[i] = data[Math.max(0, i - 1)];
            }
        }
        return data;
    }

    @Test
    public void benchmarkSubcolumnPruneNew() {
        int blockSize = 512;
        int repeatTime = 200;
        int[] data = syntheticData(512 * 200, 99);
        byte[] encoded = new byte[data.length * 8];

        int encodedLen = SubcolumnPruneNewTest.Encoder(data, blockSize, encoded);

        long encodeTotal = 0;
        for (int i = 0; i < repeatTime; i++) {
            long start = System.nanoTime();
            encodedLen = SubcolumnPruneNewTest.Encoder(data, blockSize, encoded);
            encodeTotal += System.nanoTime() - start;
        }

        long decodeTotal = 0;
        byte[] encodedCopy = Arrays.copyOf(encoded, encodedLen);
        for (int i = 0; i < repeatTime; i++) {
            long start = System.nanoTime();
            SubcolumnPruneNewTest.Decoder(encodedCopy);
            decodeTotal += System.nanoTime() - start;
        }

        double avgEncodeMs = encodeTotal / (double) repeatTime / 1_000_000.0;
        double avgDecodeMs = decodeTotal / (double) repeatTime / 1_000_000.0;
        System.out.println("SubcolumnPruneNew benchmark (points=" + data.length
                + ", blockSize=" + blockSize + ", repeats=" + repeatTime + ")");
        System.out.println("avg encode ms: " + avgEncodeMs);
        System.out.println("avg decode ms: " + avgDecodeMs);
        System.out.println("compressed bytes: " + encodedLen);
    }

    @Test
    public void benchmarkSprintzSubcolumn() {
        int blockSize = 512;
        int repeatTime = 200;
        int[] data = syntheticData(512 * 200, 99);
        byte[] encoded = new byte[data.length * 8];

        int encodedLen = SPRINTZSubcolumnPruneNewTest.Encoder(data, blockSize, encoded);

        long encodeTotal = 0;
        for (int i = 0; i < repeatTime; i++) {
            long start = System.nanoTime();
            encodedLen = SPRINTZSubcolumnPruneNewTest.Encoder(data, blockSize, encoded);
            encodeTotal += System.nanoTime() - start;
        }

        long decodeTotal = 0;
        byte[] encodedCopy = Arrays.copyOf(encoded, encodedLen);
        for (int i = 0; i < repeatTime; i++) {
            long start = System.nanoTime();
            SPRINTZSubcolumnPruneNewTest.Decoder(encodedCopy);
            decodeTotal += System.nanoTime() - start;
        }

        double avgEncodeMs = encodeTotal / (double) repeatTime / 1_000_000.0;
        double avgDecodeMs = decodeTotal / (double) repeatTime / 1_000_000.0;
        System.out.println("SPRINTZ+Subcolumn benchmark (points=" + data.length
                + ", blockSize=" + blockSize + ", repeats=" + repeatTime + ")");
        System.out.println("avg encode ms: " + avgEncodeMs);
        System.out.println("avg decode ms: " + avgDecodeMs);
        System.out.println("compressed bytes: " + encodedLen);
    }

    @Test
    public void benchmarkTs2diffSubcolumn() {
        int blockSize = 512;
        int repeatTime = 200;
        int[] data = syntheticData(512 * 200, 99);
        byte[] encoded = new byte[data.length * 8];

        int encodedLen = TSDIFFSubcolumnPruneNewTest.Encoder(data, blockSize, encoded);

        long encodeTotal = 0;
        for (int i = 0; i < repeatTime; i++) {
            long start = System.nanoTime();
            encodedLen = TSDIFFSubcolumnPruneNewTest.Encoder(data, blockSize, encoded);
            encodeTotal += System.nanoTime() - start;
        }

        long decodeTotal = 0;
        byte[] encodedCopy = Arrays.copyOf(encoded, encodedLen);
        for (int i = 0; i < repeatTime; i++) {
            long start = System.nanoTime();
            TSDIFFSubcolumnPruneNewTest.Decoder(encodedCopy);
            decodeTotal += System.nanoTime() - start;
        }

        double avgEncodeMs = encodeTotal / (double) repeatTime / 1_000_000.0;
        double avgDecodeMs = decodeTotal / (double) repeatTime / 1_000_000.0;
        System.out.println("TS2DIFF+Subcolumn benchmark (points=" + data.length
                + ", blockSize=" + blockSize + ", repeats=" + repeatTime + ")");
        System.out.println("avg encode ms: " + avgEncodeMs);
        System.out.println("avg decode ms: " + avgDecodeMs);
        System.out.println("compressed bytes: " + encodedLen);
    }
}
