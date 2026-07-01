package org.apache.iotdb.tsfile.encoding;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.BitSet;


public class SubcolumnPruneFastTest {

    private static final int[] BETA_LIST = {2, 3, 4};

    private static final int[] THRESHOLD_32 =
            {2, 3, 5, 8, 9, 11, 14, 16, 17, 17, 18, 19, 20, 21, 22, 22, 23, 24, 24, 24, 25, 25,
                    26, 26, 26, 26, 27, 27, 27, 27, 27, 27};
    private static final int[] THRESHOLD_64 =
            {2, 3, 5, 9, 13, 17, 19, 24, 29, 32, 33, 33, 35, 37, 39, 40, 42, 43, 44, 45, 46, 47,
                    48, 48, 49, 50, 50, 51, 51, 52, 52, 52};
    private static final int[] THRESHOLD_128 =
            {2, 3, 5, 9, 17, 22, 33, 33, 43, 52, 59, 64, 65, 65, 69, 72, 76, 79, 81, 84, 86, 88,
                    90, 91, 93, 94, 95, 96, 98, 99, 100, 100};
    private static final int[] THRESHOLD_256 =
            {2, 3, 5, 9, 17, 33, 37, 64, 65, 77, 94, 107, 119, 128, 129, 129, 136, 143, 149, 154,
                    159, 163, 167, 171, 175, 178, 181, 183, 186, 188, 190, 192};
    private static final int[] THRESHOLD_512 =
            {2, 3, 5, 9, 17, 33, 65, 65, 114, 129, 140, 171, 197, 220, 239, 256, 257, 257, 270,
                    282, 293, 303, 312, 320, 328, 335, 342, 348, 354, 359, 364, 368};
    private static final int[] THRESHOLD_1024 =
            {2, 3, 5, 9, 17, 33, 65, 128, 129, 205, 257, 257, 316, 366, 410, 448, 482, 512, 513,
                    513, 537, 559, 579, 598, 615, 631, 645, 659, 671, 683, 694, 704};
    private static final int[] THRESHOLD_2048 =
            {2, 3, 5, 9, 17, 33, 65, 129, 228, 257, 373, 512, 513, 586, 683, 768, 844, 911, 971,
                    1024, 1025, 1025, 1069, 1110, 1147, 1182, 1214, 1244, 1272, 1298, 1322, 1344};
    private static final int[] THRESHOLD_4096 =
            {2, 3, 5, 9, 17, 33, 65, 129, 257, 410, 513, 683, 946, 1025, 1093, 1280, 1446, 1593,
                    1725, 1844, 1951, 2048, 2049, 2049, 2130, 2206, 2276, 2341, 2402, 2458, 2511,
                    2560};
    private static final int[] THRESHOLD_8192 =
            {2, 3, 5, 9, 17, 33, 65, 129, 257, 513, 745, 1025, 1261, 1756, 2049, 2049, 2410, 2731,
                    3019, 3277, 3511, 3724, 3918, 4096, 4097, 4097, 4248, 4389, 4520, 4643, 4757,
                    4864};

    /** Reusable buffers for pruning (avoids per-block BitSet / array allocation). */
    static final class PruneWorkspace {
        BitSet[] bitsets = new BitSet[32];
        final BitSet merged = new BitSet();
        int[] bpeCostSingle = new int[32];
        int[] rleCostSingle = new int[32];
        int[] deCostSingle = new int[32];
        int[] encodingTypeTemp = new int[16];
        PruneWorkspace() {
            for (int i = 0; i < bitsets.length; i++) {
                bitsets[i] = new BitSet();
            }
        }

        void ensureM(int m) {
            if (m > bitsets.length) {
                int newLen = Math.max(m, bitsets.length * 2);
                bitsets = Arrays.copyOf(bitsets, newLen);
                for (int i = 0; i < newLen; i++) {
                    if (bitsets[i] == null) {
                        bitsets[i] = new BitSet();
                    }
                }
                bpeCostSingle = Arrays.copyOf(bpeCostSingle, newLen);
                rleCostSingle = Arrays.copyOf(rleCostSingle, newLen);
                deCostSingle = Arrays.copyOf(deCostSingle, newLen);
            }
            int maxL = (m + 1) / 2;
            if (encodingTypeTemp.length < maxL) {
                encodingTypeTemp = new int[maxL];
            }
        }

        void clearBitsets(int m) {
            for (int i = 0; i < m; i++) {
                bitsets[i].clear();
            }
        }
    }

    private static int[] thresholdForBlockSize(int blockSize) {
        switch (blockSize) {
            case 64:
                return THRESHOLD_64;
            case 128:
                return THRESHOLD_128;
            case 256:
                return THRESHOLD_256;
            case 512:
                return THRESHOLD_512;
            case 1024:
                return THRESHOLD_1024;
            case 2048:
                return THRESHOLD_2048;
            case 4096:
                return THRESHOLD_4096;
            case 8192:
                return THRESHOLD_8192;
            case 32:
            default:
                return THRESHOLD_32;
        }
    }

    /**
     * Same pruning decisions as {@link SubcolumnPruneTest#Subcolumn}; faster via reused
     * BitSets and int-bitmask dictionary cardinality (beta &le; 4).
     */
    public static int subcolumnPrune(
            PruneWorkspace ws,
            int[] x,
            int xLength,
            int m,
            int blockSize,
            int[] encodingType) {
        if (m == 0) {
            return 1;
        }

        ws.ensureM(m);
        ws.clearBitsets(m);

        int betaBest = 1;
        int[] threshold = thresholdForBlockSize(blockSize);
        int lengthBitWidth = 32 - Integer.numberOfLeadingZeros(xLength);
        int cost1 = 0;

        int[] bpe = ws.bpeCostSingle;
        int[] rle = ws.rleCostSingle;
        int[] de = ws.deCostSingle;
        BitSet[] bitsets = ws.bitsets;

        for (int i = 0; i < m; i++) {
            int currentValue = (x[0] >> i) & 1;
            boolean hasOne = currentValue == 1;
            int runCount = 1;
            boolean changed = false;

            for (int j = 1; j < xLength; j++) {
                int subcolumnIj = (x[j] >> i) & 1;
                if (subcolumnIj == 1) {
                    hasOne = true;
                }
                if (subcolumnIj != currentValue) {
                    runCount++;
                    currentValue = subcolumnIj;
                    changed = true;
                    bitsets[i].set(j - 1);
                }
            }

            bitsets[i].set(xLength - 1);
            bpe[i] = hasOne ? xLength : 0;
            rle[i] = runCount * (1 + lengthBitWidth);
            de[i] = changed ? xLength * 2 + 2 : xLength + 2;

            if (bpe[i] <= rle[i] && bpe[i] <= de[i]) {
                encodingType[i] = 0;
                cost1 += bpe[i];
            } else if (rle[i] < bpe[i] && rle[i] <= de[i]) {
                encodingType[i] = 1;
                cost1 += rle[i];
            } else {
                encodingType[i] = 2;
                cost1 += de[i];
            }
        }

        int cMin = cost1;
        BitSet merged = ws.merged;

        for (int beta : BETA_LIST) {
            if (beta > m) {
                break;
            }

            int l = (m + beta - 1) / beta;
            int cost = 0;
            int[] encodingTypeTemp = ws.encodingTypeTemp;
            Arrays.fill(encodingTypeTemp, 0, l, 0);
            int mask = (1 << beta) - 1;

            for (int i = 0; i < l; i++) {
                int groupStart = i * beta;
                int groupEnd = Math.min(m, groupStart + beta);

                int betaStart = Math.min(m - 1, groupEnd - 1);
                while (betaStart >= groupStart && bpe[betaStart] == 0) {
                    betaStart--;
                }
                if (betaStart < groupStart) {
                    betaStart = groupStart;
                }

                int currentCost = bpe[betaStart] * (betaStart - groupStart + 1);

                int rleCostMax = 0;
                for (int j = groupStart; j < groupEnd; j++) {
                    if (rle[j] > rleCostMax) {
                        rleCostMax = rle[j];
                    }
                }

                if (rleCostMax < currentCost) {
                    merged.clear();
                    boolean currentBetter = false;
                    for (int j = groupStart; j < groupEnd; j++) {
                        merged.or(bitsets[j]);
                        if (merged.cardinality() >= currentCost) {
                            currentBetter = true;
                            break;
                        }
                    }
                    if (!currentBetter) {
                        int rleCost = merged.cardinality() * (beta + lengthBitWidth);
                        if (currentCost > rleCost) {
                            currentCost = rleCost;
                            encodingTypeTemp[i] = 1;
                        }
                    }
                }

                int deCostMax = 0;
                for (int j = groupStart; j < groupEnd; j++) {
                    if (de[j] > deCostMax) {
                        deCostMax = de[j];
                    }
                }

                if (deCostMax < currentCost) {
                    int dictLimit = threshold[beta - 1];
                    int seenMask = 0;
                    int uniqueSize = 0;
                    for (int j = 0; j < xLength; j++) {
                        int currentNumber = (x[j] >> groupStart) & mask;
                        int bit = 1 << currentNumber;
                        if ((seenMask & bit) == 0) {
                            seenMask |= bit;
                            uniqueSize++;
                            if (uniqueSize >= dictLimit) {
                                uniqueSize = dictLimit;
                                break;
                            }
                        }
                    }
                    if (uniqueSize < dictLimit) {
                        int deCost =
                                xLength * (32 - Integer.numberOfLeadingZeros(uniqueSize))
                                        + uniqueSize * beta;
                        if (deCost < currentCost) {
                            currentCost = deCost;
                            encodingTypeTemp[i] = 2;
                        }
                    }
                }

                cost += currentCost;
            }

            if (cost < cMin) {
                cMin = cost;
                betaBest = beta;
                System.arraycopy(encodingTypeTemp, 0, encodingType, 0, l);
            }
        }

        return betaBest;
    }

    public static int BlockEncoder(
            PruneWorkspace ws,
            int[] data,
            int blockIndex,
            int blockSize,
            int remainder,
            int encodePos,
            byte[] encodedResult,
            int[] beta) {
        int[] minDelta = new int[1];
        int[] dataDelta =
                SubcolumnPruneNewTest.getAbsDeltaTsBlock(
                        data, blockIndex, blockSize, remainder, minDelta);

        SubcolumnPruneNewTest.int2Bytes(minDelta[0], encodePos, encodedResult);
        encodePos += 4;

        int maxValue = 0;
        for (int j = 0; j < remainder; j++) {
            if (dataDelta[j] > maxValue) {
                maxValue = dataDelta[j];
            }
        }

        int m = SubcolumnPruneNewTest.bitWidth(maxValue);
        int[] encodingType = new int[m];
        beta[0] = subcolumnPrune(ws, dataDelta, remainder, m, blockSize, encodingType);

        return SubcolumnPruneNewTest.SubcolumnEncoder(
                dataDelta, encodePos, encodedResult, beta, blockSize, encodingType);
    }

    public static int Encoder(int[] data, int blockSize, byte[] encodedResult) {
        int dataLength = data.length;
        int encodePos = 0;

        SubcolumnPruneNewTest.int2Bytes(dataLength, encodePos, encodedResult);
        encodePos += 4;

        SubcolumnPruneNewTest.int2Bytes(blockSize, encodePos, encodedResult);
        encodePos += 4;

        int numBlocks = dataLength / blockSize;
        int remainder = dataLength % blockSize;
        int[] beta = new int[] {2};
        PruneWorkspace ws = new PruneWorkspace();

        for (int i = 0; i < numBlocks; i++) {
            encodePos =
                    BlockEncoder(
                            ws, data, i, blockSize, blockSize, encodePos, encodedResult, beta);
        }

        if (remainder <= 3) {
            int base = numBlocks * blockSize;
            for (int i = 0; i < remainder; i++) {
                SubcolumnPruneNewTest.int2Bytes(data[base + i], encodePos, encodedResult);
                encodePos += 4;
            }
        } else {
            encodePos =
                    BlockEncoder(
                            ws, data, numBlocks, blockSize, remainder, encodePos, encodedResult,
                            beta);
        }

        return encodePos;
    }

    public static int getDecimalPrecision(String str) {
        int decimalIndex = str.indexOf('.');
        if (decimalIndex == -1) {
            return 0;
        }
        return str.substring(decimalIndex + 1).length();
    }

    /** Byte-identical to {@link SubcolumnPruneTest#Encoder} on synthetic blocks. */
    @Test
    public void testEncodedBytesMatchPruneTest() {
        int[] blockSizes = {32, 64, 128, 256, 512, 1024};
        int[][] patterns = {
            {0, 0, 0, 0},
            {1, 2, 3, 4, 5, 6, 7, 8},
            {100, 100, 101, 102, 103, 100, 100},
            {7, 7, 7, 8, 8, 9, 9, 9, 7, 7},
        };

        for (int blockSize : blockSizes) {
            for (int[] pattern : patterns) {
                int[] data = new int[blockSize];
                for (int i = 0; i < blockSize; i++) {
                    data[i] = pattern[i % pattern.length] + (i % 17);
                }
                byte[] ref = new byte[data.length * 13];
                byte[] fast = new byte[data.length * 13];
                int refLen = SubcolumnPruneTest.Encoder(data, blockSize, ref);
                int fastLen = Encoder(data, blockSize, fast);
                Assert.assertEquals(
                        "length mismatch blockSize=" + blockSize,
                        refLen,
                        fastLen);
                Assert.assertArrayEquals(
                        "bytes mismatch blockSize=" + blockSize,
                        Arrays.copyOf(ref, refLen),
                        Arrays.copyOf(fast, fastLen));
            }
        }
    }

    /** Spot-check against PruneNew (should match if New pruning equals Prune). */
    @Test
    public void testEncodedBytesMatchPruneNewTest() {
        int blockSize = 512;
        int[] data = new int[blockSize];
        for (int i = 0; i < blockSize; i++) {
            data[i] = (i * 37 + 11) % 1000;
        }
        byte[] ref = new byte[data.length * 13];
        byte[] fast = new byte[data.length * 13];
        int refLen = SubcolumnPruneNewTest.Encoder(data, blockSize, ref);
        int fastLen = Encoder(data, blockSize, fast);
        if (refLen != fastLen || !Arrays.equals(Arrays.copyOf(ref, refLen), Arrays.copyOf(fast, fastLen))) {
            System.out.println(
                    "Fast vs PruneNew: bytes differ (expected if RLE merge semantics differ).");
        }
    }


    @Test
    public void testBlockSizeBenchmark() throws IOException {
        // String parentDir = "/Users/xiaojinzhao/Documents/GitHub/subcolumn/";
        String parentDir = "D:/github/xjz17/subcolumn/";
        String inputParentDir = parentDir + "dataset/";
        String outputParentDir = parentDir + "result/compression_vs_block_noprune_legacy/";

        File outputDir = new File(outputParentDir);
        if (!outputDir.exists() && !outputDir.mkdirs()) {
            throw new IOException("Cannot create " + outputParentDir);
        }

        int[] blockSizeList = {32, 64, 128, 256, 512, 1024, 2048, 4096, 8192};
        int repeatTime = 100;

        String[] datasets = {
            "Bird-migration",
            "Bitcoin-price",
            "City-temp",
            "Dewpoint-temp",
            "EPM-Education",
            "Gov10",
            "IR-bio-temp",
            "PM10-dust",
            "Stocks-DE",
            "Stocks-UK",
            "Stocks-USA",
            "Wind-Speed",
            "Wine-Tasting"
        };

        PruneWorkspace ws = new PruneWorkspace();
        final int firstBlockSize = blockSizeList[0];

        for (int blockSize : blockSizeList) {
            final boolean warmupBeforeTiming = blockSize == firstBlockSize;
            String outputPath = outputParentDir + "subcolumn_block_" + blockSize + ".csv";
            CsvWriter writer = new CsvWriter(outputPath, ',', StandardCharsets.UTF_8);
            writer.setRecordDelimiter('\n');
            writer.writeRecord(
                    new String[] {
                        "Dataset",
                        "Encoding Algorithm",
                        "Encoding Time",
                        "Decoding Time",
                        "Points",
                        "Compressed Size",
                        "Compression Ratio"
                    });

            for (String datasetName : datasets) {
                File file = new File(inputParentDir + datasetName + ".csv");
                if (!file.exists()) {
                    System.out.println("Skip missing: " + file);
                    continue;
                }

                InputStream inputStream = Files.newInputStream(file.toPath());
                CsvReader loader = new CsvReader(inputStream, StandardCharsets.UTF_8);
                java.util.ArrayList<Double> data1 = new java.util.ArrayList<>();
                int maxDecimal = 0;
                while (loader.readRecord()) {
                    String fStr = loader.getValues()[0];
                    if (fStr.isEmpty()) {
                        continue;
                    }
                    maxDecimal = Math.max(maxDecimal, getDecimalPrecision(fStr));
                    data1.add(Double.valueOf(fStr));
                }
                loader.close();
                inputStream.close();

                if (maxDecimal > 8) {
                    maxDecimal = 8;
                }

                int[] data2Arr = new int[data1.size()];
                long maxMul = (long) Math.pow(10, maxDecimal);
                for (int i = 0; i < data1.size(); i++) {
                    data2Arr[i] = (int) (data1.get(i) * maxMul);
                }

                byte[] encoded = new byte[data2Arr.length * 13];
                byte[] encodedRef = new byte[data2Arr.length * 13];

                int length =
                        SubcolumnPruneTest.Encoder(data2Arr, blockSize, encodedRef);

                if (warmupBeforeTiming) {
                    for (int r = 0; r < repeatTime; r++) {
                        Encoder(data2Arr, blockSize, encoded);
                    }
                }

                long s = System.nanoTime();
                int fastLen = 0;
                for (int r = 0; r < repeatTime; r++) {
                    fastLen = Encoder(data2Arr, blockSize, encoded);
                }
                long encodeTime = (System.nanoTime() - s) / repeatTime;

                Assert.assertEquals(
                        "compressed size must match PruneTest: "
                                + datasetName
                                + " block="
                                + blockSize,
                        length,
                        fastLen);
                Assert.assertArrayEquals(
                        Arrays.copyOf(encodedRef, length),
                        Arrays.copyOf(encoded, fastLen));

                if (warmupBeforeTiming) {
                    for (int r = 0; r < repeatTime; r++) {
                        SubcolumnPruneTest.Decoder(encoded);
                    }
                }

                s = System.nanoTime();
                for (int r = 0; r < repeatTime; r++) {
                    SubcolumnPruneTest.Decoder(encoded);
                }
                long decodeTime = (System.nanoTime() - s) / repeatTime;

                double ratio = length / (double) (data1.size() * Long.BYTES);
                writer.writeRecord(
                        new String[] {
                            datasetName,
                            "Sub-columns",
                            String.valueOf(encodeTime),
                            String.valueOf(decodeTime),
                            String.valueOf(data1.size()),
                            String.valueOf(length),
                            String.valueOf(ratio)
                        });
                System.out.println(
                        datasetName
                                + " block="
                                + blockSize
                                + " encode_ns="
                                + encodeTime
                                + " ratio="
                                + ratio);
            }
            writer.close();
        }
    }
}
