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
import java.util.ArrayList;
import java.util.Arrays;


public class SubcolumnPruneNewBlockSizeNoPruneTest {

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

    private static int bitWidth(int value) {
        return 32 - Integer.numberOfLeadingZeros(value);
    }

    private static void int2Bytes(int integer, int encodePos, byte[] currentBytes) {
        currentBytes[encodePos] = (byte) (integer >> 24);
        currentBytes[encodePos + 1] = (byte) (integer >> 16);
        currentBytes[encodePos + 2] = (byte) (integer >> 8);
        currentBytes[encodePos + 3] = (byte) integer;
    }

    private static void intByte2Bytes(int integer, int encodePos, byte[] currentBytes) {
        currentBytes[encodePos] = (byte) integer;
    }

    private static void intToBytes(int srcNum, byte[] result, int pos, int width) {
        int cnt = pos & 0x07;
        int index = pos >> 3;
        while (width > 0) {
            int m = width + cnt >= 8 ? 8 - cnt : width;
            width -= m;
            int mask = 1 << (8 - cnt);
            cnt += m;
            byte y = (byte) (srcNum >>> width);
            y = (byte) (y << (8 - cnt));
            mask = ~(mask - (1 << (8 - cnt)));
            result[index] = (byte) (result[index] & mask | y);
            srcNum = srcNum & ~(-1 << width);
            if (cnt == 8) {
                index++;
                cnt = 0;
            }
        }
    }

    private static int bytesToInt(byte[] result, int pos, int width) {
        int ret = 0;
        int cnt = pos & 0x07;
        int index = pos >> 3;
        while (width > 0) {
            int m = width + cnt >= 8 ? 8 - cnt : width;
            width -= m;
            ret = ret << m;
            byte y = (byte) (result[index] & (0xff >> cnt));
            y = (byte) ((y & 0xff) >>> (8 - cnt - m));
            ret = ret | (y & 0xff);
            cnt += m;
            if (cnt == 8) {
                cnt = 0;
                index++;
            }
        }
        return ret;
    }

    private static void pack8Values(int[] values, int offset, int width, int encodePos,
            byte[] encodedResult) {
        int bufIdx = 0;
        int valueIdx = offset;
        int leftBit = 0;

        while (valueIdx < 8 + offset) {
            int buffer = 0;
            int leftSize = 32;

            if (leftBit > 0) {
                buffer |= (values[valueIdx] << (32 - leftBit));
                leftSize -= leftBit;
                leftBit = 0;
                valueIdx++;
            }

            while (leftSize >= width && valueIdx < 8 + offset) {
                buffer |= (values[valueIdx] << (leftSize - width));
                leftSize -= width;
                valueIdx++;
            }

            if (leftSize > 0 && valueIdx < 8 + offset) {
                buffer |= (values[valueIdx] >>> (width - leftSize));
                leftBit = width - leftSize;
            }

            for (int j = 0; j < 4; j++) {
                encodedResult[encodePos] = (byte) ((buffer >>> ((3 - j) * 8)) & 0xFF);
                encodePos++;
                bufIdx++;
                if (bufIdx >= width) {
                    return;
                }
            }
        }
    }

    private static int bitPacking(int[] numbers, int bitWidth, int encodePos, byte[] encodedResult,
            int numValues) {
        int blockNum = numValues / 8;
        int remainder = numValues % 8;

        for (int i = 0; i < blockNum; i++) {
            pack8Values(numbers, i * 8, bitWidth, encodePos, encodedResult);
            encodePos += bitWidth;
        }

        encodePos *= 8;

        for (int i = 0; i < remainder; i++) {
            intToBytes(numbers[blockNum * 8 + i], encodedResult, encodePos, bitWidth);
            encodePos += bitWidth;
        }

        return (encodePos + 7) / 8;
    }

    private static int bitPackingFromList(
            ArrayList<Integer> numbers, int bitWidth, int encodePos, byte[] encodedResult) {
        int numValues = numbers.size();
        int[] packed = new int[numValues];
        for (int i = 0; i < numValues; i++) {
            packed[i] = numbers.get(i);
        }
        return bitPacking(packed, bitWidth, encodePos, encodedResult, numValues);
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

    private static int[] getAbsDeltaTsBlock(int[] tsBlock, int blockIndex, int blockSize,
            int remaining, int[] minDelta) {
        int[] tsBlockDelta = new int[remaining];
        int valueDeltaMin = Integer.MAX_VALUE;
        int base = blockIndex * blockSize;
        int end = base + remaining;

        for (int j = base; j < end; j++) {
            int current = tsBlock[j];
            if (current < valueDeltaMin) {
                valueDeltaMin = current;
            }
        }

        for (int j = base; j < end; j++) {
            tsBlockDelta[j - base] = tsBlock[j] - valueDeltaMin;
        }

        minDelta[0] = valueDeltaMin;
        return tsBlockDelta;
    }

    private static int countTrue(boolean[] flags) {
        int count = 0;
        for (boolean flag : flags) {
            if (flag) {
                count++;
            }
        }
        return count;
    }

    private static void orMerge(boolean[] merged, boolean[] column) {
        for (int p = 0; p < merged.length; p++) {
            if (column[p]) {
                merged[p] = true;
            }
        }
    }

  /** Same beta decisions as {@link SubcolumnPruneTest#Subcolumn}; uses boolean[] / ArrayList. */
    private static int selectBetaAndEncodingTypes(int[] x, int xLength, int m, int blockSize,
            int[] encodingType) {
        if (m == 0) {
            return 1;
        }

        int betaBest = 1;
        int[] threshold = thresholdForBlockSize(blockSize);
        int lengthBitWidth = bitWidth(xLength);

        int[] bpeCostSingle = new int[m];
        int[] rleCostSingle = new int[m];
        int[] deCostSingle = new int[m];
        boolean[][] bitsets = new boolean[m][xLength];

        int cost1 = 0;
        for (int i = 0; i < m; i++) {
            int currentValue = (x[0] >> i) & 1;
            if (currentValue == 1) {
                bpeCostSingle[i] = xLength;
            }

            int count = 0;
            deCostSingle[i] = xLength + 2;

            for (int j = 1; j < xLength; j++) {
                int subcolumnIj = (x[j] >> i) & 1;
                if (subcolumnIj == 1) {
                    bpeCostSingle[i] = xLength;
                }
                if (subcolumnIj != currentValue) {
                    count++;
                    currentValue = subcolumnIj;
                    deCostSingle[i] = xLength * 2 + 2;
                    bitsets[i][j - 1] = true;
                }
            }

            bitsets[i][xLength - 1] = true;
            count++;
            rleCostSingle[i] = count * (1 + lengthBitWidth);

            if (bpeCostSingle[i] <= rleCostSingle[i] && bpeCostSingle[i] <= deCostSingle[i]) {
                encodingType[i] = 0;
                cost1 += bpeCostSingle[i];
            } else if (rleCostSingle[i] < bpeCostSingle[i] && rleCostSingle[i] <= deCostSingle[i]) {
                encodingType[i] = 1;
                cost1 += rleCostSingle[i];
            } else {
                encodingType[i] = 2;
                cost1 += deCostSingle[i];
            }
        }

        int cMin = cost1;

        for (int beta : BETA_LIST) {
            if (beta > m) {
                break;
            }

            int l = (m + beta - 1) / beta;
            int cost = 0;
            int[] encodingTypeTemp = new int[l];
            int mask = (1 << beta) - 1;

            for (int i = 0; i < l; i++) {
                int groupStart = i * beta;
                int groupEnd = Math.min(m, groupStart + beta);

                int betaStart = Math.min(m - 1, groupEnd - 1);
                while (betaStart >= groupStart && bpeCostSingle[betaStart] == 0) {
                    betaStart--;
                }
                if (betaStart < groupStart) {
                    betaStart = groupStart;
                }

                int currentCost = bpeCostSingle[betaStart] * (betaStart - groupStart + 1);

                int rleCostMax = 0;
                for (int j = groupStart; j < groupEnd; j++) {
                    if (rleCostSingle[j] > rleCostMax) {
                        rleCostMax = rleCostSingle[j];
                    }
                }

                if (rleCostMax < currentCost) {
                    boolean[] mergedFlags = new boolean[xLength];
                    boolean currentBetter = false;
                    for (int j = groupStart; j < groupEnd; j++) {
                        orMerge(mergedFlags, bitsets[j]);
                        if (countTrue(mergedFlags) >= currentCost) {
                            currentBetter = true;
                            break;
                        }
                    }
                    if (!currentBetter) {
                        int rleCost = countTrue(mergedFlags) * (beta + lengthBitWidth);
                        if (currentCost > rleCost) {
                            currentCost = rleCost;
                            encodingTypeTemp[i] = 1;
                        }
                    }
                }

                int deCostMax = 0;
                for (int j = groupStart; j < groupEnd; j++) {
                    if (deCostSingle[j] > deCostMax) {
                        deCostMax = deCostSingle[j];
                    }
                }

                if (deCostMax < currentCost) {
                    ArrayList<Integer> uniqueValues = new ArrayList<>();
                    boolean currentBetter = false;
                    for (int j = 0; j < xLength; j++) {
                        int currentNumber = (x[j] >> groupStart) & mask;
                        if (!uniqueValues.contains(currentNumber)) {
                            uniqueValues.add(currentNumber);
                        }
                        if (uniqueValues.size() >= threshold[beta - 1]) {
                            currentBetter = true;
                            break;
                        }
                    }
                    if (!currentBetter) {
                        int deCost =
                                xLength * bitWidth(uniqueValues.size())
                                        + uniqueValues.size() * beta;
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

    private static int subcolumnEncoder(int[] list, int encodePos, byte[] encodedResult, int[] beta,
            int blockSize, int[] encodingType) {
        int listLength = list.length;

        int maxValue = 0;
        for (int value : list) {
            if (value > maxValue) {
                maxValue = value;
            }
        }

        int m = bitWidth(maxValue);
        intByte2Bytes(m, encodePos, encodedResult);
        encodePos += 1;

        if (m == 0) {
            return encodePos;
        }

        int betaValue = beta[0];
        int l = (m + betaValue - 1) / betaValue;
        ArrayList<Integer> bitWidthList = new ArrayList<>(l);
        ArrayList<ArrayList<Integer>> subcolumnList = new ArrayList<>(l);

        intByte2Bytes(betaValue, encodePos, encodedResult);
        encodePos += 1;

        int bw = bitWidth(blockSize);
        int mask = (1 << betaValue) - 1;

        for (int i = 0; i < l; i++) {
            ArrayList<Integer> groupValues = new ArrayList<>(listLength);
            int maxValuePart = 0;
            int shiftAmount = i * betaValue;
            for (int j = 0; j < listLength; j++) {
                int part = (list[j] >> shiftAmount) & mask;
                groupValues.add(part);
                if (part > maxValuePart) {
                    maxValuePart = part;
                }
            }
            subcolumnList.add(groupValues);
            bitWidthList.add(bitWidth(maxValuePart));
        }

        encodePos = bitPackingFromList(bitWidthList, 8, encodePos, encodedResult);

        int preTypePos = encodePos;
        encodePos += (l + 3) / 4;

        for (int i = 0; i < l; i++) {
            ArrayList<Integer> subcolumnBuffer = subcolumnList.get(i);

            if (encodingType[i] == 2) {
                boolean[] seenValues = new boolean[mask + 1];
                for (int j = 0; j < listLength; j++) {
                    seenValues[subcolumnBuffer.get(j)] = true;
                }
                ArrayList<Integer> dictKeys = new ArrayList<>();
                for (int value = 0; value <= mask; value++) {
                    if (seenValues[value]) {
                        dictKeys.add(value);
                    }
                }
                int cardinality = dictKeys.size();
                int dictBitWidth = bitWidth(cardinality);

                for (int j = 0; j < listLength; j++) {
                    int current = subcolumnBuffer.get(j);
                    int code = dictKeys.indexOf(current);
                    subcolumnBuffer.set(j, code);
                }

                encodedResult[encodePos] = (byte) (cardinality >> 8);
                encodePos += 1;
                encodedResult[encodePos] = (byte) (cardinality & 0xFF);
                encodePos += 1;

                encodePos =
                        bitPackingFromList(
                                dictKeys, bitWidthList.get(i), encodePos, encodedResult);
                encodePos =
                        bitPackingFromList(subcolumnBuffer, dictBitWidth, encodePos, encodedResult);
                continue;
            }

            if (encodingType[i] == 0) {
                encodePos =
                        bitPackingFromList(
                                subcolumnBuffer, bitWidthList.get(i), encodePos, encodedResult);
            } else {
                ArrayList<Integer> runLength = new ArrayList<>();
                ArrayList<Integer> rleValues = new ArrayList<>();
                int previous = subcolumnBuffer.get(0);

                for (int j = 1; j < listLength; j++) {
                    int current = subcolumnBuffer.get(j);
                    if (current != previous) {
                        runLength.add(j);
                        rleValues.add(previous);
                        previous = current;
                    }
                }

                runLength.add(listLength);
                rleValues.add(previous);
                int runCount = runLength.size();

                encodedResult[encodePos] = (byte) (runCount >> 8);
                encodePos += 1;
                encodedResult[encodePos] = (byte) (runCount & 0xFF);
                encodePos += 1;

                encodePos = bitPackingFromList(runLength, bw, encodePos, encodedResult);
                encodePos =
                        bitPackingFromList(
                                rleValues, bitWidthList.get(i), encodePos, encodedResult);
            }
        }

        ArrayList<Integer> encodingTypeList = new ArrayList<>(l);
        for (int i = 0; i < l; i++) {
            encodingTypeList.add(encodingType[i]);
        }
        bitPackingFromList(encodingTypeList, 2, preTypePos, encodedResult);
        return encodePos;
    }

    private static int blockEncoder(int[] data, int blockIndex, int blockSize, int remainder,
            int encodePos, byte[] encodedResult, int[] beta) {
        int[] minDelta = new int[1];
        int[] dataDelta = getAbsDeltaTsBlock(data, blockIndex, blockSize, remainder, minDelta);

        int2Bytes(minDelta[0], encodePos, encodedResult);
        encodePos += 4;

        int maxValue = 0;
        for (int j = 0; j < remainder; j++) {
            if (dataDelta[j] > maxValue) {
                maxValue = dataDelta[j];
            }
        }

        int m = bitWidth(maxValue);
        int[] encodingType = new int[m];
        beta[0] = selectBetaAndEncodingTypes(dataDelta, remainder, m, blockSize, encodingType);

        return subcolumnEncoder(dataDelta, encodePos, encodedResult, beta, blockSize, encodingType);
    }

    public static int Encoder(int[] data, int blockSize, byte[] encodedResult) {
        int dataLength = data.length;
        int encodePos = 0;

        int2Bytes(dataLength, encodePos, encodedResult);
        encodePos += 4;

        int2Bytes(blockSize, encodePos, encodedResult);
        encodePos += 4;

        int numBlocks = dataLength / blockSize;
        int remainder = dataLength % blockSize;
        int[] beta = new int[] {2};

        for (int i = 0; i < numBlocks; i++) {
            encodePos = blockEncoder(data, i, blockSize, blockSize, encodePos, encodedResult, beta);
        }

        if (remainder <= 3) {
            int base = numBlocks * blockSize;
            for (int i = 0; i < remainder; i++) {
                int2Bytes(data[base + i], encodePos, encodedResult);
                encodePos += 4;
            }
        } else {
            encodePos =
                    blockEncoder(
                            data, numBlocks, blockSize, remainder, encodePos, encodedResult, beta);
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

    @Test
    public void testEncodedBytesMatchPruneFastTest() {
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
                byte[] plain = new byte[data.length * 13];
                int refLen = SubcolumnPruneFastTest.Encoder(data, blockSize, ref);
                int plainLen = Encoder(data, blockSize, plain);
                Assert.assertEquals("length mismatch blockSize=" + blockSize, refLen, plainLen);
                Assert.assertArrayEquals(
                        "bytes mismatch blockSize=" + blockSize,
                        Arrays.copyOf(ref, refLen),
                        Arrays.copyOf(plain, plainLen));
            }
        }
    }

    @Test
    public void testBlockSizeBenchmark() throws IOException {
        String parentDir = "/Users/xiaojinzhao/Documents/GitHub/subcolumn/";
        String inputParentDir = parentDir + "dataset/";
        String outputParentDir = parentDir + "result/compression_vs_block_noprune/";

        File outputDir = new File(outputParentDir);
        if (!outputDir.exists() && !outputDir.mkdirs()) {
            throw new IOException("Cannot create " + outputParentDir);
        }

        int[] blockSizeList = {32, 64, 128, 256, 512, 1024, 2048, 4096, 8192};
        int repeatTime = 400;
        int decodeRepeatTime = 400;

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
                        SubcolumnPruneFastTest.Encoder(data2Arr, blockSize, encodedRef);

                if (warmupBeforeTiming) {
                    for (int r = 0; r < repeatTime; r++) {
                        Encoder(data2Arr, blockSize, encoded);
                    }
                }

                long s = System.nanoTime();
                int encodedLen = 0;
                for (int r = 0; r < repeatTime; r++) {
                    encodedLen = Encoder(data2Arr, blockSize, encoded);
                }
                long encodeTime = (System.nanoTime() - s) / repeatTime;

                Assert.assertEquals(
                        "compressed size must match PruneFastTest: "
                                + datasetName
                                + " block="
                                + blockSize,
                        length,
                        encodedLen);
                Assert.assertArrayEquals(
                        Arrays.copyOf(encodedRef, length),
                        Arrays.copyOf(encoded, encodedLen));

                if (warmupBeforeTiming) {
                    for (int r = 0; r < decodeRepeatTime; r++) {
                        SubcolumnPruneNewTest.Decoder(encoded);
                    }
                }

                s = System.nanoTime();
                for (int r = 0; r < decodeRepeatTime; r++) {
                    SubcolumnPruneNewTest.Decoder(encoded);
                }
                long decodeTime = (System.nanoTime() - s) / decodeRepeatTime;

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
