package org.apache.iotdb.tsfile.encoding;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SubcolumnPruneNewBetaTest {

    private static final int[] DEFAULT_THRESHOLD =
            {2, 3, 5, 8, 9, 11, 14, 16, 17, 17, 18, 19, 20, 21, 22, 22, 23, 24, 24, 24, 25, 25, 26, 26, 26, 26, 27, 27, 27, 27, 27, 27};
    private static final int[] THRESHOLD_64 =
            {2, 3, 5, 9, 13, 17, 19, 24, 29, 32, 33, 33, 35, 37, 39, 40, 42, 43, 44, 45, 46, 47, 48, 48, 49, 50, 50, 51, 51, 52, 52, 52};
    private static final int[] THRESHOLD_128 =
            {2, 3, 5, 9, 17, 22, 33, 33, 43, 52, 59, 64, 65, 65, 69, 72, 76, 79, 81, 84, 86, 88, 90, 91, 93, 94, 95, 96, 98, 99, 100, 100};
    private static final int[] THRESHOLD_256 =
            {2, 3, 5, 9, 17, 33, 37, 64, 65, 77, 94, 107, 119, 128, 129, 129, 136, 143, 149, 154, 159, 163, 167, 171, 175, 178, 181, 183, 186, 188, 190, 192};
    private static final int[] THRESHOLD_512 =
            {2, 3, 5, 9, 17, 33, 65, 65, 114, 129, 140, 171, 197, 220, 239, 256, 257, 257, 270, 282, 293, 303, 312, 320, 328, 335, 342, 348, 354, 359, 364, 368};
    private static final int[] THRESHOLD_1024 =
            {2, 3, 5, 9, 17, 33, 65, 128, 129, 205, 257, 257, 316, 366, 410, 448, 482, 512, 513, 513, 537, 559, 579, 598, 615, 631, 645, 659, 671, 683, 694, 704};
    private static final int[] THRESHOLD_2048 =
            {2, 3, 5, 9, 17, 33, 65, 129, 228, 257, 373, 512, 513, 586, 683, 768, 844, 911, 971, 1024, 1025, 1025, 1069, 1110, 1147, 1182, 1214, 1244, 1272, 1298, 1322, 1344};
    private static final int[] THRESHOLD_4096 =
            {2, 3, 5, 9, 17, 33, 65, 129, 257, 410, 513, 683, 946, 1025, 1093, 1280, 1446, 1593, 1725, 1844, 1951, 2048, 2049, 2049, 2130, 2206, 2276, 2341, 2402, 2458, 2511, 2560};
    private static final int[] THRESHOLD_8192 =
            {2, 3, 5, 9, 17, 33, 65, 129, 257, 513, 745, 1025, 1261, 1756, 2049, 2049, 2410, 2731, 3019, 3277, 3511, 3724, 3918, 4096, 4097, 4097, 4248, 4389, 4520, 4643, 4757, 4864};
    private static final int[] BETA_LIST = {2, 3, 4};

    public static int bitWidth(int value) {
        return 32 - Integer.numberOfLeadingZeros(value);
    }

    public static void intToBytes(int srcNum, byte[] result, int pos, int width) {
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

    public static int bytesToInt(byte[] result, int pos, int width) {
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

    public static void boolToBytes(boolean value, byte[] result, int pos) {
        int byteIndex = pos >> 3;
        int bitOffset = pos & 0x07;

        if (value) {
            result[byteIndex] |= (1 << (7 - bitOffset));
        } else {
            result[byteIndex] &= ~(1 << (7 - bitOffset));
        }
    }

    public static boolean bytesToBool(byte[] result, int pos) {
        int byteIndex = pos >> 3;
        int bitOffset = pos & 0x07;

        return (result[byteIndex] & (1 << (7 - bitOffset))) != 0;
    }

    public static void pack8Values(int[] values, int offset, int width, int encodePos,
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

    public static void unpack8Values(byte[] encoded, int offset, int width, int[] resultList,
            int resultOffset) {
        int byteIdx = offset;
        long buffer = 0;
        int totalBits = 0;
        int valueIdx = 0;

        while (valueIdx < 8) {
            while (totalBits < width) {
                buffer = (buffer << 8) | (encoded[byteIdx] & 0xFF);
                byteIdx++;
                totalBits += 8;
            }

            while (totalBits >= width && valueIdx < 8) {
                resultList[resultOffset + valueIdx] = (int) (buffer >>> (totalBits - width));
                valueIdx++;
                totalBits -= width;
                buffer = buffer & ((1L << totalBits) - 1);
            }
        }
    }

    public static int bitPacking(int[] numbers, int bitWidth, int encodePos,
            byte[] encodedResult, int numValues) {
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

    public static int decodeBitPacking(
            byte[] encoded, int decodePos, int bitWidth, int numValues, int[] resultList) {
        int blockNum = numValues / 8;
        int remainder = numValues % 8;

        for (int i = 0; i < blockNum; i++) {
            unpack8Values(encoded, decodePos, bitWidth, resultList, i * 8);
            decodePos += bitWidth;
        }

        decodePos *= 8;

        for (int i = 0; i < remainder; i++) {
            resultList[blockNum * 8 + i] = bytesToInt(encoded, decodePos, bitWidth);
            decodePos += bitWidth;
        }

        return (decodePos + 7) / 8;
    }

    public static void int2Bytes(int integer, int encodePos, byte[] currentBytes) {
        currentBytes[encodePos] = (byte) (integer >> 24);
        currentBytes[encodePos + 1] = (byte) (integer >> 16);
        currentBytes[encodePos + 2] = (byte) (integer >> 8);
        currentBytes[encodePos + 3] = (byte) integer;
    }

    public static void intByte2Bytes(int integer, int encodePos, byte[] currentBytes) {
        currentBytes[encodePos] = (byte) integer;
    }

    public static void long2intBytes(long integer, int encodePos, byte[] currentBytes) {
        currentBytes[encodePos] = (byte) (integer >> 24);
        currentBytes[encodePos + 1] = (byte) (integer >> 16);
        currentBytes[encodePos + 2] = (byte) (integer >> 8);
        currentBytes[encodePos + 3] = (byte) integer;
    }

    public static int bytes2Integer(byte[] encoded, int start, int num) {
        int value = 0;

        for (int i = 0; i < num; i++) {
            value <<= 8;
            int b = encoded[i + start] & 0xFF;
            value |= b;
        }
        return value;
    }

    public static long bytesLong2Integer(byte[] encoded, int decodePos) {
        long value = 0;
        for (int i = 0; i < 4; i++) {
            value <<= 8;
            int b = encoded[i + decodePos] & 0xFF;
            value |= b;
        }
        return value;
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
                return DEFAULT_THRESHOLD;
        }
    }

    private static int countGroupedRuns(int[] values, int length, int shiftAmount, int mask) {
        int previous = (values[0] >> shiftAmount) & mask;
        int runs = 1;
        for (int i = 1; i < length; i++) {
            int current = (values[i] >> shiftAmount) & mask;
            if (current != previous) {
                runs++;
                previous = current;
            }
        }
        return runs;
    }

    private static int countDistinctValuesUntilLimit(int[] values, int length, int shiftAmount,
            int mask, int limit) {
        int seenMask = 0;
        int distinctCount = 0;
        for (int i = 0; i < length; i++) {
            int value = (values[i] >> shiftAmount) & mask;
            int bit = 1 << value;
            if ((seenMask & bit) == 0) {
                seenMask |= bit;
                distinctCount++;
                if (distinctCount >= limit) {
                    return distinctCount;
                }
            }
        }
        return distinctCount;
    }

    public static int Subcolumn(int[] x, int xLength, int m, int blockSize, int[] encodingType) {

        if (m == 0) {
            return 1;
        }

        int betaBest = 1;

        int[] bpeCostSingle = new int[m];
        int[] rleCostSingle = new int[m];
        int[] deCostSingle = new int[m];

        int[] threshold = thresholdForBlockSize(blockSize);
        int lengthBitWidth = bitWidth(xLength);
        int cost1 = 0;

        for (int i = 0; i < m; i++) {
            int currentValue = (x[0] >> i) & 1;
            boolean hasOne = currentValue == 1;
            int runCount = 1;
            boolean changed = false;

            for (int j = 1; j < xLength; j++) {
                int subcolumnValue = (x[j] >> i) & 1;
                if (subcolumnValue == 1) {
                    hasOne = true;
                }
                if (subcolumnValue != currentValue) {
                    runCount++;
                    currentValue = subcolumnValue;
                    changed = true;
                }
            }

            bpeCostSingle[i] = hasOne ? xLength : 0;
            rleCostSingle[i] = runCount * (1 + lengthBitWidth);
            deCostSingle[i] = changed ? xLength * 2 + 2 : xLength + 2;

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
                int betaStart = groupEnd - 1;

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
                    int runCount = countGroupedRuns(x, xLength, groupStart, mask);
                    int rleCost = runCount * (beta + lengthBitWidth);
                    if (rleCost < currentCost) {
                        currentCost = rleCost;
                        encodingTypeTemp[i] = 1;
                    }
                }

                int deCostMax = 0;
                for (int j = groupStart; j < groupEnd; j++) {
                    if (deCostSingle[j] > deCostMax) {
                        deCostMax = deCostSingle[j];
                    }
                }

                if (deCostMax < currentCost) {
                    int distinctCount = countDistinctValuesUntilLimit(
                            x, xLength, groupStart, mask, threshold[beta - 1]);
                    if (distinctCount < threshold[beta - 1]) {
                        int deCost = xLength * bitWidth(distinctCount) + distinctCount * beta;
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

    /**
     * Pick per-group encoding types (BPE/RLE/DE) for a fixed beta, using the same prune rules as
     * {@link #Subcolumn} but without searching {@link #BETA_LIST}.
     */
    public static void fillEncodingTypeForFixedBeta(
            int[] x,
            int xLength,
            int m,
            int blockSize,
            int beta,
            int[] encodingType) {
        if (m == 0 || beta > m) {
            return;
        }

        int[] bpeCostSingle = new int[m];
        int[] rleCostSingle = new int[m];
        int[] deCostSingle = new int[m];

        int[] threshold = thresholdForBlockSize(blockSize);
        int lengthBitWidth = bitWidth(xLength);

        for (int i = 0; i < m; i++) {
            int currentValue = (x[0] >> i) & 1;
            boolean hasOne = currentValue == 1;
            int runCount = 1;
            boolean changed = false;

            for (int j = 1; j < xLength; j++) {
                int subcolumnValue = (x[j] >> i) & 1;
                if (subcolumnValue == 1) {
                    hasOne = true;
                }
                if (subcolumnValue != currentValue) {
                    runCount++;
                    currentValue = subcolumnValue;
                    changed = true;
                }
            }

            bpeCostSingle[i] = hasOne ? xLength : 0;
            rleCostSingle[i] = runCount * (1 + lengthBitWidth);
            deCostSingle[i] = changed ? xLength * 2 + 2 : xLength + 2;
        }

        int l = (m + beta - 1) / beta;
        int mask = (1 << beta) - 1;

        for (int i = 0; i < l; i++) {
            int groupStart = i * beta;
            int groupEnd = Math.min(m, groupStart + beta);
            int betaStart = groupEnd - 1;

            while (betaStart >= groupStart && bpeCostSingle[betaStart] == 0) {
                betaStart--;
            }

            if (betaStart < groupStart) {
                betaStart = groupStart;
            }

            int currentCost = bpeCostSingle[betaStart] * (betaStart - groupStart + 1);
            encodingType[i] = 0;

            int rleCostMax = 0;
            for (int j = groupStart; j < groupEnd; j++) {
                if (rleCostSingle[j] > rleCostMax) {
                    rleCostMax = rleCostSingle[j];
                }
            }

            if (rleCostMax < currentCost) {
                int runCount = countGroupedRuns(x, xLength, groupStart, mask);
                int rleCost = runCount * (beta + lengthBitWidth);
                if (rleCost < currentCost) {
                    currentCost = rleCost;
                    encodingType[i] = 1;
                }
            }

            int deCostMax = 0;
            for (int j = groupStart; j < groupEnd; j++) {
                if (deCostSingle[j] > deCostMax) {
                    deCostMax = deCostSingle[j];
                }
            }

            if (deCostMax < currentCost) {
                int distinctCount =
                        countDistinctValuesUntilLimit(x, xLength, groupStart, mask, threshold[beta - 1]);
                if (distinctCount < threshold[beta - 1]) {
                    int deCost = xLength * bitWidth(distinctCount) + distinctCount * beta;
                    if (deCost < currentCost) {
                        encodingType[i] = 2;
                    }
                }
            }
        }
    }

    public static int SubcolumnEncoder(int[] list, int encodePos, byte[] encodedResult,
            int[] beta, int blockSize, int[] encodingType) {
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
        int[] bitWidthList = new int[l];

        intByte2Bytes(betaValue, encodePos, encodedResult);
        encodePos += 1;

        int bw = bitWidth(blockSize);
        int mask = (1 << betaValue) - 1;

        for (int i = 0; i < l; i++) {
            int maxValuePart = 0;
            int shiftAmount = i * betaValue;
            for (int j = 0; j < listLength; j++) {
                int current = (list[j] >> shiftAmount) & mask;
                if (current > maxValuePart) {
                    maxValuePart = current;
                }
            }
            bitWidthList[i] = bitWidth(maxValuePart);
        }

        encodePos = bitPacking(bitWidthList, 8, encodePos, encodedResult, l);

        int preTypePos = encodePos;
        encodePos += (l + 3) / 4;

        int[] subcolumnBuffer = new int[listLength];
        int[] runLength = new int[listLength];
        int[] rleValues = new int[listLength];
        boolean[] seenValues = new boolean[mask + 1];
        int[] dictKeyList = new int[mask + 1];
        int[] codeMap = new int[mask + 1];

        for (int i = 0; i < l; i++) {
            int shiftAmount = i * betaValue;
            for (int j = 0; j < listLength; j++) {
                subcolumnBuffer[j] = (list[j] >> shiftAmount) & mask;
            }

            if (encodingType[i] == 2) {
                Arrays.fill(seenValues, false);
                int cardinality = 0;
                for (int j = 0; j < listLength; j++) {
                    int current = subcolumnBuffer[j];
                    if (!seenValues[current]) {
                        seenValues[current] = true;
                        cardinality++;
                    }
                }

                int dictBitWidth = bitWidth(cardinality);
                int dictSize = 0;
                for (int value = 0; value <= mask; value++) {
                    if (seenValues[value]) {
                        dictKeyList[dictSize] = value;
                        codeMap[value] = dictSize;
                        dictSize++;
                    }
                }

                for (int j = 0; j < listLength; j++) {
                    subcolumnBuffer[j] = codeMap[subcolumnBuffer[j]];
                }

                encodedResult[encodePos] = (byte) (cardinality >> 8);
                encodePos += 1;
                encodedResult[encodePos] = (byte) (cardinality & 0xFF);
                encodePos += 1;

                encodePos = bitPacking(dictKeyList, bitWidthList[i], encodePos, encodedResult,
                        cardinality);
                encodePos = bitPacking(subcolumnBuffer, dictBitWidth, encodePos, encodedResult,
                        listLength);
                continue;
            }

            if (encodingType[i] == 0) {
                encodePos = bitPacking(subcolumnBuffer, bitWidthList[i], encodePos, encodedResult,
                        listLength);
            } else {
                int previous = subcolumnBuffer[0];
                int runCount = 0;

                for (int j = 1; j < listLength; j++) {
                    int current = subcolumnBuffer[j];
                    if (current != previous) {
                        runLength[runCount] = j;
                        rleValues[runCount] = previous;
                        runCount++;
                        previous = current;
                    }
                }

                runLength[runCount] = listLength;
                rleValues[runCount] = previous;
                runCount++;

                encodedResult[encodePos] = (byte) (runCount >> 8);
                encodePos += 1;
                encodedResult[encodePos] = (byte) (runCount & 0xFF);
                encodePos += 1;

                encodePos = bitPacking(runLength, bw, encodePos, encodedResult, runCount);
                encodePos = bitPacking(rleValues, bitWidthList[i], encodePos, encodedResult,
                        runCount);
            }
        }

        bitPacking(encodingType, 2, preTypePos, encodedResult, l);
        return encodePos;
    }

    public static int SubcolumnDecoder(byte[] encodedResult, int encodePos, int[] list,
            int blockSize) {
        int listLength = list.length;
        int m = bytes2Integer(encodedResult, encodePos, 1);
        encodePos += 1;

        if (m == 0) {
            return encodePos;
        }

        int bw = bitWidth(blockSize);
        int beta = bytes2Integer(encodedResult, encodePos, 1);
        encodePos += 1;

        int l = (m + beta - 1) / beta;
        int[] bitWidthList = new int[l];
        encodePos = decodeBitPacking(encodedResult, encodePos, 8, l, bitWidthList);

        int[] encodingType = new int[l];
        encodePos = decodeBitPacking(encodedResult, encodePos, 2, l, encodingType);

        int[] subcolumnBuffer = new int[listLength];
        int[] runLength = new int[listLength];
        int[] rleValues = new int[listLength];

        for (int i = 0; i < l; i++) {
            int type = encodingType[i];
            int currentBitWidth = bitWidthList[i];

            if (type == 0) {
                encodePos = decodeBitPacking(encodedResult, encodePos, currentBitWidth,
                        listLength, subcolumnBuffer);
            } else if (type == 1) {
                int index = ((encodedResult[encodePos] & 0xFF) << 8)
                        | (encodedResult[encodePos + 1] & 0xFF);
                encodePos += 2;

                encodePos = decodeBitPacking(encodedResult, encodePos, bw, index, runLength);
                encodePos = decodeBitPacking(encodedResult, encodePos, currentBitWidth, index,
                        rleValues);

                int currentIndex = 0;
                for (int j = 0; j < index; j++) {
                    int endPos = runLength[j];
                    int value = rleValues[j];
                    while (currentIndex < endPos) {
                        subcolumnBuffer[currentIndex] = value;
                        currentIndex++;
                    }
                }
            } else {
                int cardinality = ((encodedResult[encodePos] & 0xFF) << 8)
                        | (encodedResult[encodePos + 1] & 0xFF);
                encodePos += 2;

                int dictBitWidth = bitWidth(cardinality);
                int[] dictKeyList = new int[cardinality];
                encodePos = decodeBitPacking(encodedResult, encodePos, currentBitWidth,
                        cardinality, dictKeyList);
                encodePos = decodeBitPacking(encodedResult, encodePos, dictBitWidth, listLength,
                        subcolumnBuffer);

                for (int j = 0; j < listLength; j++) {
                    subcolumnBuffer[j] = dictKeyList[subcolumnBuffer[j]];
                }
            }

            int shiftAmount = i * beta;
            for (int j = 0; j < listLength; j++) {
                list[j] |= subcolumnBuffer[j] << shiftAmount;
            }
        }

        return encodePos;
    }

    public static int[] getAbsDeltaTsBlock(int[] tsBlock, int blockIndex, int blockSize,
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

    public static int BlockEncoder(int[] data, int blockIndex, int blockSize, int remainder,
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
        int betaValue = beta[0];
        int l = (m + betaValue - 1) / betaValue;
        int[] encodingType = new int[l];
        fillEncodingTypeForFixedBeta(dataDelta, remainder, m, blockSize, betaValue, encodingType);

        return SubcolumnEncoder(dataDelta, encodePos, encodedResult, beta, blockSize,
                encodingType);
    }

    public static int BlockDecoder(byte[] encodedResult, int blockIndex, int blockSize,
            int remainder, int encodePos, int[] data) {
        int minDelta = bytes2Integer(encodedResult, encodePos, 4);
        encodePos += 4;

        int[] blockData = new int[remainder];
        encodePos = SubcolumnDecoder(encodedResult, encodePos, blockData, blockSize);

        int base = blockIndex * blockSize;
        for (int i = 0; i < remainder; i++) {
            data[base + i] = blockData[i] + minDelta;
        }

        return encodePos;
    }

    public static int Encoder(int[] data, int blockSize, byte[] encodedResult) {
        return Encoder(data, blockSize, encodedResult, 2);
    }

    public static int Encoder(int[] data, int blockSize, byte[] encodedResult, int betaValue) {
        int dataLength = data.length;
        int encodePos = 0;

        int2Bytes(dataLength, encodePos, encodedResult);
        encodePos += 4;

        int2Bytes(blockSize, encodePos, encodedResult);
        encodePos += 4;

        int numBlocks = dataLength / blockSize;
        int remainder = dataLength % blockSize;
        int[] beta = new int[] {betaValue};

        for (int i = 0; i < numBlocks; i++) {
            encodePos =
                    BlockEncoder(data, i, blockSize, blockSize, encodePos, encodedResult, beta);
        }

        if (remainder <= 3) {
            int base = numBlocks * blockSize;
            for (int i = 0; i < remainder; i++) {
                int2Bytes(data[base + i], encodePos, encodedResult);
                encodePos += 4;
            }
        } else {
            encodePos =
                    BlockEncoder(data, numBlocks, blockSize, remainder, encodePos, encodedResult, beta);
        }

        return encodePos;
    }

    public static int[] Decoder(byte[] encodedResult) {
        int encodePos = 0;

        int dataLength = bytes2Integer(encodedResult, encodePos, 4);
        encodePos += 4;

        int blockSize = bytes2Integer(encodedResult, encodePos, 4);
        encodePos += 4;

        int numBlocks = dataLength / blockSize;
        int[] data = new int[dataLength];

        for (int i = 0; i < numBlocks; i++) {
            encodePos = BlockDecoder(encodedResult, i, blockSize, blockSize, encodePos, data);
        }

        int remainder = dataLength % blockSize;
        if (remainder <= 3) {
            int base = numBlocks * blockSize;
            for (int i = 0; i < remainder; i++) {
                data[base + i] = bytes2Integer(encodedResult, encodePos, 4);
                encodePos += 4;
            }
        } else {
            encodePos = BlockDecoder(encodedResult, numBlocks, blockSize, remainder, encodePos,
                    data);
        }

        return data;
    }

    public static int getDecimalPrecision(String str) {
        int decimalIndex = str.indexOf('.');

        if (decimalIndex == -1) {
            return 0;
        }

        return str.length() - decimalIndex - 1;
    }

    public static String extractFileName(String path) {
        if (path == null || path.isEmpty()) {
            return "";
        }

        File file = new File(path);
        String fileName = file.getName();
        int dotIndex = fileName.lastIndexOf('.');

        if (dotIndex == -1 || dotIndex == 0) {
            return fileName;
        }

        return fileName.substring(0, dotIndex);
    }

    @Test
    public void testCompressionVsBeta() throws IOException {
        // String parentDir = "path/to/your/directory/";
        String parentDir = "D:/github/xjz17/subcolumn/";

        String inputParentDir = parentDir + "dataset/";

        String outputParentDir = parentDir + "result/compression_vs_beta_prune/";

        File outputDir = new File(outputParentDir);
        if (!outputDir.exists()) {
            outputDir.mkdirs();
        }

        int[] betaList = {
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
            25, 26, 27, 28, 29, 30, 31
        };

        int blockSize = 1024;
        int repeatTime = 200;

        List<String> datasetList = new ArrayList<>();
        datasetList.add("Arade4");
        datasetList.add("Bird-migration");
        datasetList.add("Bitcoin-price");
        datasetList.add("Census-Population");
        datasetList.add("City-temp");
        datasetList.add("Dewpoint-temp");
        datasetList.add("EPM-Education");
        datasetList.add("Gov10");
        datasetList.add("MeteoNet-Weather");
        // datasetList.add("POI-lat");
        datasetList.add("IR-bio-temp");
        datasetList.add("PM10-dust");
        datasetList.add("Stocks-DE");
        datasetList.add("Stocks-UK");
        datasetList.add("Stocks-USA");
        datasetList.add("Wind-Speed");
        datasetList.add("Wine-Tasting");

        for (int beta : betaList) {
            String outputPath = outputParentDir + "subcolumn_beta_" + beta + ".csv";

            CsvWriter writer = new CsvWriter(outputPath, ',', StandardCharsets.UTF_8);
            writer.setRecordDelimiter('\n');

            String[] head = {
                "Dataset",
                "Encoding Algorithm",
                "Encoding Time",
                "Decoding Time",
                "Points",
                "Compressed Size",
                "Compression Ratio"
            };
            writer.writeRecord(head);

            for (String datasetName : datasetList) {
                String filePath = inputParentDir + datasetName + ".csv";
                File file = new File(filePath);

                if (!file.exists()) {
                    System.out.println("File not found: " + filePath);
                    continue;
                }

                System.out.println(datasetName);

                InputStream inputStream = Files.newInputStream(file.toPath());
                CsvReader loader = new CsvReader(inputStream, StandardCharsets.UTF_8);
                ArrayList<Double> data1 = new ArrayList<>();

                int maxDecimal = 0;
                while (loader.readRecord()) {
                    String fStr = loader.getValues()[0];
                    if (fStr.isEmpty()) {
                        continue;
                    }
                    int curDecimal = getDecimalPrecision(fStr);
                    if (curDecimal > maxDecimal) {
                        maxDecimal = curDecimal;
                    }
                    data1.add(Double.valueOf(fStr));
                }
                inputStream.close();

                if (maxDecimal > 8) {
                    maxDecimal = 8;
                }

                int[] data2Arr = new int[data1.size()];
                long maxMul = (long) Math.pow(10, maxDecimal);
                for (int i = 0; i < data1.size(); i++) {
                    data2Arr[i] = (int) (data1.get(i) * maxMul);
                }

                byte[] encodedResult = new byte[data2Arr.length * 13];

                long encodeTime = 0;
                long decodeTime = 0;
                double ratio = 0;
                double compressedSize = 0;

                int length = 0;

                long s = System.nanoTime();
                for (int repeat = 0; repeat < repeatTime; repeat++) {
                    length = Encoder(data2Arr, blockSize, encodedResult, beta);
                }
                long e = System.nanoTime();
                encodeTime += ((e - s) / repeatTime);
                compressedSize += length;

                ratio += compressedSize / (double) (data1.size() * Long.BYTES);

                s = System.nanoTime();
                for (int repeat = 0; repeat < repeatTime; repeat++) {
                    Decoder(encodedResult);
                }
                e = System.nanoTime();
                decodeTime += ((e - s) / repeatTime);

                String[] record = {
                    datasetName,
                    "Sub-columns",
                    String.valueOf(encodeTime),
                    String.valueOf(decodeTime),
                    String.valueOf(data1.size()),
                    String.valueOf(compressedSize),
                    String.valueOf(ratio)
                };
                writer.writeRecord(record);

                System.out.println("beta: " + beta);
                System.out.println(ratio);
            }

            writer.close();
        }
    }
}
