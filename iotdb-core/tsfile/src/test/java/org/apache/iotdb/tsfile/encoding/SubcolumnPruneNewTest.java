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

public class SubcolumnPruneNewTest {

    private static final ThreadLocal<EncodeScratch> ENCODE_SCRATCH =
            ThreadLocal.withInitial(EncodeScratch::new);

    private static final ThreadLocal<DecodeScratch> DECODE_SCRATCH =
            ThreadLocal.withInitial(DecodeScratch::new);

    private static final class EncodeScratch {
        private final int[] dataDelta = new int[8192];
        private final int[] bpeCostSingle = new int[32];
        private final int[] rleCostSingle = new int[32];
        private final int[] deCostSingle = new int[32];
        private final int[] encodingType = new int[32];
        private final int[] encodingTypeTemp = new int[32];
        private final int[] bitWidthList = new int[32];
        private final int[] subcolumnBuffer = new int[8192];
        private final int[] runLength = new int[8192];
        private final int[] rleValues = new int[8192];
        private final int[] dictKeyList = new int[16];
        private final int[] codeMap = new int[16];
        private final int[] minDelta = new int[1];
        private final int[] minDelta3 = new int[3];
        private final int[] beta = new int[1];
        private final int[] betaCandidateOrder = new int[3];
        private final int[] betaCandidateLowerBound = new int[3];
        private final int[] betaCandidateCost = new int[3];
        private final int[] betaCandidateEncodingType = new int[3 * 32];
        private int lastBestBeta = 2;
        /** Flattened grouped subcolumns: group i starts at i * listLength. */
        private final int[] groupFlat = new int[32 * 8192];
        private final int[] groupMax = new int[32];
        private int cachedBeta = -1;
        private int cachedL;
        private int cachedListLength = -1;
    }

    private static final class DecodeScratch {
        private int[] bitWidthList = new int[32];
        private int[] encodingType = new int[32];
        private int[] subcolumnBuffer = new int[8192];
        private int[] runLength = new int[8192];
        private int[] rleValues = new int[8192];
        private int[] dictKeyList = new int[16];

        private void ensureL(int l) {
            if (bitWidthList.length < l) {
                bitWidthList = new int[l];
                encodingType = new int[l];
            }
        }

        private void ensureListLength(int listLength) {
            if (subcolumnBuffer.length < listLength) {
                subcolumnBuffer = new int[listLength];
                runLength = new int[listLength];
                rleValues = new int[listLength];
            }
        }

        private int[] ensureDict(int cardinality) {
            if (dictKeyList.length < cardinality) {
                dictKeyList = new int[cardinality];
            }
            return dictKeyList;
        }
    }

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
    private static boolean USE_ALPHA_HYBRID = false;
    private static boolean USE_ALPHA_FAST_HYBRID = false;

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
        return bitPackingAt(numbers, 0, bitWidth, encodePos, encodedResult, numValues);
    }

    private static int bitPackingAt(int[] numbers, int offset, int bitWidth, int encodePos,
            byte[] encodedResult, int numValues) {
        if (bitWidth == 0) {
            return encodePos;
        }
        if (bitWidth == 1) {
            return bitPackingWidth1At(numbers, offset, encodePos, encodedResult, numValues);
        }
        if (bitWidth == 2) {
            return bitPackingWidth2At(numbers, offset, encodePos, encodedResult, numValues);
        }
        if (bitWidth == 4) {
            return bitPackingWidth4At(numbers, offset, encodePos, encodedResult, numValues);
        }
        if (bitWidth == 8) {
            return bitPackingWidth8At(numbers, offset, encodePos, encodedResult, numValues);
        }

        int blockNum = numValues / 8;
        int remainder = numValues % 8;

        for (int i = 0; i < blockNum; i++) {
            pack8Values(numbers, offset + i * 8, bitWidth, encodePos, encodedResult);
            encodePos += bitWidth;
        }

        encodePos *= 8;

        for (int i = 0; i < remainder; i++) {
            intToBytes(numbers[offset + blockNum * 8 + i], encodedResult, encodePos, bitWidth);
            encodePos += bitWidth;
        }

        return (encodePos + 7) / 8;
    }

    private static int bitPackingWidth1At(int[] numbers, int offset, int encodePos,
            byte[] encodedResult, int numValues) {
        int i = 0;
        while (i + 8 <= numValues) {
            int base = offset + i;
            encodedResult[encodePos] = (byte) ((numbers[base] << 7) | (numbers[base + 1] << 6)
                    | (numbers[base + 2] << 5) | (numbers[base + 3] << 4)
                    | (numbers[base + 4] << 3) | (numbers[base + 5] << 2)
                    | (numbers[base + 6] << 1) | numbers[base + 7]);
            encodePos++;
            i += 8;
        }
        int bitPos = encodePos * 8;
        while (i < numValues) {
            intToBytes(numbers[offset + i], encodedResult, bitPos, 1);
            bitPos++;
            i++;
        }
        return (bitPos + 7) / 8;
    }

    private static int bitPackingWidth2At(int[] numbers, int offset, int encodePos,
            byte[] encodedResult, int numValues) {
        int i = 0;
        while (i + 4 <= numValues) {
            int base = offset + i;
            encodedResult[encodePos] = (byte) ((numbers[base] << 6) | (numbers[base + 1] << 4)
                    | (numbers[base + 2] << 2) | numbers[base + 3]);
            encodePos++;
            i += 4;
        }
        int bitPos = encodePos * 8;
        while (i < numValues) {
            intToBytes(numbers[offset + i], encodedResult, bitPos, 2);
            bitPos += 2;
            i++;
        }
        return (bitPos + 7) / 8;
    }

    private static int bitPackingWidth4At(int[] numbers, int offset, int encodePos,
            byte[] encodedResult, int numValues) {
        int i = 0;
        while (i + 2 <= numValues) {
            int base = offset + i;
            encodedResult[encodePos] = (byte) ((numbers[base] << 4) | numbers[base + 1]);
            encodePos++;
            i += 2;
        }
        if (i < numValues) {
            int bitPos = encodePos * 8;
            intToBytes(numbers[offset + i], encodedResult, bitPos, 4);
            bitPos += 4;
            return (bitPos + 7) / 8;
        }
        return encodePos;
    }

    private static int bitPackingWidth8At(int[] numbers, int offset, int encodePos,
            byte[] encodedResult, int numValues) {
        for (int i = 0; i < numValues; i++) {
            encodedResult[encodePos++] = (byte) numbers[offset + i];
        }
        return encodePos;
    }

    private static int bitPackingShifted(int[] list, int listLength, int shiftAmount, int mask,
            int bitWidth, int encodePos, byte[] encodedResult, int[] fallbackBuffer) {
        if (bitWidth == 0) {
            return encodePos;
        }
        if (bitWidth == 1) {
            return bitPackingWidth1Shifted(list, listLength, shiftAmount, mask, encodePos,
                    encodedResult);
        }
        if (bitWidth == 2) {
            return bitPackingWidth2Shifted(list, listLength, shiftAmount, mask, encodePos,
                    encodedResult);
        }
        if (bitWidth == 4) {
            return bitPackingWidth4Shifted(list, listLength, shiftAmount, mask, encodePos,
                    encodedResult);
        }
        if (bitWidth == 8) {
            return bitPackingWidth8Shifted(list, listLength, shiftAmount, mask, encodePos,
                    encodedResult);
        }

        for (int j = 0; j < listLength; j++) {
            fallbackBuffer[j] = (list[j] >> shiftAmount) & mask;
        }
        return bitPacking(fallbackBuffer, bitWidth, encodePos, encodedResult, listLength);
    }

    private static int bitPackingWidth1Shifted(int[] list, int listLength, int shiftAmount,
            int mask, int encodePos, byte[] encodedResult) {
        int i = 0;
        while (i + 8 <= listLength) {
            encodedResult[encodePos] = (byte) ((((list[i] >> shiftAmount) & mask) << 7)
                    | (((list[i + 1] >> shiftAmount) & mask) << 6)
                    | (((list[i + 2] >> shiftAmount) & mask) << 5)
                    | (((list[i + 3] >> shiftAmount) & mask) << 4)
                    | (((list[i + 4] >> shiftAmount) & mask) << 3)
                    | (((list[i + 5] >> shiftAmount) & mask) << 2)
                    | (((list[i + 6] >> shiftAmount) & mask) << 1)
                    | ((list[i + 7] >> shiftAmount) & mask));
            encodePos++;
            i += 8;
        }
        int bitPos = encodePos * 8;
        while (i < listLength) {
            intToBytes((list[i] >> shiftAmount) & mask, encodedResult, bitPos, 1);
            bitPos++;
            i++;
        }
        return (bitPos + 7) / 8;
    }

    private static int bitPackingWidth2Shifted(int[] list, int listLength, int shiftAmount,
            int mask, int encodePos, byte[] encodedResult) {
        int i = 0;
        while (i + 4 <= listLength) {
            encodedResult[encodePos] = (byte) ((((list[i] >> shiftAmount) & mask) << 6)
                    | (((list[i + 1] >> shiftAmount) & mask) << 4)
                    | (((list[i + 2] >> shiftAmount) & mask) << 2)
                    | ((list[i + 3] >> shiftAmount) & mask));
            encodePos++;
            i += 4;
        }
        int bitPos = encodePos * 8;
        while (i < listLength) {
            intToBytes((list[i] >> shiftAmount) & mask, encodedResult, bitPos, 2);
            bitPos += 2;
            i++;
        }
        return (bitPos + 7) / 8;
    }

    private static int bitPackingWidth4Shifted(int[] list, int listLength, int shiftAmount,
            int mask, int encodePos, byte[] encodedResult) {
        int i = 0;
        while (i + 2 <= listLength) {
            encodedResult[encodePos] = (byte) ((((list[i] >> shiftAmount) & mask) << 4)
                    | ((list[i + 1] >> shiftAmount) & mask));
            encodePos++;
            i += 2;
        }
        if (i < listLength) {
            int bitPos = encodePos * 8;
            intToBytes((list[i] >> shiftAmount) & mask, encodedResult, bitPos, 4);
            bitPos += 4;
            return (bitPos + 7) / 8;
        }
        return encodePos;
    }

    private static int bitPackingWidth8Shifted(int[] list, int listLength, int shiftAmount,
            int mask, int encodePos, byte[] encodedResult) {
        for (int i = 0; i < listLength; i++) {
            encodedResult[encodePos++] = (byte) ((list[i] >> shiftAmount) & mask);
        }
        return encodePos;
    }

    public static int decodeBitPacking(
            byte[] encoded, int decodePos, int bitWidth, int numValues, int[] resultList) {
        if (bitWidth == 0) {
            Arrays.fill(resultList, 0, numValues, 0);
            return decodePos;
        }
        if (bitWidth == 1) {
            return decodeBitPackingWidth1(encoded, decodePos, numValues, resultList);
        }
        if (bitWidth == 2) {
            return decodeBitPackingWidth2(encoded, decodePos, numValues, resultList);
        }
        if (bitWidth == 4) {
            return decodeBitPackingWidth4(encoded, decodePos, numValues, resultList);
        }
        if (bitWidth == 8) {
            return decodeBitPackingWidth8(encoded, decodePos, numValues, resultList);
        }

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

    private static int decodeBitPackingWidth1(
            byte[] encoded, int decodePos, int numValues, int[] resultList) {
        int i = 0;
        while (i + 8 <= numValues) {
            int value = encoded[decodePos++] & 0xFF;
            resultList[i] = (value >>> 7) & 1;
            resultList[i + 1] = (value >>> 6) & 1;
            resultList[i + 2] = (value >>> 5) & 1;
            resultList[i + 3] = (value >>> 4) & 1;
            resultList[i + 4] = (value >>> 3) & 1;
            resultList[i + 5] = (value >>> 2) & 1;
            resultList[i + 6] = (value >>> 1) & 1;
            resultList[i + 7] = value & 1;
            i += 8;
        }
        int bitPos = decodePos * 8;
        while (i < numValues) {
            resultList[i++] = bytesToInt(encoded, bitPos, 1);
            bitPos++;
        }
        return (bitPos + 7) / 8;
    }

    private static int decodeBitPackingWidth2(
            byte[] encoded, int decodePos, int numValues, int[] resultList) {
        int i = 0;
        while (i + 4 <= numValues) {
            int value = encoded[decodePos++] & 0xFF;
            resultList[i] = (value >>> 6) & 3;
            resultList[i + 1] = (value >>> 4) & 3;
            resultList[i + 2] = (value >>> 2) & 3;
            resultList[i + 3] = value & 3;
            i += 4;
        }
        int bitPos = decodePos * 8;
        while (i < numValues) {
            resultList[i++] = bytesToInt(encoded, bitPos, 2);
            bitPos += 2;
        }
        return (bitPos + 7) / 8;
    }

    private static int decodeBitPackingWidth4(
            byte[] encoded, int decodePos, int numValues, int[] resultList) {
        int i = 0;
        while (i + 2 <= numValues) {
            int value = encoded[decodePos++] & 0xFF;
            resultList[i] = (value >>> 4) & 15;
            resultList[i + 1] = value & 15;
            i += 2;
        }
        if (i < numValues) {
            int bitPos = decodePos * 8;
            resultList[i] = bytesToInt(encoded, bitPos, 4);
            bitPos += 4;
            return (bitPos + 7) / 8;
        }
        return decodePos;
    }

    private static int decodeBitPackingWidth8(
            byte[] encoded, int decodePos, int numValues, int[] resultList) {
        for (int i = 0; i < numValues; i++) {
            resultList[i] = encoded[decodePos++] & 0xFF;
        }
        return decodePos;
    }

    private static int decodeBitPackingOrShifted(
            byte[] encoded,
            int decodePos,
            int bitWidth,
            int numValues,
            int[] output,
            int outputOffset,
            int shiftAmount,
            int[] fallbackBuffer) {
        if (bitWidth == 0) {
            return decodePos;
        }
        if (bitWidth == 1) {
            return decodeBitPackingWidth1OrShifted(encoded, decodePos, numValues, output,
                    outputOffset, shiftAmount);
        }
        if (bitWidth == 2) {
            return decodeBitPackingWidth2OrShifted(encoded, decodePos, numValues, output,
                    outputOffset, shiftAmount);
        }
        if (bitWidth == 4) {
            return decodeBitPackingWidth4OrShifted(encoded, decodePos, numValues, output,
                    outputOffset, shiftAmount);
        }
        if (bitWidth == 8) {
            return decodeBitPackingWidth8OrShifted(encoded, decodePos, numValues, output,
                    outputOffset, shiftAmount);
        }

        decodePos = decodeBitPacking(encoded, decodePos, bitWidth, numValues, fallbackBuffer);
        for (int i = 0; i < numValues; i++) {
            output[outputOffset + i] |= fallbackBuffer[i] << shiftAmount;
        }
        return decodePos;
    }

    private static int decodeBitPackingWidth1OrShifted(
            byte[] encoded, int decodePos, int numValues, int[] output, int outputOffset,
            int shiftAmount) {
        int i = 0;
        while (i + 8 <= numValues) {
            int value = encoded[decodePos++] & 0xFF;
            output[outputOffset + i] |= ((value >>> 7) & 1) << shiftAmount;
            output[outputOffset + i + 1] |= ((value >>> 6) & 1) << shiftAmount;
            output[outputOffset + i + 2] |= ((value >>> 5) & 1) << shiftAmount;
            output[outputOffset + i + 3] |= ((value >>> 4) & 1) << shiftAmount;
            output[outputOffset + i + 4] |= ((value >>> 3) & 1) << shiftAmount;
            output[outputOffset + i + 5] |= ((value >>> 2) & 1) << shiftAmount;
            output[outputOffset + i + 6] |= ((value >>> 1) & 1) << shiftAmount;
            output[outputOffset + i + 7] |= (value & 1) << shiftAmount;
            i += 8;
        }
        int bitPos = decodePos * 8;
        while (i < numValues) {
            output[outputOffset + i] |= bytesToInt(encoded, bitPos, 1) << shiftAmount;
            bitPos++;
            i++;
        }
        return (bitPos + 7) / 8;
    }

    private static int decodeBitPackingWidth2OrShifted(
            byte[] encoded, int decodePos, int numValues, int[] output, int outputOffset,
            int shiftAmount) {
        int i = 0;
        while (i + 4 <= numValues) {
            int value = encoded[decodePos++] & 0xFF;
            output[outputOffset + i] |= ((value >>> 6) & 3) << shiftAmount;
            output[outputOffset + i + 1] |= ((value >>> 4) & 3) << shiftAmount;
            output[outputOffset + i + 2] |= ((value >>> 2) & 3) << shiftAmount;
            output[outputOffset + i + 3] |= (value & 3) << shiftAmount;
            i += 4;
        }
        int bitPos = decodePos * 8;
        while (i < numValues) {
            output[outputOffset + i] |= bytesToInt(encoded, bitPos, 2) << shiftAmount;
            bitPos += 2;
            i++;
        }
        return (bitPos + 7) / 8;
    }

    private static int decodeBitPackingWidth4OrShifted(
            byte[] encoded, int decodePos, int numValues, int[] output, int outputOffset,
            int shiftAmount) {
        int i = 0;
        while (i + 2 <= numValues) {
            int value = encoded[decodePos++] & 0xFF;
            output[outputOffset + i] |= ((value >>> 4) & 15) << shiftAmount;
            output[outputOffset + i + 1] |= (value & 15) << shiftAmount;
            i += 2;
        }
        if (i < numValues) {
            int bitPos = decodePos * 8;
            output[outputOffset + i] |= bytesToInt(encoded, bitPos, 4) << shiftAmount;
            bitPos += 4;
            return (bitPos + 7) / 8;
        }
        return decodePos;
    }

    private static int decodeBitPackingWidth8OrShifted(
            byte[] encoded, int decodePos, int numValues, int[] output, int outputOffset,
            int shiftAmount) {
        for (int i = 0; i < numValues; i++) {
            output[outputOffset + i] |= (encoded[decodePos++] & 0xFF) << shiftAmount;
        }
        return decodePos;
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

    private static int countGroupedRunsAndDistinctUntilLimit(
            int[] values,
            int length,
            int shiftAmount,
            int mask,
            int distinctLimit,
            int[] out) {
        int previous = (values[0] >> shiftAmount) & mask;
        int seenMask = 1 << previous;
        int runs = 1;
        int distinctCount = 1;

        for (int i = 1; i < length; i++) {
            int current = (values[i] >> shiftAmount) & mask;
            if (current != previous) {
                runs++;
                previous = current;
            }
            int bit = 1 << current;
            if ((seenMask & bit) == 0) {
                seenMask |= bit;
                distinctCount++;
            }
        }

        out[0] = runs;
        out[1] = distinctCount >= distinctLimit ? distinctLimit : distinctCount;
        return distinctCount;
    }

    private static int betaLowerBound(
            int beta,
            int m,
            int xLength,
            int[] bpeCostSingle,
            int[] rleCostSingle,
            int[] deCostSingle) {
        int l = (m + beta - 1) / beta;
        int lowerBound = 0;
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

            int bpeCost = bpeCostSingle[betaStart] * (betaStart - groupStart + 1);
            int rleCostMax = 0;
            int deCostMax = 0;
            for (int j = groupStart; j < groupEnd; j++) {
                if (rleCostSingle[j] > rleCostMax) {
                    rleCostMax = rleCostSingle[j];
                }
                if (deCostSingle[j] > deCostMax) {
                    deCostMax = deCostSingle[j];
                }
            }
            lowerBound += Math.min(bpeCost, Math.min(rleCostMax, deCostMax));
        }
        return lowerBound;
    }

    private static int buildHybridBetaOrder(
            int m,
            int xLength,
            int[] bpeCostSingle,
            int[] rleCostSingle,
            int[] deCostSingle,
            EncodeScratch scratch) {
        int count = 0;
        for (int beta : BETA_LIST) {
            if (beta > m) {
                break;
            }
            scratch.betaCandidateOrder[count] = beta;
            scratch.betaCandidateLowerBound[count] =
                    betaLowerBound(beta, m, xLength, bpeCostSingle, rleCostSingle, deCostSingle);
            count++;
        }

        for (int i = 0; i < count; i++) {
            for (int j = i + 1; j < count; j++) {
                boolean preferJ = scratch.betaCandidateLowerBound[j]
                        < scratch.betaCandidateLowerBound[i];
                if (scratch.betaCandidateOrder[j] == scratch.lastBestBeta
                        && scratch.betaCandidateOrder[i] != scratch.lastBestBeta) {
                    preferJ = true;
                }
                if (preferJ) {
                    int betaTmp = scratch.betaCandidateOrder[i];
                    scratch.betaCandidateOrder[i] = scratch.betaCandidateOrder[j];
                    scratch.betaCandidateOrder[j] = betaTmp;

                    int lowerTmp = scratch.betaCandidateLowerBound[i];
                    scratch.betaCandidateLowerBound[i] = scratch.betaCandidateLowerBound[j];
                    scratch.betaCandidateLowerBound[j] = lowerTmp;
                }
            }
        }
        return count;
    }

    private static void extractAllGroups(
            EncodeScratch scratch,
            int[] x,
            int xLength,
            int beta,
            int m,
            int mask) {
        int l = (m + beta - 1) / beta;
        int[] flat = scratch.groupFlat;
        for (int i = 0; i < l; i++) {
            int shiftAmount = i * beta;
            int maxValuePart = 0;
            int base = i * xLength;
            for (int j = 0; j < xLength; j++) {
                int current = (x[j] >> shiftAmount) & mask;
                flat[base + j] = current;
                if (current > maxValuePart) {
                    maxValuePart = current;
                }
            }
            scratch.groupMax[i] = maxValuePart;
        }
        scratch.cachedBeta = beta;
        scratch.cachedL = l;
        scratch.cachedListLength = xLength;
    }

    private static boolean useGroupCache(EncodeScratch scratch, int betaValue, int l, int listLength) {
        return scratch.cachedBeta == betaValue
                && scratch.cachedL == l
                && scratch.cachedListLength == listLength;
    }

    public static int Subcolumn(int[] x, int xLength, int m, int blockSize, int[] encodingType) {
        return Subcolumn(x, xLength, m, blockSize, encodingType, ENCODE_SCRATCH.get());
    }

    private static int Subcolumn(
            int[] x,
            int xLength,
            int m,
            int blockSize,
            int[] encodingType,
            EncodeScratch scratch) {
        if (m == 0) {
            return 1;
        }

        int betaBest = 1;
        int[] bpeCostSingle = scratch.bpeCostSingle;
        int[] rleCostSingle = scratch.rleCostSingle;
        int[] deCostSingle = scratch.deCostSingle;
        int[] encodingTypeTemp = scratch.encodingTypeTemp;
        int[] groupStats = scratch.minDelta3;

        int[] threshold = blockSize == 512 ? THRESHOLD_512 : thresholdForBlockSize(blockSize);
        int lengthBitWidth = bitWidth(xLength);
        int cost1 = 0;

        Arrays.fill(rleCostSingle, 0, m, 1);
        int valueMask = m == Integer.SIZE ? -1 : (1 << m) - 1;
        int previousValue = x[0] & valueMask;
        int unionValue = previousValue;
        for (int j = 1; j < xLength; j++) {
            int currentValue = x[j] & valueMask;
            unionValue |= currentValue;
            int changedBits = previousValue ^ currentValue;
            while (changedBits != 0) {
                int changedBit = Integer.numberOfTrailingZeros(changedBits);
                rleCostSingle[changedBit]++;
                changedBits &= changedBits - 1;
            }
            previousValue = currentValue;
        }

        for (int i = 0; i < m; i++) {
            int runCount = rleCostSingle[i];

            bpeCostSingle[i] = ((unionValue >>> i) & 1) == 1 ? xLength : 0;
            rleCostSingle[i] = runCount * (1 + lengthBitWidth);
            deCostSingle[i] = runCount > 1 ? xLength * 2 + 2 : xLength + 2;

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

        int betaCandidateCount = USE_ALPHA_HYBRID
                ? buildHybridBetaOrder(m, xLength, bpeCostSingle, rleCostSingle, deCostSingle,
                        scratch)
                : BETA_LIST.length;
        if (USE_ALPHA_HYBRID) {
            Arrays.fill(scratch.betaCandidateCost, Integer.MAX_VALUE);
        }
        for (int betaCandidateIndex = 0; betaCandidateIndex < betaCandidateCount;
                betaCandidateIndex++) {
            int beta = USE_ALPHA_HYBRID
                    ? scratch.betaCandidateOrder[betaCandidateIndex]
                    : BETA_LIST[betaCandidateIndex];
            if (beta > m) {
                break;
            }
            if (USE_ALPHA_FAST_HYBRID
                    && scratch.betaCandidateLowerBound[betaCandidateIndex] >= cMin) {
                continue;
            }

            int l = (m + beta - 1) / beta;
            int cost = 0;
            int mask = (1 << beta) - 1;
            int betaThreshold = threshold[beta - 1];
            for (int t = 0; t < l; t++) {
                encodingTypeTemp[t] = 0;
            }

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

                int deCostMax = 0;
                for (int j = groupStart; j < groupEnd; j++) {
                    if (deCostSingle[j] > deCostMax) {
                        deCostMax = deCostSingle[j];
                    }
                }

                boolean needRle = rleCostMax < currentCost;
                boolean maybeNeedDe = deCostMax < currentCost;
                int groupedRunCount = -1;
                int groupedDistinctCount = -1;

                if (needRle && maybeNeedDe) {
                    countGroupedRunsAndDistinctUntilLimit(
                            x, xLength, groupStart, mask, betaThreshold, groupStats);
                    groupedRunCount = groupStats[0];
                    groupedDistinctCount = groupStats[1];
                }

                if (needRle) {
                    int runCount = groupedRunCount >= 0
                            ? groupedRunCount
                            : countGroupedRuns(x, xLength, groupStart, mask);
                    int rleCost = runCount * (beta + lengthBitWidth);
                    if (rleCost < currentCost) {
                        currentCost = rleCost;
                        encodingTypeTemp[i] = 1;
                    }
                }

                if (deCostMax < currentCost) {
                    int distinctCount = groupedDistinctCount >= 0
                            ? groupedDistinctCount
                            : countDistinctValuesUntilLimit(
                                    x, xLength, groupStart, mask, betaThreshold);
                    if (distinctCount < betaThreshold) {
                        int deCost = xLength * bitWidth(distinctCount) + distinctCount * beta;
                        if (deCost < currentCost) {
                            currentCost = deCost;
                            encodingTypeTemp[i] = 2;
                        }
                    }
                }

                cost += currentCost;
                int pruningLimit = USE_ALPHA_FAST_HYBRID ? cMin : (USE_ALPHA_HYBRID ? cost1 : cMin);
                if (cost >= pruningLimit) {
                    break;
                }
            }

            if (USE_ALPHA_FAST_HYBRID) {
                if (cost < cMin) {
                    cMin = cost;
                    betaBest = beta;
                    System.arraycopy(encodingTypeTemp, 0, encodingType, 0, l);
                }
            } else if (USE_ALPHA_HYBRID) {
                int candidateIndex = beta - BETA_LIST[0];
                scratch.betaCandidateCost[candidateIndex] = cost;
                if (cost < cost1) {
                    System.arraycopy(
                            encodingTypeTemp,
                            0,
                            scratch.betaCandidateEncodingType,
                            candidateIndex * 32,
                            l);
                }
            } else if (cost < cMin) {
                cMin = cost;
                betaBest = beta;
                System.arraycopy(encodingTypeTemp, 0, encodingType, 0, l);
            }
        }

        if (USE_ALPHA_HYBRID && !USE_ALPHA_FAST_HYBRID) {
            cMin = cost1;
            betaBest = 1;
            for (int beta : BETA_LIST) {
                if (beta > m) {
                    break;
                }
                int candidateIndex = beta - BETA_LIST[0];
                int cost = scratch.betaCandidateCost[candidateIndex];
                if (cost < cMin) {
                    int l = (m + beta - 1) / beta;
                    cMin = cost;
                    betaBest = beta;
                    System.arraycopy(
                            scratch.betaCandidateEncodingType,
                            candidateIndex * 32,
                            encodingType,
                            0,
                            l);
                }
            }
        }

        if (betaBest > 1) {
            extractAllGroups(scratch, x, xLength, betaBest, m, (1 << betaBest) - 1);
        }
        scratch.lastBestBeta = betaBest;

        return betaBest;
    }

    public static int SubcolumnEncoder(int[] list, int encodePos, byte[] encodedResult,
            int[] beta, int blockSize, int[] encodingType) {
        return SubcolumnEncoder(list, list.length, encodePos, encodedResult, beta, blockSize,
                encodingType, -1);
    }

    public static int SubcolumnEncoder(int[] list, int encodePos, byte[] encodedResult,
            int[] beta, int blockSize, int[] encodingType, int knownM) {
        return SubcolumnEncoder(list, list.length, encodePos, encodedResult, beta, blockSize,
                encodingType, knownM);
    }

    public static int SubcolumnEncoder(
            int[] list,
            int listLength,
            int encodePos,
            byte[] encodedResult,
            int[] beta,
            int blockSize,
            int[] encodingType,
            int knownM) {
        int m = knownM;
        if (m < 0) {
            int maxValue = 0;
            for (int i = 0; i < listLength; i++) {
                int value = list[i];
                if (value > maxValue) {
                    maxValue = value;
                }
            }
            m = bitWidth(maxValue);
        }
        return SubcolumnEncoder(list, listLength, encodePos, encodedResult, beta, blockSize,
                encodingType, m, ENCODE_SCRATCH.get());
    }

    private static int encodeRleRuns(
            int[] runLength,
            int[] rleValues,
            int runCount,
            int runLengthBitWidth,
            int valueBitWidth,
            int encodePos,
            byte[] encodedResult) {
        encodedResult[encodePos] = (byte) (runCount >> 8);
        encodePos += 1;
        encodedResult[encodePos] = (byte) (runCount & 0xFF);
        encodePos += 1;

        encodePos = bitPacking(runLength, runLengthBitWidth, encodePos, encodedResult, runCount);
        return bitPacking(rleValues, valueBitWidth, encodePos, encodedResult, runCount);
    }

    private static int encodeRleFromValues(
            int[] values,
            int offset,
            int listLength,
            int[] runLength,
            int[] rleValues,
            int runLengthBitWidth,
            int valueBitWidth,
            int encodePos,
            byte[] encodedResult) {
        int previous = values[offset];
        int runCount = 0;

        for (int j = 1; j < listLength; j++) {
            int current = values[offset + j];
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

        return encodeRleRuns(runLength, rleValues, runCount, runLengthBitWidth, valueBitWidth,
                encodePos, encodedResult);
    }

    private static int encodeRleShifted(
            int[] list,
            int listLength,
            int shiftAmount,
            int mask,
            int[] runLength,
            int[] rleValues,
            int runLengthBitWidth,
            int valueBitWidth,
            int encodePos,
            byte[] encodedResult) {
        int previous = (list[0] >> shiftAmount) & mask;
        int runCount = 0;

        for (int j = 1; j < listLength; j++) {
            int current = (list[j] >> shiftAmount) & mask;
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

        return encodeRleRuns(runLength, rleValues, runCount, runLengthBitWidth, valueBitWidth,
                encodePos, encodedResult);
    }

    public static int[] borrowMinDelta3Buffer() {
        return ENCODE_SCRATCH.get().minDelta3;
    }

    public static int[] borrowEncodingTypeBuffer() {
        return ENCODE_SCRATCH.get().encodingType;
    }

    public static int[] borrowDataDeltaBuffer() {
        return ENCODE_SCRATCH.get().dataDelta;
    }

    private static int SubcolumnEncoder(
            int[] list,
            int listLength,
            int encodePos,
            byte[] encodedResult,
            int[] beta,
            int blockSize,
            int[] encodingType,
            int m,
            EncodeScratch scratch) {
        intByte2Bytes(m, encodePos, encodedResult);
        encodePos += 1;

        if (m == 0) {
            return encodePos;
        }

        int betaValue = beta[0];
        int l = (m + betaValue - 1) / betaValue;
        int[] bitWidthList = scratch.bitWidthList;
        int[] subcolumnBuffer = scratch.subcolumnBuffer;
        int[] runLength = scratch.runLength;
        int[] rleValues = scratch.rleValues;
        int[] dictKeyList = scratch.dictKeyList;
        int[] codeMap = scratch.codeMap;

        intByte2Bytes(betaValue, encodePos, encodedResult);
        encodePos += 1;

        int bw = bitWidth(blockSize);
        int mask = (1 << betaValue) - 1;
        boolean useCache = useGroupCache(scratch, betaValue, l, listLength);

        if (useCache) {
            for (int i = 0; i < l; i++) {
                bitWidthList[i] = bitWidth(scratch.groupMax[i]);
            }
        } else if (betaValue == 1) {
            int unionValue = 0;
            for (int j = 0; j < listLength; j++) {
                unionValue |= list[j];
            }
            for (int i = 0; i < l; i++) {
                bitWidthList[i] = (unionValue >>> i) & 1;
            }
        } else {
            for (int i = 0; i < l; i++) {
                int shiftAmount = i * betaValue;
                int maxValuePart = 0;
                for (int j = 0; j < listLength; j++) {
                    int current = (list[j] >> shiftAmount) & mask;
                    if (current > maxValuePart) {
                        maxValuePart = current;
                    }
                }
                bitWidthList[i] = bitWidth(maxValuePart);
            }
        }

        encodePos = bitPacking(bitWidthList, 8, encodePos, encodedResult, l);

        int preTypePos = encodePos;
        encodePos += (l + 3) / 4;

        for (int i = 0; i < l; i++) {
            int shiftAmount = i * betaValue;
            int groupOffset = i * listLength;

            if (encodingType[i] == 0) {
                if (useCache) {
                    encodePos = bitPackingAt(scratch.groupFlat, groupOffset, bitWidthList[i],
                            encodePos, encodedResult, listLength);
                } else {
                    encodePos = bitPackingShifted(list, listLength, shiftAmount, mask,
                            bitWidthList[i], encodePos, encodedResult, subcolumnBuffer);
                }
                continue;
            }

            if (encodingType[i] == 1) {
                if (useCache) {
                    encodePos = encodeRleFromValues(scratch.groupFlat, groupOffset, listLength,
                            runLength, rleValues, bw, bitWidthList[i], encodePos, encodedResult);
                } else {
                    encodePos = encodeRleShifted(list, listLength, shiftAmount, mask, runLength,
                            rleValues, bw, bitWidthList[i], encodePos, encodedResult);
                }
                continue;
            }

            int seenMask = 0;
            if (useCache) {
                System.arraycopy(scratch.groupFlat, groupOffset, subcolumnBuffer, 0, listLength);
                for (int j = 0; j < listLength; j++) {
                    seenMask |= 1 << subcolumnBuffer[j];
                }
            } else {
                for (int j = 0; j < listLength; j++) {
                    int current = (list[j] >> shiftAmount) & mask;
                    subcolumnBuffer[j] = current;
                    seenMask |= 1 << current;
                }
            }

            int cardinality = Integer.bitCount(seenMask);
            int dictBitWidth = bitWidth(cardinality);
            int dictSize = 0;
            for (int value = 0; value <= mask; value++) {
                if ((seenMask & (1 << value)) != 0) {
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
        }

        bitPacking(encodingType, 2, preTypePos, encodedResult, l);
        return encodePos;
    }

    public static int SubcolumnDecoder(byte[] encodedResult, int encodePos, int[] list,
            int blockSize) {
        return SubcolumnDecoder(encodedResult, encodePos, list, 0, list.length, blockSize);
    }

    private static int SubcolumnDecoder(byte[] encodedResult, int encodePos, int[] list,
            int outputOffset, int listLength, int blockSize) {
        int m = bytes2Integer(encodedResult, encodePos, 1);
        encodePos += 1;

        if (m == 0) {
            return encodePos;
        }

        int bw = bitWidth(blockSize);
        int beta = bytes2Integer(encodedResult, encodePos, 1);
        encodePos += 1;

        int l = (m + beta - 1) / beta;
        DecodeScratch scratch = DECODE_SCRATCH.get();
        scratch.ensureL(l);
        scratch.ensureListLength(listLength);

        int[] bitWidthList = scratch.bitWidthList;
        encodePos = decodeBitPacking(encodedResult, encodePos, 8, l, bitWidthList);

        int[] encodingType = scratch.encodingType;
        encodePos = decodeBitPacking(encodedResult, encodePos, 2, l, encodingType);

        int[] subcolumnBuffer = scratch.subcolumnBuffer;
        int[] runLength = scratch.runLength;
        int[] rleValues = scratch.rleValues;

        for (int i = 0; i < l; i++) {
            int type = encodingType[i];
            int currentBitWidth = bitWidthList[i];
            int shiftAmount = i * beta;

            if (type == 0) {
                encodePos = decodeBitPackingOrShifted(encodedResult, encodePos, currentBitWidth,
                        listLength, list, outputOffset, shiftAmount, subcolumnBuffer);
                continue;
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
                    int value = rleValues[j] << shiftAmount;
                    int outputBase = outputOffset + currentIndex;
                    for (int k = currentIndex; k < endPos; k++) {
                        list[outputBase++] |= value;
                    }
                    currentIndex = endPos;
                }
                continue;
            } else {
                int cardinality = ((encodedResult[encodePos] & 0xFF) << 8)
                        | (encodedResult[encodePos + 1] & 0xFF);
                encodePos += 2;

                int dictBitWidth = bitWidth(cardinality);
                int[] dictKeyList = scratch.ensureDict(cardinality);
                encodePos = decodeBitPacking(encodedResult, encodePos, currentBitWidth,
                        cardinality, dictKeyList);
                encodePos = decodeBitPacking(encodedResult, encodePos, dictBitWidth, listLength,
                        subcolumnBuffer);

                for (int j = 0; j < listLength; j++) {
                    list[outputOffset + j] |= dictKeyList[subcolumnBuffer[j]] << shiftAmount;
                }
                continue;
            }
        }

        return encodePos;
    }

    public static int[] getAbsDeltaTsBlock(int[] tsBlock, int blockIndex, int blockSize,
            int remaining, int[] minDelta) {
        int[] out = new int[remaining];
        fillAbsDeltaTsBlock(tsBlock, blockIndex, blockSize, remaining, minDelta, out);
        return out;
    }

    private static int fillAbsDeltaTsBlock(
            int[] tsBlock,
            int blockIndex,
            int blockSize,
            int remaining,
            int[] minDelta,
            int[] out) {
        int valueDeltaMin = Integer.MAX_VALUE;
        int valueDeltaMax = Integer.MIN_VALUE;
        int base = blockIndex * blockSize;
        int end = base + remaining;

        for (int j = base; j < end; j++) {
            int current = tsBlock[j];
            if (current < valueDeltaMin) {
                valueDeltaMin = current;
            }
            if (current > valueDeltaMax) {
                valueDeltaMax = current;
            }
        }

        for (int j = base; j < end; j++) {
            out[j - base] = tsBlock[j] - valueDeltaMin;
        }

        minDelta[0] = valueDeltaMin;
        return valueDeltaMax - valueDeltaMin;
    }

    public static int BlockEncoder(int[] data, int blockIndex, int blockSize, int remainder,
            int encodePos, byte[] encodedResult, int[] beta) {
        return BlockEncoder(data, blockIndex, blockSize, remainder, encodePos, encodedResult, beta,
                null, null);
    }

    public static int BlockEncoder(int[] data, int blockIndex, int blockSize, int remainder,
            int encodePos, byte[] encodedResult, int[] beta, long[] forTime,
            long[] subcolumnTime) {
        EncodeScratch scratch = ENCODE_SCRATCH.get();
        long forStart = System.nanoTime();
        int maxValue = fillAbsDeltaTsBlock(
                data, blockIndex, blockSize, remainder, scratch.minDelta, scratch.dataDelta);
        long forEnd = System.nanoTime();
        if (forTime != null) {
            forTime[0] += (forEnd - forStart);
        }

        long subStart = System.nanoTime();
        int2Bytes(scratch.minDelta[0], encodePos, encodedResult);
        encodePos += 4;

        int m = bitWidth(maxValue);
        beta[0] = Subcolumn(scratch.dataDelta, remainder, m, blockSize, scratch.encodingType, scratch);
        encodePos = SubcolumnEncoder(scratch.dataDelta, remainder, encodePos, encodedResult, beta,
                blockSize, scratch.encodingType, m, scratch);
        long subEnd = System.nanoTime();
        if (subcolumnTime != null) {
            subcolumnTime[0] += (subEnd - subStart);
        }
        return encodePos;
    }

    public static int BlockDecoder(byte[] encodedResult, int blockIndex, int blockSize,
            int remainder, int encodePos, int[] data) {
        int minDelta = bytes2Integer(encodedResult, encodePos, 4);
        encodePos += 4;

        int base = blockIndex * blockSize;
        encodePos = SubcolumnDecoder(encodedResult, encodePos, data, base, remainder, blockSize);

        for (int i = 0; i < remainder; i++) {
            data[base + i] += minDelta;
        }

        return encodePos;
    }

    public static int Encoder(int[] data, int blockSize, byte[] encodedResult) {
        return Encoder(data, blockSize, encodedResult, null, null);
    }

    private static void resetAlphaHybridState() {
        EncodeScratch scratch = ENCODE_SCRATCH.get();
        scratch.lastBestBeta = 2;
        scratch.cachedBeta = -1;
        scratch.cachedL = 0;
        scratch.cachedListLength = -1;
    }

    public static int EncoderHybridAlpha(int[] data, int blockSize, byte[] encodedResult) {
        return EncoderHybridAlpha(data, blockSize, encodedResult, null, null);
    }

    public static int EncoderHybridAlpha(int[] data, int blockSize, byte[] encodedResult,
            long[] forTime, long[] subcolumnTime) {
        boolean previous = USE_ALPHA_HYBRID;
        boolean previousFast = USE_ALPHA_FAST_HYBRID;
        USE_ALPHA_HYBRID = true;
        USE_ALPHA_FAST_HYBRID = false;
        resetAlphaHybridState();
        try {
            return Encoder(data, blockSize, encodedResult, forTime, subcolumnTime);
        } finally {
            USE_ALPHA_HYBRID = previous;
            USE_ALPHA_FAST_HYBRID = previousFast;
        }
    }

    public static int EncoderFastHybridAlpha(int[] data, int blockSize, byte[] encodedResult) {
        return EncoderFastHybridAlpha(data, blockSize, encodedResult, null, null);
    }

    public static int EncoderFastHybridAlpha(int[] data, int blockSize, byte[] encodedResult,
            long[] forTime, long[] subcolumnTime) {
        boolean previous = USE_ALPHA_HYBRID;
        boolean previousFast = USE_ALPHA_FAST_HYBRID;
        USE_ALPHA_HYBRID = true;
        USE_ALPHA_FAST_HYBRID = true;
        resetAlphaHybridState();
        try {
            return Encoder(data, blockSize, encodedResult, forTime, subcolumnTime);
        } finally {
            USE_ALPHA_HYBRID = previous;
            USE_ALPHA_FAST_HYBRID = previousFast;
        }
    }

    public static int Encoder(int[] data, int blockSize, byte[] encodedResult, long[] forTime,
            long[] subcolumnTime) {
        int dataLength = data.length;
        int encodePos = 0;

        int2Bytes(dataLength, encodePos, encodedResult);
        encodePos += 4;

        int2Bytes(blockSize, encodePos, encodedResult);
        encodePos += 4;

        int numBlocks = dataLength / blockSize;
        int remainder = dataLength % blockSize;
        int[] beta = ENCODE_SCRATCH.get().beta;
        beta[0] = 2;

        for (int i = 0; i < numBlocks; i++) {
            encodePos = BlockEncoder(data, i, blockSize, blockSize, encodePos, encodedResult,
                    beta, forTime, subcolumnTime);
        }

        if (remainder <= 3) {
            int base = numBlocks * blockSize;
            for (int i = 0; i < remainder; i++) {
                int2Bytes(data[base + i], encodePos, encodedResult);
                encodePos += 4;
            }
        } else {
            encodePos = BlockEncoder(data, numBlocks, blockSize, remainder, encodePos,
                    encodedResult, beta, forTime, subcolumnTime);
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

    private static int[] loadCsvAsScaledInts(File file) throws IOException {
        InputStream inputStream = Files.newInputStream(file.toPath());
        CsvReader loader = new CsvReader(inputStream, StandardCharsets.UTF_8);
        ArrayList<Float> data = new ArrayList<>();

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
            data.add(Float.valueOf(fStr));
        }
        inputStream.close();

        int[] result = new int[data.size()];
        int maxMul = (int) Math.pow(10, maxDecimal);
        for (int i = 0; i < data.size(); i++) {
            result[i] = (int) (data.get(i) * maxMul);
        }
        return result;
    }

    @Test
    public void benchmarkAlphaHybrid() throws IOException {
        String inputParentDir = "/Users/xiaojinzhao/Documents/GitHub/subcolumn/dataset/";
        int blockSize = 512;
        int warmupTime = 3;
        int repeatTime = 30;

        File directory = new File(inputParentDir);
        File[] csvFiles = directory.listFiles((dir, name) -> name.endsWith(".csv"));
        if (csvFiles == null) {
            return;
        }
        Arrays.sort(csvFiles);

        System.out.println(
                "Dataset,Points,BaselineSize,HybridSize,BaselineRatio,HybridRatio,"
                        + "BaselineNsPerPoint,HybridNsPerPoint,TimeChangePct,SameSize");
        for (File file : csvFiles) {
            int[] data = loadCsvAsScaledInts(file);
            byte[] baselineEncoded = new byte[Math.max(16, data.length * 8)];
            byte[] hybridEncoded = new byte[Math.max(16, data.length * 8)];

            int baselineLength = 0;
            int hybridLength = 0;
            for (int i = 0; i < warmupTime; i++) {
                baselineLength = Encoder(data, blockSize, baselineEncoded);
                hybridLength = EncoderHybridAlpha(data, blockSize, hybridEncoded);
            }

            long baselineTime = 0;
            long hybridTime = 0;
            for (int i = 0; i < repeatTime; i++) {
                if ((i & 1) == 0) {
                    long start = System.nanoTime();
                    baselineLength = Encoder(data, blockSize, baselineEncoded);
                    baselineTime += System.nanoTime() - start;

                    start = System.nanoTime();
                    hybridLength = EncoderHybridAlpha(data, blockSize, hybridEncoded);
                    hybridTime += System.nanoTime() - start;
                } else {
                    long start = System.nanoTime();
                    hybridLength = EncoderHybridAlpha(data, blockSize, hybridEncoded);
                    hybridTime += System.nanoTime() - start;

                    start = System.nanoTime();
                    baselineLength = Encoder(data, blockSize, baselineEncoded);
                    baselineTime += System.nanoTime() - start;
                }
            }
            baselineTime /= repeatTime;
            hybridTime /= repeatTime;

            Assert.assertArrayEquals(
                    data, Decoder(Arrays.copyOf(hybridEncoded, hybridLength)));

            double baselineRatio = baselineLength / (double) (Math.max(1, data.length) * Long.BYTES);
            double hybridRatio = hybridLength / (double) (Math.max(1, data.length) * Long.BYTES);
            double baselineNsPerPoint = baselineTime / (double) Math.max(1, data.length);
            double hybridNsPerPoint = hybridTime / (double) Math.max(1, data.length);
            double timeChangePct = (hybridNsPerPoint - baselineNsPerPoint)
                    / Math.max(1.0e-9, baselineNsPerPoint) * 100.0;

            System.out.println(
                    extractFileName(file.toString())
                            + ","
                            + data.length
                            + ","
                            + baselineLength
                            + ","
                            + hybridLength
                            + ","
                            + baselineRatio
                            + ","
                            + hybridRatio
                            + ","
                            + baselineNsPerPoint
                            + ","
                            + hybridNsPerPoint
                            + ","
                            + timeChangePct
                            + ","
                            + (baselineLength == hybridLength));
        }
    }

    @Test
    public void benchmarkFastAlphaHybrid() throws IOException {
        String inputParentDir = "/Users/xiaojinzhao/Documents/GitHub/subcolumn/dataset/";
        int blockSize = 512;
        int warmupTime = 3;
        int repeatTime = 30;

        File directory = new File(inputParentDir);
        File[] csvFiles = directory.listFiles((dir, name) -> name.endsWith(".csv"));
        if (csvFiles == null) {
            return;
        }
        Arrays.sort(csvFiles);

        System.out.println(
                "Dataset,Points,HybridSize,FastSize,HybridRatio,FastRatio,"
                        + "HybridNsPerPoint,FastNsPerPoint,FastVsHybridTimePct,"
                        + "FastVsHybridSizeDelta,FastNoWorseRatio,FastFaster");
        for (File file : csvFiles) {
            int[] data = loadCsvAsScaledInts(file);
            byte[] hybridEncoded = new byte[Math.max(16, data.length * 8)];
            byte[] fastEncoded = new byte[Math.max(16, data.length * 8)];

            int hybridLength = 0;
            int fastLength = 0;
            for (int i = 0; i < warmupTime; i++) {
                hybridLength = EncoderHybridAlpha(data, blockSize, hybridEncoded);
                fastLength = EncoderFastHybridAlpha(data, blockSize, fastEncoded);
            }

            long start = System.nanoTime();
            for (int i = 0; i < repeatTime; i++) {
                hybridLength = EncoderHybridAlpha(data, blockSize, hybridEncoded);
            }
            long hybridTime = (System.nanoTime() - start) / repeatTime;

            start = System.nanoTime();
            for (int i = 0; i < repeatTime; i++) {
                fastLength = EncoderFastHybridAlpha(data, blockSize, fastEncoded);
            }
            long fastTime = (System.nanoTime() - start) / repeatTime;

            Assert.assertArrayEquals(data, Decoder(Arrays.copyOf(fastEncoded, fastLength)));

            double hybridRatio = hybridLength / (double) (Math.max(1, data.length) * Long.BYTES);
            double fastRatio = fastLength / (double) (Math.max(1, data.length) * Long.BYTES);
            double hybridNsPerPoint = hybridTime / (double) Math.max(1, data.length);
            double fastNsPerPoint = fastTime / (double) Math.max(1, data.length);
            double timeChangePct = (fastNsPerPoint - hybridNsPerPoint)
                    / Math.max(1.0e-9, hybridNsPerPoint) * 100.0;

            System.out.println(
                    extractFileName(file.toString())
                            + ","
                            + data.length
                            + ","
                            + hybridLength
                            + ","
                            + fastLength
                            + ","
                            + hybridRatio
                            + ","
                            + fastRatio
                            + ","
                            + hybridNsPerPoint
                            + ","
                            + fastNsPerPoint
                            + ","
                            + timeChangePct
                            + ","
                            + (fastLength - hybridLength)
                            + ","
                            + (fastLength <= hybridLength)
                            + ","
                            + (fastNsPerPoint < hybridNsPerPoint));
        }
    }

    @Test
    public void test0() throws IOException {
        String parentDir = "/Users/xiaojinzhao/Documents/GitHub/subcolumn/";

        String inputParentDir = parentDir + "dataset/";
        String outputParentDir = parentDir + "result/";
        String outputPath = outputParentDir + "subcolumn_adddict_prunenew_opt2.csv";

        int blockSize = 512;
        int repeatTime = 100;

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
                    "Compression Ratio",
                    "For Time",
                    "Subcolumn Encode Time"
                });

        File directory = new File(inputParentDir);
        File[] csvFiles = directory.listFiles((dir, name) -> name.endsWith(".csv"));
        if (csvFiles == null) {
            writer.close();
            return;
        }

        for (File file : csvFiles) {
            String datasetName = extractFileName(file.toString());
            System.out.println(datasetName);

            InputStream inputStream = Files.newInputStream(file.toPath());
            CsvReader loader = new CsvReader(inputStream, StandardCharsets.UTF_8);
            ArrayList<Float> data1 = new ArrayList<>();

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
                data1.add(Float.valueOf(fStr));
            }
            inputStream.close();

            int[] data2Arr = new int[data1.size()];
            int maxMul = (int) Math.pow(10, maxDecimal);
            for (int i = 0; i < data1.size(); i++) {
                data2Arr[i] = (int) (data1.get(i) * maxMul);
            }

            byte[] encodedResult = new byte[Math.max(16, data2Arr.length * 8)];
            long encodeTime = 0;
            long decodeTime = 0;
            long forTime = 0;
            long subcolumnEncodeTime = 0;
            double compressedSize = 0;
            int length = 0;
            long[] forTimeArr = new long[1];
            long[] subcolumnEncodeTimeArr = new long[1];

            long s = System.nanoTime();
            for (int repeat = 0; repeat < repeatTime; repeat++) {
                forTimeArr[0] = 0;
                subcolumnEncodeTimeArr[0] = 0;
                length = Encoder(data2Arr, blockSize, encodedResult, forTimeArr,
                        subcolumnEncodeTimeArr);
                forTime += forTimeArr[0];
                subcolumnEncodeTime += subcolumnEncodeTimeArr[0];
            }
            long e = System.nanoTime();
            encodeTime += (e - s) / repeatTime;
            forTime /= repeatTime;
            subcolumnEncodeTime /= repeatTime;
            compressedSize += length;

            s = System.nanoTime();
            for (int repeat = 0; repeat < repeatTime; repeat++) {
                Decoder(encodedResult);
            }
            e = System.nanoTime();
            decodeTime += (e - s) / repeatTime;

            double ratio = compressedSize / (double) (Math.max(1, data1.size()) * Long.BYTES);
            writer.writeRecord(
                    new String[] {
                        datasetName,
                        "Sub-columns(AddDictPruneNew-Opt2)",
                        String.valueOf(encodeTime),
                        String.valueOf(decodeTime),
                        String.valueOf(data1.size()),
                        String.valueOf(compressedSize),
                        String.valueOf(ratio),
                        String.valueOf(forTime),
                        String.valueOf(subcolumnEncodeTime)
                    });
            System.out.println(ratio);
        }

        writer.close();
    }

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
    public void testRoundTripAfterOpt3() {
        int blockSize = 512;
        int[][] patterns = {
            syntheticData(2048, 1),
            syntheticData(4096, 7),
            syntheticData(8192, 42)
        };
        byte[] encoded = new byte[65536];
        for (int[] data : patterns) {
            int len = Encoder(data, blockSize, encoded);
            int[] decoded = Decoder(Arrays.copyOf(encoded, len));
            Assert.assertArrayEquals(data, decoded);
        }
    }

}
