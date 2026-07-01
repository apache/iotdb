package org.apache.iotdb.tsfile.encoding;

import java.util.Arrays;

public final class SubcolumnAblationCodecSupport {

    private static final ThreadLocal<EncodeScratch> ENCODE_SCRATCH =
            ThreadLocal.withInitial(EncodeScratch::new);

    private static final ThreadLocal<DecodeScratch> DECODE_SCRATCH =
            ThreadLocal.withInitial(DecodeScratch::new);

    public static final class EncodeScratch {
        public final int[] dataDelta = new int[8192];
        public final int[] bitWidthList = new int[32];
        public final int[] subcolumnBuffer = new int[8192];
        public final int[] runLength = new int[8192];
        public final int[] rleValues = new int[8192];
        public final int[] dictKeyList = new int[16];
        public final int[] codeMap = new int[16];
        public final int[] minDelta = new int[1];
        public final int[] beta = new int[1];
    }

    public static final class DecodeScratch {
        public int[] bitWidthList = new int[32];
        public int[] encodingType = new int[32];
        public int[] subcolumnBuffer = new int[8192];
        public int[] runLength = new int[8192];
        public int[] rleValues = new int[8192];
        public int[] dictKeyList = new int[16];

        public void ensureL(int l) {
            if (bitWidthList.length < l) {
                bitWidthList = new int[l];
                encodingType = new int[l];
            }
        }

        public void ensureListLength(int listLength) {
            if (subcolumnBuffer.length < listLength) {
                subcolumnBuffer = new int[listLength];
                runLength = new int[listLength];
                rleValues = new int[listLength];
            }
        }

        public int[] ensureDict(int cardinality) {
            if (dictKeyList.length < cardinality) {
                dictKeyList = new int[cardinality];
            }
            return dictKeyList;
        }
    }

    private SubcolumnAblationCodecSupport() {}

    public static EncodeScratch encodeScratch() {
        return ENCODE_SCRATCH.get();
    }

    public static DecodeScratch decodeScratch() {
        return DECODE_SCRATCH.get();
    }

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

    public static int bitPackingAt(int[] numbers, int offset, int bitWidth, int encodePos,
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

    public static int bitPackingWidth1At(int[] numbers, int offset, int encodePos,
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

    public static int bitPackingWidth2At(int[] numbers, int offset, int encodePos,
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

    public static int bitPackingWidth4At(int[] numbers, int offset, int encodePos,
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

    public static int bitPackingWidth8At(int[] numbers, int offset, int encodePos,
            byte[] encodedResult, int numValues) {
        for (int i = 0; i < numValues; i++) {
            encodedResult[encodePos++] = (byte) numbers[offset + i];
        }
        return encodePos;
    }

    public static int bitPackingShifted(int[] list, int listLength, int shiftAmount, int mask,
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

    public static int bitPackingWidth1Shifted(int[] list, int listLength, int shiftAmount,
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

    public static int bitPackingWidth2Shifted(int[] list, int listLength, int shiftAmount,
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

    public static int bitPackingWidth4Shifted(int[] list, int listLength, int shiftAmount,
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

    public static int bitPackingWidth8Shifted(int[] list, int listLength, int shiftAmount,
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

    public static int decodeBitPackingWidth1(
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

    public static int decodeBitPackingWidth2(
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

    public static int decodeBitPackingWidth4(
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

    public static int decodeBitPackingWidth8(
            byte[] encoded, int decodePos, int numValues, int[] resultList) {
        for (int i = 0; i < numValues; i++) {
            resultList[i] = encoded[decodePos++] & 0xFF;
        }
        return decodePos;
    }

    public static int decodeBitPackingOrShifted(
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

    public static int decodeBitPackingWidth1OrShifted(
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

    public static int decodeBitPackingWidth2OrShifted(
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

    public static int decodeBitPackingWidth4OrShifted(
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

    public static int decodeBitPackingWidth8OrShifted(
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

    public static int bytes2Integer(byte[] encoded, int start, int num) {
        int value = 0;

        for (int i = 0; i < num; i++) {
            value <<= 8;
            int b = encoded[i + start] & 0xFF;
            value |= b;
        }
        return value;
    }

    public static int encodeRleRuns(
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

    public static int encodeRleFromValues(
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

    public static int encodeRleShifted(
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

    public static int fillAbsDeltaTsBlock(
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
}
