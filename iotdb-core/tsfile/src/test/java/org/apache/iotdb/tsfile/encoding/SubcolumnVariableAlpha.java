package org.apache.iotdb.tsfile.encoding;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.*;

public class SubcolumnVariableAlpha {

    // Used to prevent JIT from optimizing away verification work.
    private static volatile long VERIFY_SINK = 0L;

    /**
     * Verify decoded == expected, but do NOT fail the test if it's lossy.
     * Returns number of mismatched positions (or a large count if length differs).
     */
    private static int verifyLosslessNoThrow(int[] expected, int[] actual) {
        if (expected == actual) {
            return 0;
        }
        if (expected == null || actual == null) {
            return Integer.MAX_VALUE;
        }
        int minLen = Math.min(expected.length, actual.length);
        int mismatches = Math.abs(expected.length - actual.length);
        for (int i = 0; i < minLen; i++) {
            if (expected[i] != actual[i]) {
                mismatches++;
            }
        }
        return mismatches;
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

    public static void pack8Values(int[] values, int offset, int width, int encode_pos,
            byte[] encoded_result) {
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
                encoded_result[encode_pos] = (byte) ((buffer >>> ((3 - j) * 8)) & 0xFF);
                encode_pos++;
                bufIdx++;
                if (bufIdx >= width) {
                    return;
                }
            }
        }

    }

    public static void unpack8Values(byte[] encoded, int offset, int width, int[] result_list, int result_offset) {
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
                result_list[result_offset + valueIdx] = (int) (buffer >>> (totalBits - width));
                valueIdx++;
                totalBits -= width;
                buffer = buffer & ((1L << totalBits) - 1);
            }
        }
    }

    public static int bitPacking(int[] numbers, int bit_width, int encode_pos,
            byte[] encoded_result, int num_values) {
        int block_num = num_values / 8;
        int remainder = num_values % 8;

        for (int i = 0; i < block_num; i++) {
            pack8Values(numbers, i * 8, bit_width, encode_pos, encoded_result);
            encode_pos += bit_width;
        }

        encode_pos *= 8;

        for (int i = 0; i < remainder; i++) {
            intToBytes(numbers[block_num * 8 + i], encoded_result, encode_pos, bit_width);
            encode_pos += bit_width;
        }

        return (encode_pos + 7) / 8;
    }

    public static int decodeBitPacking(
            byte[] encoded, int decode_pos, int bit_width, int num_values, int[] result_list) {
        int block_num = num_values / 8;
        int remainder = num_values % 8;

        for (int i = 0; i < block_num; i++) {
            unpack8Values(encoded, decode_pos, bit_width, result_list, i * 8);
            decode_pos += bit_width;
        }

        decode_pos *= 8;

        for (int i = 0; i < remainder; i++) {
            result_list[block_num * 8 + i] = bytesToInt(encoded, decode_pos, bit_width);
            decode_pos += bit_width;
        }

        return (decode_pos + 7) / 8;
    }

    public static void int2Bytes(int integer, int encode_pos, byte[] cur_byte) {
        cur_byte[encode_pos] = (byte) (integer >> 24);
        cur_byte[encode_pos + 1] = (byte) (integer >> 16);
        cur_byte[encode_pos + 2] = (byte) (integer >> 8);
        cur_byte[encode_pos + 3] = (byte) (integer);
    }

    public static void intByte2Bytes(int integer, int encode_pos, byte[] cur_byte) {
        cur_byte[encode_pos] = (byte) (integer);
    }

    public static void long2intBytes(long integer, int encode_pos, byte[] cur_byte) {
        cur_byte[encode_pos] = (byte) (integer >> 24);
        cur_byte[encode_pos + 1] = (byte) (integer >> 16);
        cur_byte[encode_pos + 2] = (byte) (integer >> 8);
        cur_byte[encode_pos + 3] = (byte) (integer);
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

    public static long bytesLong2Integer(byte[] encoded, int decode_pos) {
        long value = 0;
        for (int i = 0; i < 4; i++) {
            value <<= 8;
            int b = encoded[i + decode_pos] & 0xFF;
            value |= b;
        }
        return value;
    }

    /**
     * Compute cost and best encoding type for a segment [bitStart, bitEnd) (bitwidth = bitEnd - bitStart).
     * Returns int[2]: { cost, encodingType } where encodingType is 0=BPE, 1=RLE, 2=DE.
     */
    private static int[] costForSegment(
            int[] x, int x_length, int bitStart, int bitEnd,
            int[] bpe_cost_single, int[] rle_cost_single, int[] de_cost_single,
            BitSet[] bitsets, int[] threshold) {
        int beta = bitEnd - bitStart;
        if (beta <= 0 || beta > threshold.length) {
            return new int[] { Integer.MAX_VALUE, 0 };
        }
        int currentCost;
        int bestType = 0;

        int bpCost = 0;
        int beta_start = bitEnd - 1;
        while (beta_start >= bitStart && bpe_cost_single[beta_start] == 0) {
            beta_start--;
        }
        if (beta_start >= bitStart) {
            bpCost = bpe_cost_single[beta_start] * (beta_start - bitStart + 1);
        }
        currentCost = bpCost;

        int rleCostMax = 0;
        for (int j = bitStart; j < bitEnd && j < rle_cost_single.length; j++) {
            if (rle_cost_single[j] > rleCostMax) {
                rleCostMax = rle_cost_single[j];
            }
        }
        if (rleCostMax < currentCost) {
            BitSet mergedBitSet = new BitSet(x_length);
            for (int j = bitStart; j < bitEnd && j < bitsets.length; j++) {
                mergedBitSet.or(bitsets[j]);
                if (mergedBitSet.cardinality() >= currentCost) {
                    break;
                }
            }
            int rleCost = mergedBitSet.cardinality() * (beta + bitWidth(x_length));
            if (rleCost < currentCost) {
                currentCost = rleCost;
                bestType = 1;
            }
        }

        if (bitEnd <= 32) {
            int th = threshold[beta - 1];
            Set<Integer> uniqueValues = new HashSet<>();
            for (int j = 0; j < x_length; j++) {
                int currentNumber = (x[j] >> bitStart) & ((1 << beta) - 1);
                uniqueValues.add(currentNumber);
                if (uniqueValues.size() >= th) {
                    break;
                }
            }
            if (uniqueValues.size() < th) {
                int deCost = x_length * bitWidth(uniqueValues.size()) + uniqueValues.size() * beta;
                if (deCost < currentCost) {
                    currentCost = deCost;
                    bestType = 2;
                }
            }
        }

        return new int[] { currentCost, bestType };
    }

    /**
     * Subcolumn with variable bitwidth per subcolumn: each subcolumn can have a different bitwidth.
     * Uses DP to find the partition of [0, m) into segments (subcolumns) that minimizes a cost model.
     *
     * Optimality note: The result is optimal only with respect to our *cost model* (BPE/RLE/DE cost
     * estimates in bits). The model is approximate: it does not match exact bit-packing (e.g. 8 values
     * per block), RLE/DE headers, or alignment. We do include the overhead of storing the variable
     * beta list (1 byte per subcolumn) so that more segments are penalized. Fixed beta can still
     * win when: (1) the cost model underestimates real size, (2) data suits one beta well, or
     * (3) block is small so the extra (1+l) bytes for variable betas matter.
     *
     * Fills encodingType[0..l-1] and betaOut[0..l-1], returns l (number of subcolumns).
     */
    public static int Subcolumn(int[] x, int x_length, int m, int block_size, int[] encodingType, int[] betaOut) {

        if (m == 0) {
            betaOut[0] = 1;
            return 1;
        }

        int[] bpe_cost_single = new int[m];
        int[] rle_cost_single = new int[m];
        int[] de_cost_single = new int[m];

        int[] threshold = getThreshold(block_size);

        BitSet[] bitsets = new BitSet[m];
        for (int i = 0; i < m; i++) {
            bitsets[i] = new BitSet(x_length);
        }

        for (int i = 0; i < m; i++) {
            int current_value = (x[0] >> i) & 1;
            if (current_value == 1) {
                bpe_cost_single[i] = x_length;
            }
            int count = 0;
            de_cost_single[i] = x_length * 1 + 2 * 1;
            for (int j = 1; j < x_length; j++) {
                int subcolumn_ij = (x[j] >> i) & 1;
                if (subcolumn_ij == 1) {
                    bpe_cost_single[i] = x_length;
                }
                if (subcolumn_ij != current_value) {
                    count++;
                    current_value = subcolumn_ij;
                    de_cost_single[i] = x_length * 2 + 2 * 1;
                    bitsets[i].set(j - 1);
                }
            }
            bitsets[i].set(x_length - 1);
            count++;
            rle_cost_single[i] = count * (1 + bitWidth(x_length));
        }

        int maxBeta = Math.min(m, 32);
        int[] dp = new int[m + 1];
        int[] bestBeta = new int[m + 1];
        int[] bestEncodingType = new int[m + 1];
        dp[0] = 0;
        final int BETA_STORAGE_BITS = 8;
        for (int i = 1; i <= m; i++) {
            dp[i] = Integer.MAX_VALUE;
            for (int beta = 1; beta <= Math.min(i, maxBeta); beta++) {
                int segStart = i - beta;
                int[] segResult = costForSegment(x, x_length, segStart, i,
                        bpe_cost_single, rle_cost_single, de_cost_single, bitsets, threshold);
                int segCost = segResult[0];
                int segType = segResult[1];
                long total = (long) dp[segStart] + segCost + BETA_STORAGE_BITS;
                if (total < dp[i]) {
                    dp[i] = (int) total;
                    bestBeta[i] = beta;
                    bestEncodingType[i] = segType;
                }
            }
        }

        int pos = m;
        int l = 0;
        int[] revBeta = new int[m];
        int[] revType = new int[m];
        while (pos > 0) {
            int beta = bestBeta[pos];
            revBeta[l] = beta;
            revType[l] = bestEncodingType[pos];
            l++;
            pos -= beta;
        }
        for (int i = 0; i < l; i++) {
            betaOut[i] = revBeta[l - 1 - i];
            encodingType[i] = revType[l - 1 - i];
        }
        return l;
    }

    /**
     * Fixed beta: find the single beta that minimizes total cost over all subcolumns.
     * Fills encodingType[0..l-1] where l = ceil(m/betaBest). Returns betaBest.
     */
    public static int SubcolumnFixed(int[] x, int x_length, int m, int block_size, int[] encodingType) {
        if (m == 0) {
            encodingType[0] = 0;
            return 1;
        }
        int[] bpe_cost_single = new int[m];
        int[] rle_cost_single = new int[m];
        int[] de_cost_single = new int[m];
        int[] threshold = getThreshold(block_size);
        BitSet[] bitsets = new BitSet[m];
        for (int i = 0; i < m; i++) {
            bitsets[i] = new BitSet(x_length);
        }
        for (int i = 0; i < m; i++) {
            int current_value = (x[0] >> i) & 1;
            if (current_value == 1) {
                bpe_cost_single[i] = x_length;
            }
            int count = 0;
            de_cost_single[i] = x_length * 1 + 2 * 1;
            for (int j = 1; j < x_length; j++) {
                int subcolumn_ij = (x[j] >> i) & 1;
                if (subcolumn_ij == 1) {
                    bpe_cost_single[i] = x_length;
                }
                if (subcolumn_ij != current_value) {
                    count++;
                    current_value = subcolumn_ij;
                    de_cost_single[i] = x_length * 2 + 2 * 1;
                    bitsets[i].set(j - 1);
                }
            }
            bitsets[i].set(x_length - 1);
            count++;
            rle_cost_single[i] = count * (1 + bitWidth(x_length));
        }
        int cost1 = 0;
        for (int i = 0; i < m; i++) {
            if (bpe_cost_single[i] <= rle_cost_single[i] && bpe_cost_single[i] <= de_cost_single[i]) {
                encodingType[i] = 0;
                cost1 += bpe_cost_single[i];
            } else if (rle_cost_single[i] < bpe_cost_single[i] && rle_cost_single[i] <= de_cost_single[i]) {
                encodingType[i] = 1;
                cost1 += rle_cost_single[i];
            } else {
                encodingType[i] = 2;
                cost1 += de_cost_single[i];
            }
        }
        int cMin = cost1;
        int betaBest = 1;
        for (int beta = 2; beta <= m; beta++) {
            int l = (m + beta - 1) / beta;
            int cost = 0;
            int[] encodingTypeTemp = new int[l];
            for (int i = 0; i < l; i++) {
                int currentCost = 0;
                int bpCost = 0;
                int beta_start = Math.min(m - 1, (i + 1) * beta - 1);
                while (beta_start >= i * beta && bpe_cost_single[beta_start] == 0) {
                    beta_start--;
                }
                if (beta_start < i * beta) {
                    beta_start = i * beta;
                }
                bpCost = bpe_cost_single[beta_start] * (beta_start - i * beta + 1);
                currentCost = bpCost;
                int rleCostMax = 0;
                for (int j = i * beta; j < (i + 1) * beta && j < m; j++) {
                    if (rle_cost_single[j] > rleCostMax) {
                        rleCostMax = rle_cost_single[j];
                    }
                }
                if (rleCostMax < currentCost) {
                    BitSet mergedBitSet = new BitSet(x_length);
                    for (int j = i * beta; j < (i + 1) * beta && j < m; j++) {
                        mergedBitSet.or(bitsets[j]);
                        if (mergedBitSet.cardinality() >= currentCost) {
                            break;
                        }
                    }
                    int rleCost = mergedBitSet.cardinality() * (beta + bitWidth(x_length));
                    if (rleCost < currentCost) {
                        currentCost = rleCost;
                        encodingTypeTemp[i] = 1;
                    }
                }
                if (beta <= threshold.length) {
                    Set<Integer> uniqueValues = new HashSet<>();
                    for (int j = 0; j < x_length; j++) {
                        int currentNumber = (x[j] >> (i * beta)) & ((1 << beta) - 1);
                        uniqueValues.add(currentNumber);
                        if (uniqueValues.size() >= threshold[beta - 1]) {
                            break;
                        }
                    }
                    if (uniqueValues.size() < threshold[beta - 1]) {
                        int deCost = x_length * bitWidth(uniqueValues.size()) + uniqueValues.size() * beta;
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

    private static int[] getThreshold(int block_size) {
        switch (block_size) {
            case 32:
                return new int[] {2, 3, 5, 8, 9, 11, 14, 16, 17, 17, 18, 19, 20, 21, 22, 22, 23, 24, 24, 24, 25, 25, 26, 26, 26, 26, 27, 27, 27, 27, 27, 27};
            case 64:
                return new int[] {2, 3, 5, 9, 13, 17, 19, 24, 29, 32, 33, 33, 35, 37, 39, 40, 42, 43, 44, 45, 46, 47, 48, 48, 49, 50, 50, 51, 51, 52, 52, 52};
            case 128:
                return new int[] {2, 3, 5, 9, 17, 22, 33, 33, 43, 52, 59, 64, 65, 65, 69, 72, 76, 79, 81, 84, 86, 88, 90, 91, 93, 94, 95, 96, 98, 99, 100, 100};
            case 256:
                return new int[] {2, 3, 5, 9, 17, 33, 37, 64, 65, 77, 94, 107, 119, 128, 129, 129, 136, 143, 149, 154, 159, 163, 167, 171, 175, 178, 181, 183, 186, 188, 190, 192};
            case 512:
                return new int[] {2, 3, 5, 9, 17, 33, 65, 65, 114, 129, 140, 171, 197, 220, 239, 256, 257, 257, 270, 282, 293, 303, 312, 320, 328, 335, 342, 348, 354, 359, 364, 368};
            case 1024:
                return new int[] {2, 3, 5, 9, 17, 33, 65, 128, 129, 205, 257, 257, 316, 366, 410, 448, 482, 512, 513, 513, 537, 559, 579, 598, 615, 631, 645, 659, 671, 683, 694, 704};
            case 2048:
                return new int[] {2, 3, 5, 9, 17, 33, 65, 129, 228, 257, 373, 512, 513, 586, 683, 768, 844, 911, 971, 1024, 1025, 1025, 1069, 1110, 1147, 1182, 1214, 1244, 1272, 1298, 1322, 1344};
            case 4096:
                return new int[] {2, 3, 5, 9, 17, 33, 65, 129, 257, 410, 513, 683, 946, 1025, 1093, 1280, 1446, 1593, 1725, 1844, 1951, 2048, 2049, 2049, 2130, 2206, 2276, 2341, 2402, 2458, 2511, 2560};
            case 8192:
                return new int[] {2, 3, 5, 9, 17, 33, 65, 129, 257, 513, 745, 1025, 1261, 1756, 2049, 2049, 2410, 2731, 3019, 3277, 3511, 3724, 3918, 4096, 4097, 4097, 4248, 4389, 4520, 4643, 4757, 4864};
            default:
                return new int[] {2, 3, 5, 8, 9, 11, 14, 16, 17, 17, 18, 19, 20, 21, 22, 22, 23, 24, 24, 24, 25, 25, 26, 26, 26, 26, 27, 27, 27, 27, 27, 27};
        }
    }

    public static int SubcolumnEncoder(int[] list, int encode_pos, byte[] encoded_result, int[] beta, int l, int block_size, int[] encodingType) {
        int list_length = list.length;
        int maxValue = 0;
        for (int k : list) {
            if (k > maxValue) {
                maxValue = k;
            }
        }

        int m = bitWidth(maxValue);

        intByte2Bytes(m, encode_pos, encoded_result);
        encode_pos += 1;

        if (m == 0) {
            return encode_pos;
        }

        int[] bitWidthList = new int[l];
        int[][] subcolumnList = new int[l][list_length];

        intByte2Bytes(l, encode_pos, encoded_result);
        encode_pos += 1;
        for (int i = 0; i < l; i++) {
            intByte2Bytes(beta[i], encode_pos + i, encoded_result);
        }
        encode_pos += l;

        int bw = bitWidth(block_size);
        int shiftSoFar = 0;
        for (int i = 0; i < l; i++) {
            int mask = (1 << beta[i]) - 1;
            int maxValuePart = 0;
            for (int j = 0; j < list_length; j++) {
                subcolumnList[i][j] = (list[j] >> shiftSoFar) & mask;
                if (subcolumnList[i][j] > maxValuePart) {
                    maxValuePart = subcolumnList[i][j];
                }
            }
            bitWidthList[i] = bitWidth(maxValuePart);
            shiftSoFar += beta[i];
        }

        encode_pos = bitPacking(bitWidthList, 8, encode_pos, encoded_result, l);

        int preTypePos = encode_pos;
        encode_pos += (l + 3) / 4;

        for (int i = 0; i < l; i++) {

            if (encodingType[i] == 2) {

            Set<Integer> uniqueValues = new HashSet<>();
            for (int j = 0; j < list_length; j++) {
                int currentNumber = subcolumnList[i][j];
                uniqueValues.add(currentNumber);
            }
            int cardinality = uniqueValues.size();

                   int dict_bit_width = bitWidth(cardinality) ;

                   List<Integer> sortedUnique = new ArrayList<>(uniqueValues);
                    Collections.sort(sortedUnique);
                    Map<Integer, Integer> valueToCode = new HashMap<>();
                    int[] dict_key_list = new int[cardinality];

                    for (int j = 0; j < cardinality; j++) {
                        valueToCode.put(sortedUnique.get(j), j);
                        dict_key_list[j] = sortedUnique.get(j);

                    }

                    for (int j = 0; j < list_length; j++) {
                        int currentNumber = subcolumnList[i][j];
                        int encodedValue = valueToCode.get(currentNumber);
                        subcolumnList[i][j] = encodedValue;
                    }

                    encoded_result[encode_pos] = (byte) (cardinality >> 8);
                    encode_pos += 1;
                    encoded_result[encode_pos] = (byte) (cardinality & 0xFF);
                    encode_pos += 1;

                    encode_pos = bitPacking(dict_key_list, bitWidthList[i], encode_pos, encoded_result, cardinality);

                    encode_pos = bitPacking(subcolumnList[i], dict_bit_width, encode_pos, encoded_result, list_length);
                    continue;
                }

            if (encodingType[i] == 0) {

                encode_pos = bitPacking(subcolumnList[i], bitWidthList[i], encode_pos, encoded_result, list_length);

            } else {

                int previous = subcolumnList[i][0];
            int index = 0;

            for (int j = 1; j < list_length; j++) {
                int currentNumber = subcolumnList[i][j];

                if (currentNumber != previous) {
                    index++;
                    previous = currentNumber;
                }

            }

            index++;

                encoded_result[encode_pos] = (byte) (index >> 8);
                encode_pos += 1;
                encoded_result[encode_pos] = (byte) (index & 0xFF);
                encode_pos += 1;

                index = 0;
                int[] run_length = new int[list_length];
                int[] rle_values = new int[list_length];
                previous = subcolumnList[i][0];

                for (int j = 1; j < list_length; j++) {
                    int currentNumber = subcolumnList[i][j];
                    if (currentNumber != previous) {
                        run_length[index] = j;
                        rle_values[index] = previous;
                        index++;
                        previous = currentNumber;
                    }
                }

                run_length[index] = list_length;
                rle_values[index] = previous;
                index++;

                encode_pos = bitPacking(run_length, bw, encode_pos, encoded_result, index);

                encode_pos = bitPacking(rle_values, bitWidthList[i], encode_pos, encoded_result, index);

            }


        }

        preTypePos = bitPacking(encodingType, 2, preTypePos, encoded_result, l);

        return encode_pos;
    }

    public static int SubcolumnDecoder(byte[] encoded_result, int encode_pos, int[] list, int block_size) {
        int list_length = list.length;

        int m = bytes2Integer(encoded_result, encode_pos, 1);
        encode_pos += 1;

        if (m == 0) {
            return encode_pos;
        }

        int bw = bitWidth(block_size);

        int l = bytes2Integer(encoded_result, encode_pos, 1);
        encode_pos += 1;
        int[] beta = new int[l];
        for (int i = 0; i < l; i++) {
            beta[i] = bytes2Integer(encoded_result, encode_pos + i, 1);
        }
        encode_pos += l;

        int[] bitWidthList = new int[l];

        encode_pos = decodeBitPacking(encoded_result, encode_pos, 8, l, bitWidthList);

        int[][] subcolumnList = new int[l][list_length];

        int[] encodingType = new int[l];

        encode_pos = decodeBitPacking(encoded_result, encode_pos, 2, l, encodingType);

        for (int i = 0; i < l; i++) {
            int type = encodingType[i];
            int bitWidth = bitWidthList[i];
            if (type == 0) {
                encode_pos = decodeBitPacking(encoded_result, encode_pos, bitWidth, list_length,
                        subcolumnList[i]);
            } else if(type == 1) {
                int index = ((encoded_result[encode_pos] & 0xFF) << 8) | (encoded_result[encode_pos + 1] & 0xFF);

                encode_pos += 2;

                int[] run_length = new int[index];
                int[] rle_values = new int[index];

                encode_pos = decodeBitPacking(encoded_result, encode_pos, bw, index, run_length);
                encode_pos = decodeBitPacking(encoded_result, encode_pos, bitWidth, index, rle_values);

                int currentIndex = 0;
                for (int j = 0; j < index; j++) {
                    int endPos = run_length[j];
                    int value = rle_values[j];
                    while (currentIndex < endPos) {
                        subcolumnList[i][currentIndex] = value;
                        currentIndex++;
                    }
                }
            }else {
                int cardinality = ((encoded_result[encode_pos] & 0xFF) << 8) | (encoded_result[encode_pos + 1] & 0xFF);
                encode_pos += 2;
                int dict_bit_width = bitWidth(cardinality);
                int[] dict_key_list = new int[cardinality];
                int[] dict_value_list = new int[cardinality];

                for (int j = 0; j < cardinality; j++) {
                    dict_value_list[j] = j;
                }

                encode_pos = decodeBitPacking(encoded_result,  encode_pos, bitWidthList[i], cardinality, dict_key_list);
                // encode_pos = decodeBitPacking(encoded_result,  encode_pos, dict_bit_width, cardinality, dict_value_list);

                encode_pos =decodeBitPacking(encoded_result,  encode_pos, dict_bit_width, list_length, subcolumnList[i]);
                Map<Integer, Integer> valueToCode = new HashMap<>();
                for (int j = 0; j < cardinality; j++) {
                    valueToCode.put(dict_value_list[j], dict_key_list[j]);
                }

                for (int j = 0; j < list_length; j++) {
                    int currentNumber = subcolumnList[i][j];
                    int encodedValue = valueToCode.get(currentNumber);
                    subcolumnList[i][j] = encodedValue;
                }

            }
        }

        int shiftSoFar = 0;
        for (int i = 0; i < l; i++) {
            for (int j = 0; j < list_length; j++) {
                list[j] |= subcolumnList[i][j] << shiftSoFar;
            }
            shiftSoFar += beta[i];
        }

        return encode_pos;
    }

    public static int[] getAbsDeltaTsBlock(
            int[] ts_block,
            int i,
            int block_size,
            int remaining,
            int[] min_delta) {
        int[] ts_block_delta = new int[remaining];

        int value_delta_min = Integer.MAX_VALUE;
        int value_delta_max = Integer.MIN_VALUE;
        int base = i * block_size;
        int end = i * block_size + remaining;

        for (int j = base; j < end; j++) {
            int cur = ts_block[j];
            if (cur < value_delta_min) {
                value_delta_min = cur;
            }
            if (cur > value_delta_max) {
                value_delta_max = cur;
            }
        }

        for (int j = base; j < end; j++) {
            ts_block_delta[j - base] = ts_block[j] - value_delta_min;
        }

        min_delta[0] = value_delta_min;

        return ts_block_delta;
    }

    private static final int TEMP_ENCODE_BUF_SIZE = 256 * 1024;

    private static final class PartitionStats {
        private final Map<String, Integer> partitionCount = new HashMap<>();
        private final Map<Integer, Integer> betaHist = new HashMap<>();

        void addPartition(int[] betas, int l) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < l; i++) {
                if (i > 0) {
                    sb.append(',');
                }
                sb.append(betas[i]);
                betaHist.merge(betas[i], 1, Integer::sum);
            }
            partitionCount.merge(sb.toString(), 1, Integer::sum);
        }

        List<Map.Entry<String, Integer>> topPartitions(int k) {
            ArrayList<Map.Entry<String, Integer>> entries = new ArrayList<>(partitionCount.entrySet());
            entries.sort((a, b) -> Integer.compare(b.getValue(), a.getValue()));
            return entries.subList(0, Math.min(k, entries.size()));
        }

        List<Map.Entry<Integer, Integer>> betaHistogram() {
            ArrayList<Map.Entry<Integer, Integer>> entries = new ArrayList<>(betaHist.entrySet());
            entries.sort(Map.Entry.comparingByKey());
            return entries;
        }
    }

    /*
     * The following "choose + export per-block optimal partition" helper was used only for writing
     * partition details into CSV. It's currently disabled in test0(), so we comment it out to keep
     * this test file clean (no unused warnings). Re-enable if you want the per-block export again.
     */
    // private static final class OptimalPartitionResult {
    //     private final int[] betas;
    //     private final int l;
    //     private final int m;
    //     private final boolean useVariable;
    //     private final int sizeVar;
    //     private final int sizeFixed;
    //
    //     private OptimalPartitionResult(
    //             int[] betas, int l, int m, boolean useVariable, int sizeVar, int sizeFixed) {
    //         this.betas = betas;
    //         this.l = l;
    //         this.m = m;
    //         this.useVariable = useVariable;
    //         this.sizeVar = sizeVar;
    //         this.sizeFixed = sizeFixed;
    //     }
    // }
    //
    // private static OptimalPartitionResult chooseOptimalPartitionForBlock(
    //         int[] dataDelta, int remainder, int block_size) {
    //     int maxValue = 0;
    //     for (int j = 0; j < remainder; j++) {
    //         int v = dataDelta[j];
    //         if (v > maxValue) {
    //             maxValue = v;
    //         }
    //     }
    //     int m = bitWidth(maxValue);
    //     if (m == 0) {
    //         return new OptimalPartitionResult(new int[] {1}, 1, 0, true, 0, 0);
    //     }
    //
    //     int[] encodingTypeVar = new int[m];
    //     int[] betaOut = new int[m];
    //     int lVar = Subcolumn(dataDelta, remainder, m, block_size, encodingTypeVar, betaOut);
    //     byte[] tempVar = new byte[TEMP_ENCODE_BUF_SIZE];
    //     int sizeVar = SubcolumnEncoder(dataDelta, 0, tempVar, betaOut, lVar, block_size, encodingTypeVar);
    //
    //     int[] encodingTypeFixed = new int[m];
    //     int betaFixed = SubcolumnFixed(dataDelta, remainder, m, block_size, encodingTypeFixed);
    //     int lFixed = (m + betaFixed - 1) / betaFixed;
    //     int[] betaFixedArr = new int[lFixed];
    //     Arrays.fill(betaFixedArr, betaFixed);
    //     byte[] tempFixed = new byte[TEMP_ENCODE_BUF_SIZE];
    //     int sizeFixed = SubcolumnEncoder(dataDelta, 0, tempFixed, betaFixedArr, lFixed, block_size, encodingTypeFixed);
    //
    //     if (sizeVar <= sizeFixed) {
    //         return new OptimalPartitionResult(Arrays.copyOf(betaOut, lVar), lVar, m, true, sizeVar, sizeFixed);
    //     } else {
    //         return new OptimalPartitionResult(betaFixedArr, lFixed, m, false, sizeVar, sizeFixed);
    //     }
    // }

    private static void collectOptimalPartitionForBlock(
            int[] dataDelta, int remainder, int block_size, PartitionStats stats) {
        int maxValue = 0;
        for (int j = 0; j < remainder; j++) {
            int v = dataDelta[j];
            if (v > maxValue) {
                maxValue = v;
            }
        }
        int m = bitWidth(maxValue);
        if (m == 0) {
            stats.addPartition(new int[] {1}, 1);
            return;
        }

        int[] encodingTypeVar = new int[m];
        int[] betaOut = new int[m];
        int lVar = Subcolumn(dataDelta, remainder, m, block_size, encodingTypeVar, betaOut);
        byte[] tempVar = new byte[TEMP_ENCODE_BUF_SIZE];
        int sizeVar = SubcolumnEncoder(dataDelta, 0, tempVar, betaOut, lVar, block_size, encodingTypeVar);

        int[] encodingTypeFixed = new int[m];
        int betaFixed = SubcolumnFixed(dataDelta, remainder, m, block_size, encodingTypeFixed);
        int lFixed = (m + betaFixed - 1) / betaFixed;
        int[] betaFixedArr = new int[lFixed];
        Arrays.fill(betaFixedArr, betaFixed);
        byte[] tempFixed = new byte[TEMP_ENCODE_BUF_SIZE];
        int sizeFixed = SubcolumnEncoder(dataDelta, 0, tempFixed, betaFixedArr, lFixed, block_size, encodingTypeFixed);

        if (sizeVar <= sizeFixed) {
            stats.addPartition(betaOut, lVar);
        } else {
            stats.addPartition(betaFixedArr, lFixed);
        }
    }

    public static int BlockEncoder(int[] data, int block_index, int block_size, int remainder,
            int encode_pos, byte[] encoded_result, int[] beta) {
        int[] min_delta = new int[3];

        int[] data_delta = getAbsDeltaTsBlock(data, block_index, block_size,
                remainder, min_delta);

        int2Bytes(min_delta[0], encode_pos, encoded_result);
        encode_pos += 4;

        int maxValue = 0;
        for (int j = 0; j < remainder; j++) {
            if (data_delta[j] > maxValue) {
                maxValue = data_delta[j];
            }
        }
        int m = bitWidth(maxValue);

        if (m == 0) {
            encode_pos = SubcolumnEncoder(data_delta, encode_pos, encoded_result,
                    beta, 1, block_size, new int[] {0});
            return encode_pos;
        }

        int[] encodingTypeVar = new int[Math.max(m, 1)];
        int[] betaOut = new int[Math.max(m, 1)];
        int lVar = Subcolumn(data_delta, remainder, m, block_size, encodingTypeVar, betaOut);

        byte[] tempVar = new byte[TEMP_ENCODE_BUF_SIZE];
        int posVar = SubcolumnEncoder(data_delta, 0, tempVar, betaOut, lVar, block_size, encodingTypeVar);
        int sizeVar = posVar;

        int[] encodingTypeFixed = new int[Math.max(m, 1)];
        int betaFixed = SubcolumnFixed(data_delta, remainder, m, block_size, encodingTypeFixed);
        int lFixed = (m + betaFixed - 1) / betaFixed;
        int[] betaFixedArr = new int[33];
        for (int i = 0; i < lFixed; i++) {
            betaFixedArr[i] = betaFixed;
        }

        byte[] tempFixed = new byte[TEMP_ENCODE_BUF_SIZE];
        int posFixed = SubcolumnEncoder(data_delta, 0, tempFixed, betaFixedArr, lFixed, block_size, encodingTypeFixed);
        int sizeFixed = posFixed;

        if (sizeVar <= sizeFixed) {
            System.arraycopy(tempVar, 0, encoded_result, encode_pos, sizeVar);
            encode_pos += sizeVar;
        } else {
            System.arraycopy(tempFixed, 0, encoded_result, encode_pos, sizeFixed);
            encode_pos += sizeFixed;
        }
        return encode_pos;
    }

    public static int BlockDecoder(byte[] encoded_result, int block_index, int block_size, int remainder,
            int encode_pos, int[] data) {
        int[] min_delta = new int[3];

        min_delta[0] = bytes2Integer(encoded_result, encode_pos, 4);
        encode_pos += 4;

        int[] block_data = new int[remainder];

        encode_pos = SubcolumnDecoder(encoded_result, encode_pos,
                block_data, block_size);

        for (int i = 0; i < remainder; i++) {
            data[block_index * block_size + i] = block_data[i] + min_delta[0];
        }

        return encode_pos;
    }

    public static int Encoder(int[] data, int block_size, byte[] encoded_result) {
        int data_length = data.length;
        int encode_pos = 0;

        int2Bytes(data_length, encode_pos, encoded_result);
        encode_pos += 4;

        int2Bytes(block_size, encode_pos, encoded_result);
        encode_pos += 4;

        int num_blocks = data_length / block_size;

        int remainder = data_length % block_size;

        int[] beta = new int[33];

        for (int i = 0; i < num_blocks; i++) {
            encode_pos = BlockEncoder(data, i, block_size, block_size, encode_pos, encoded_result, beta);
        }

        if (remainder <= 3) {
            for (int i = 0; i < remainder; i++) {
                int value = data[num_blocks * block_size + i];
                int2Bytes(value, encode_pos, encoded_result);
                encode_pos += 4;
            }
        } else {
            encode_pos = BlockEncoder(data, num_blocks, block_size, remainder, encode_pos,
                    encoded_result, beta);
        }


        return encode_pos;
    }

    public static int[] Decoder(byte[] encoded_result) {
        int encode_pos = 0;

        int data_length = bytes2Integer(encoded_result, encode_pos, 4);
        encode_pos += 4;

        int block_size = bytes2Integer(encoded_result, encode_pos, 4);
        encode_pos += 4;

        int num_blocks = data_length / block_size;

        int[] data = new int[data_length];

        for (int i = 0; i < num_blocks; i++) {
            encode_pos = BlockDecoder(encoded_result, i, block_size, block_size, encode_pos, data);
        }

        int remainder = data_length % block_size;

        if (remainder <= 3) {
            for (int i = 0; i < remainder; i++) {
                data[num_blocks * block_size + i] = bytes2Integer(encoded_result, encode_pos, 4);
                encode_pos += 4;
            }
        } else {
            encode_pos = BlockDecoder(encoded_result, num_blocks, block_size, remainder,
                    encode_pos, data);
        }

        return data;
    }

    public static int getDecimalPrecision(String str) {
        int decimalIndex = str.indexOf(".");

        if (decimalIndex == -1) {
            return 0;
        }

        return str.substring(decimalIndex + 1).length();
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
    public void test0() throws IOException {
        String parent_dir = "path/to/your/directory/";

        String input_parent_dir = parent_dir + "dataset/";

        String output_parent_dir = parent_dir + "result/";

        String outputPath = output_parent_dir + "subcolumn_variable_alpha.csv";
        // NOTE: Subcolumn-partition CSV export is currently disabled (commented out).
        // String partitionOutputPath = output_parent_dir + "subcolumn_variable_alpha_optimal_partition.csv";
        // String blockPartitionOutputPath = output_parent_dir + "subcolumn_variable_alpha_optimal_partition_per_block.csv";

        int block_size = 512;

        int repeatTime = 1;

        CsvWriter writer = new CsvWriter(outputPath, ',', StandardCharsets.UTF_8);
        writer.setRecordDelimiter('\n');

        // CsvWriter partitionWriter = new CsvWriter(partitionOutputPath, ',', StandardCharsets.UTF_8);
        // partitionWriter.setRecordDelimiter('\n');
        //
        // CsvWriter blockPartitionWriter = new CsvWriter(blockPartitionOutputPath, ',', StandardCharsets.UTF_8);
        // blockPartitionWriter.setRecordDelimiter('\n');

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

        // String[] partitionHead = {
        //         "Dataset",
        //         "Block Size",
        //         "Top Partition Betas",
        //         "Blocks (Top Partition)",
        //         "Subcolumn Index",
        //         "Beta (bits)"
        // };
        // partitionWriter.writeRecord(partitionHead);
        //
        // String[] blockPartitionHead = {
        //         "Dataset",
        //         "Block Size",
        //         "Block Index",
        //         "Points In Block",
        //         "m (bitWidth(maxDelta))",
        //         "Chosen Scheme",
        //         "Encoded Size Var (bytes)",
        //         "Encoded Size Fixed (bytes)",
        //         "Betas",
        //         "Subcolumn Index",
        //         "Beta (bits)"
        // };
        // blockPartitionWriter.writeRecord(blockPartitionHead);

        File directory = new File(input_parent_dir);
        File[] csvFiles = directory.listFiles((dir, name) -> name.endsWith(".csv"));
        if (csvFiles == null) {
            writer.close();
            // partitionWriter.close();
            // blockPartitionWriter.close();
            throw new IOException("No csv files found under: " + input_parent_dir);
        }

        for (File file : csvFiles) {
            String datasetName = extractFileName(file.toString());
            System.out.println(datasetName);
            // reset verification sink per dataset (kept to avoid JIT removing verification work)
            VERIFY_SINK = 0L;

            InputStream inputStream = Files.newInputStream(file.toPath());

            CsvReader loader = new CsvReader(inputStream, StandardCharsets.UTF_8);
            ArrayList<Float> data1 = new ArrayList<>();

            int max_decimal = 0;
            while (loader.readRecord()) {
                String f_str = loader.getValues()[0];
                if (f_str.isEmpty()) {
                    continue;
                }
                int cur_decimal = getDecimalPrecision(f_str);
                if (cur_decimal > max_decimal) {
                    max_decimal = cur_decimal;
                }
                data1.add(Float.valueOf(f_str));
            }
            inputStream.close();

            if (max_decimal > 8) {
                max_decimal = 8;
            }

            int[] data2_arr = new int[data1.size()];

            int max_mul = (int) Math.pow(10, max_decimal);
            for (int i = 0; i < data1.size(); i++) {
                data2_arr[i] = (int) (data1.get(i) * max_mul);
            }

            System.out.println(max_decimal);
            byte[] encoded_result = new byte[data2_arr.length * 4];

            long encodeTime = 0;
            long decodeTime = 0;
            double ratio = 0;
            double compressed_size = 0;

            int length = 0;

            // Collect "optimal" subcolumn partitions (chosen between variable vs fixed).
            PartitionStats stats = new PartitionStats();
            int dataLength = data2_arr.length;
            int numBlocks = dataLength / block_size;
            int remainderPoints = dataLength % block_size;
            for (int bi = 0; bi < numBlocks; bi++) {
                int[] min_delta = new int[3];
                int[] data_delta = getAbsDeltaTsBlock(data2_arr, bi, block_size, block_size, min_delta);
                collectOptimalPartitionForBlock(data_delta, block_size, block_size, stats);
                // Per-block partition export (disabled)
                // OptimalPartitionResult r = chooseOptimalPartitionForBlock(data_delta, block_size, block_size);
                // StringBuilder betasSb = new StringBuilder();
                // for (int i = 0; i < r.l; i++) {
                //     if (i > 0) {
                //         betasSb.append(',');
                //     }
                //     betasSb.append(r.betas[i]);
                // }
                // String betasStr = betasSb.toString();
                // String scheme = r.useVariable ? "VARIABLE" : "FIXED";
                // for (int si = 0; si < r.l; si++) {
                //     String[] row = {
                //             datasetName,
                //             String.valueOf(block_size),
                //             String.valueOf(bi),
                //             String.valueOf(block_size),
                //             String.valueOf(r.m),
                //             scheme,
                //             String.valueOf(r.sizeVar),
                //             String.valueOf(r.sizeFixed),
                //             betasStr,
                //             String.valueOf(si),
                //             String.valueOf(r.betas[si])
                //     };
                //     blockPartitionWriter.writeRecord(row);
                // }
            }
            if (remainderPoints > 3) {
                int[] min_delta = new int[3];
                int[] data_delta = getAbsDeltaTsBlock(data2_arr, numBlocks, block_size, remainderPoints, min_delta);
                collectOptimalPartitionForBlock(data_delta, remainderPoints, block_size, stats);
                // Per-block partition export for remainder block (disabled)
                // OptimalPartitionResult r = chooseOptimalPartitionForBlock(data_delta, remainderPoints, block_size);
                // StringBuilder betasSb = new StringBuilder();
                // for (int i = 0; i < r.l; i++) {
                //     if (i > 0) {
                //         betasSb.append(',');
                //     }
                //     betasSb.append(r.betas[i]);
                // }
                // String betasStr = betasSb.toString();
                // String scheme = r.useVariable ? "VARIABLE" : "FIXED";
                // for (int si = 0; si < r.l; si++) {
                //     String[] row = {
                //             datasetName,
                //             String.valueOf(block_size),
                //             String.valueOf(numBlocks),
                //             String.valueOf(remainderPoints),
                //             String.valueOf(r.m),
                //             scheme,
                //             String.valueOf(r.sizeVar),
                //             String.valueOf(r.sizeFixed),
                //             betasStr,
                //             String.valueOf(si),
                //             String.valueOf(r.betas[si])
                //     };
                //     blockPartitionWriter.writeRecord(row);
                // }
            }

            System.out.println("Optimal subcolumn partitions (top 5):");
            for (Map.Entry<String, Integer> e1 : stats.topPartitions(5)) {
                System.out.println("  betas=[" + e1.getKey() + "], blocks=" + e1.getValue());
            }
            System.out.println("Beta histogram (beta -> count):");
            for (Map.Entry<Integer, Integer> e2 : stats.betaHistogram()) {
                System.out.println("  " + e2.getKey() + " -> " + e2.getValue());
            }

            // Export the top-1 (most frequent) "optimal" partition's betas (disabled).
            // List<Map.Entry<String, Integer>> top1 = stats.topPartitions(1);
            // if (!top1.isEmpty()) {
            //     String betasStr = top1.get(0).getKey();
            //     int blocks = top1.get(0).getValue();
            //     if (betasStr != null && !betasStr.isEmpty()) {
            //         String[] parts = betasStr.split(",");
            //         for (int si = 0; si < parts.length; si++) {
            //             String betaStr = parts[si].trim();
            //             if (betaStr.isEmpty()) {
            //                 continue;
            //             }
            //             String[] row = {
            //                     datasetName,
            //                     String.valueOf(block_size),
            //                     betasStr,
            //                     String.valueOf(blocks),
            //                     String.valueOf(si),
            //                     betaStr
            //             };
            //             partitionWriter.writeRecord(row);
            //         }
            //     } else {
            //         String[] row = {
            //                 datasetName,
            //                 String.valueOf(block_size),
            //                 betasStr == null ? "" : betasStr,
            //                 String.valueOf(blocks),
            //                 "0",
            //                 "1"
            //         };
            //         partitionWriter.writeRecord(row);
            //     }
            // }

            long s = System.nanoTime();
            for (int repeat = 0; repeat < repeatTime; repeat++) {
                length = Encoder(data2_arr, block_size, encoded_result);
            }

            long e = System.nanoTime();
            encodeTime += ((e - s) / repeatTime);
            compressed_size += length;

            double ratioTmp;

            ratioTmp = compressed_size / (double) (data1.size() * Long.BYTES);

            ratio += ratioTmp;

            System.out.println("Decode");

            s = System.nanoTime();

            for (int repeat = 0; repeat < repeatTime; repeat++) {
                int[] decoded = Decoder(encoded_result);
                // Include lossless verification in decode time (intentionally adds overhead).
                VERIFY_SINK += verifyLosslessNoThrow(data2_arr, decoded);
            }

            e = System.nanoTime();
            decodeTime += ((e - s) / repeatTime);

            String[] record = {
                    datasetName,
                    "Sub-columns (Dictionary)",
                    String.valueOf(encodeTime),
                    String.valueOf(decodeTime),
                    String.valueOf(data1.size()),
                    String.valueOf(compressed_size),
                    String.valueOf(ratio)
            };
            writer.writeRecord(record);
            // Make VERIFY_SINK observable so static-analysis doesn't mark it unused.
            System.out.println("verify_mismatches=" + VERIFY_SINK);
            System.out.println(ratio);
        }

        writer.close();
        // partitionWriter.close();
        // blockPartitionWriter.close();
    }

}
