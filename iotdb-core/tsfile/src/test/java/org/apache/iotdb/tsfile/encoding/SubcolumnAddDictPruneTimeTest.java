package org.apache.iotdb.tsfile.encoding;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Stream;

public class SubcolumnAddDictPruneTimeTest {

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

    public static int Subcolumn(int[] x, int x_length, int m, int block_size, int[] encodingType) {

        if (m == 0) {
            return 1;
        }
    
        int betaBest = 1;

        int[] bpe_cost_single = new int[m];
        int[] rle_cost_single = new int[m];
        int[] de_cost_single = new int[m];

        int[] threshold = null;
    
        switch(block_size) {
            case 32:
                threshold = new int[] {2, 3, 5, 8, 9, 11, 14, 16, 17, 17, 18, 19, 20, 21, 22, 22, 23, 24, 24, 24, 25, 25, 26, 26, 26, 26, 27, 27, 27, 27, 27, 27};
                break;
            case 64:
                threshold = new int[] {2, 3, 5, 9, 13, 17, 19, 24, 29, 32, 33, 33, 35, 37, 39, 40, 42, 43, 44, 45, 46, 47, 48, 48, 49, 50, 50, 51, 51, 52, 52, 52};
                break;
            case 128:
                threshold = new int[] {2, 3, 5, 9, 17, 22, 33, 33, 43, 52, 59, 64, 65, 65, 69, 72, 76, 79, 81, 84, 86, 88, 90, 91, 93, 94, 95, 96, 98, 99, 100, 100};
                break;
            case 256:
                threshold = new int[] {2, 3, 5, 9, 17, 33, 37, 64, 65, 77, 94, 107, 119, 128, 129, 129, 136, 143, 149, 154, 159, 163, 167, 171, 175, 178, 181, 183, 186, 188, 190, 192};
                break;
            case 512:
                threshold = new int[] {2, 3, 5, 9, 17, 33, 65, 65, 114, 129, 140, 171, 197, 220, 239, 256, 257, 257, 270, 282, 293, 303, 312, 320, 328, 335, 342, 348, 354, 359, 364, 368};
                break;
            case 1024:
                threshold = new int[] {2, 3, 5, 9, 17, 33, 65, 128, 129, 205, 257, 257, 316, 366, 410, 448, 482, 512, 513, 513, 537, 559, 579, 598, 615, 631, 645, 659, 671, 683, 694, 704};
                break;
            case 2048:
                threshold = new int[] {2, 3, 5, 9, 17, 33, 65, 129, 228, 257, 373, 512, 513, 586, 683, 768, 844, 911, 971, 1024, 1025, 1025, 1069, 1110, 1147, 1182, 1214, 1244, 1272, 1298, 1322, 1344};
                break;
            case 4096:
                threshold = new int[] {2, 3, 5, 9, 17, 33, 65, 129, 257, 410, 513, 683, 946, 1025, 1093, 1280, 1446, 1593, 1725, 1844, 1951, 2048, 2049, 2049, 2130, 2206, 2276, 2341, 2402, 2458, 2511, 2560};
                break;
            case 8192:
                threshold = new int[] {2, 3, 5, 9, 17, 33, 65, 129, 257, 513, 745, 1025, 1261, 1756, 2049, 2049, 2410, 2731, 3019, 3277, 3511, 3724, 3918, 4096, 4097, 4097, 4248, 4389, 4520, 4643, 4757, 4864};
                break;
            default:
                threshold = new int[] {2, 3, 5, 8, 9, 11, 14, 16, 17, 17, 18, 19, 20, 21, 22, 22, 23, 24, 24, 24, 25, 25, 26, 26, 26, 26, 27, 27, 27, 27, 27, 27};
                break;
        }

        int cost1 = 0;

        // System.out.println("x:");
        // for (int i = 0; i < x_length; i++) {
        //     System.out.print(x[i] + " ");
        // }
        // System.out.println();

        BitSet[] bitsets = new BitSet[m];

        for (int i = 0; i < m; i++) {
            bitsets[i] = new BitSet(x_length);
        }

        for (int i = 0; i < m; i++) {
            // System.out.println("subcolumn index: " + i);

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

            if (bpe_cost_single[i] <= rle_cost_single[i] && bpe_cost_single[i] <= de_cost_single[i]) {
                encodingType[i] = 0; // bpe
                cost1 += bpe_cost_single[i];
            } else if (rle_cost_single[i] < bpe_cost_single[i] && rle_cost_single[i] <= de_cost_single[i]) {
                encodingType[i] = 1; // rle
                cost1 += rle_cost_single[i];
            } else {
                encodingType[i] = 2; // de
                cost1 += de_cost_single[i];
            }

        }

        int cMin = cost1;

        int[] beta_list = new int[m - 1];
        for (int i = 0; i < m - 1; i++) {
            beta_list[i] = i + 2;
        }

        for (int beta : beta_list) {
            if (beta > m) {
                break;
            }
            // System.out.println("beta: " + beta);

            int l = (m + beta - 1) / beta;

            // System.out.println("l: " + l);

            int cost = 0;
        
            int[] encodingTypeTemp = new int[l];

            for (int i = 0; i < l; i++) {
                // System.out.println("subcolumn index: " + i);

                int currentCost = 0;

                int bpCost = 0;

                int beta_start = (Math.min(m - 1, (i + 1) * beta - 1));
                while (beta_start >= i * beta && bpe_cost_single[beta_start] == 0) {
                    beta_start--;
                }

                if (beta_start < i * beta) {
                    beta_start = i * beta;
                }

                bpCost = bpe_cost_single[beta_start] * (beta_start - i * beta + 1);

                // System.out.println("bpCost: " + bpCost);

                currentCost = bpCost;

                int rleCostMax = 0;
                for (int j = i * beta; j < (i + 1) * beta && j < m; j++) {
                    if (rle_cost_single[j] > rleCostMax) {
                        rleCostMax = rle_cost_single[j];
                    }
                }

                if (rleCostMax < currentCost) {
                // if (rle_cost_single[i * beta] < currentCost) {
                    int rleCost = 0;

                    boolean currentBetter = false;

                    BitSet mergedBitSet = new BitSet(x_length);
                    for (int j = i * beta; j < (i + 1) * beta && j < m; j++) {
                        mergedBitSet.or(bitsets[j]);
                        if (mergedBitSet.cardinality() >= currentCost) {
                            currentBetter = true;
                            break;
                        }
                    }

                    if (!currentBetter) {
                        rleCost = mergedBitSet.cardinality() * (beta + bitWidth(x_length));
                        if (currentCost > rleCost) {
                            currentCost = rleCost;
                            encodingTypeTemp[i] = 1;
                        }
                    }

                }

                int deCostMax = 0;
                for (int j = i * beta; j < (i + 1) * beta && j < m; j++) {
                    if (de_cost_single[j] > deCostMax) {
                        deCostMax = de_cost_single[j];
                    }
                }

                if (deCostMax < currentCost) {
                // if (de_cost_single[i * beta] < currentCost) {
                    boolean currentBetter = false;
                    Set<Integer> uniqueValues = new HashSet<>();

                    for (int j = 0; j < x_length; j++) {
                        int currentNumber = (x[j] >> (i * beta)) & ((1 << beta) - 1);
                        uniqueValues.add(currentNumber);

                        if (uniqueValues.size() >= threshold[beta - 1]) {
                            currentBetter = true;
                            break;
                        }
                    }

                    if (!currentBetter) {
                        int deCost = x_length * bitWidth(uniqueValues.size()) + uniqueValues.size() * beta;

                        if (deCost < currentCost) {
                            currentCost = deCost;

                            encodingTypeTemp[i] = 2;
                        }
                    }

                }

                cost += currentCost;
            }

            // System.out.println("cost: " + cost);

            if (cost < cMin) {
                cMin = cost;
                betaBest = beta;

                System.arraycopy(encodingTypeTemp, 0, encodingType, 0, l);
            }
        }

        // System.out.println("betaBest: " + betaBest);

        return betaBest;
    }

    public static int SubcolumnEncoder(int[] list, int encode_pos, byte[] encoded_result, int[] beta, int block_size, int[] encodingType) {
        int list_length = list.length;
        int maxValue = 0;
        for (int k : list) {
            if (k > maxValue) {
                maxValue = k;
            }
        }

        // System.out.println("maxValue: " + maxValue);

        int m = bitWidth(maxValue);

        intByte2Bytes(m, encode_pos, encoded_result);
        encode_pos += 1;

        if (m == 0) {
            // System.out.println("All zero list.");
            return encode_pos;
        }

        int l;

        l = (m + beta[0] - 1) / beta[0];

        int[] bitWidthList = new int[l];

        int[][] subcolumnList = new int[l][list_length];

        intByte2Bytes(beta[0], encode_pos, encoded_result);
        encode_pos += 1;

        int bw = bitWidth(block_size);
        int mask = (1 << beta[0]) - 1;

        for (int i = 0; i < l; i++) {
            int maxValuePart = 0;
            int shiftAmount = i * beta[0];
            for (int j = 0; j < list_length; j++) {
                subcolumnList[i][j] = (list[j] >> shiftAmount) & mask;
                if (subcolumnList[i][j] > maxValuePart) {
                    maxValuePart = subcolumnList[i][j];
                }
            }
            bitWidthList[i] = bitWidth(maxValuePart);
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

        // int m = encoded_result[encode_pos];
        int m = bytes2Integer(encoded_result, encode_pos, 1);
        encode_pos += 1;

        if (m == 0) {
            return encode_pos;
        }

        int bw = bitWidth(block_size);

        int beta = bytes2Integer(encoded_result, encode_pos, 1);
        encode_pos += 1;

        int l = (m + beta - 1) / beta;

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

        for (int i = 0; i < l; i++) {
            int shiftAmount = i * beta;
            for (int j = 0; j < list_length; j++) {
                list[j] |= subcolumnList[i][j] << shiftAmount;
            }
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

    public static int BlockEncoder(int[] data, int block_index, int block_size, int remainder,
            int encode_pos, byte[] encoded_result, int[] beta, long[] betaTime,
        long[] encodeTime) {
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

            int[] encodingType = new int[m];

            long s1 = System.nanoTime();
    beta[0] = Subcolumn(data_delta, remainder, m, block_size, encodingType);
    long e1 = System.nanoTime();
    betaTime[0] += (e1 - s1);


            long s2 = System.nanoTime();
        encode_pos = SubcolumnEncoder(data_delta, encode_pos,
                encoded_result, beta, block_size, encodingType);
            long e2 = System.nanoTime();
    encodeTime[0] += (e2 - s2);

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

    public static int Encoder(int[] data, int block_size, byte[] encoded_result, long[] betaTime,
        long[] encodeTime) {
        int data_length = data.length;
        int encode_pos = 0;

        int2Bytes(data_length, encode_pos, encoded_result);
        encode_pos += 4;

        int2Bytes(block_size, encode_pos, encoded_result);
        encode_pos += 4;

        int num_blocks = data_length / block_size;

        int remainder = data_length % block_size;

        int[] beta = new int[1];
        beta[0] = 2;

        for (int i = 0; i < num_blocks; i++) {
            encode_pos = BlockEncoder(data, i, block_size, block_size, encode_pos, encoded_result, beta, betaTime, encodeTime);
        }

        if (remainder <= 3) {
            for (int i = 0; i < remainder; i++) {
                int value = data[num_blocks * block_size + i];
                int2Bytes(value, encode_pos, encoded_result);
                encode_pos += 4;
            }
        } else {
            encode_pos = BlockEncoder(data, num_blocks, block_size, remainder, encode_pos,
                    encoded_result, beta, betaTime, encodeTime);
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
        String parent_dir = "D://github/xjz17/subcolumn/";

        String input_parent_dir = parent_dir + "dataset/";

        String output_parent_dir = parent_dir + "result/";

        String outputPath = output_parent_dir + "subcolumn_dictionary2.csv";

        int block_size = 512;

        int repeatTime = 500;

        CsvWriter writer = new CsvWriter(outputPath, ',', StandardCharsets.UTF_8);
        writer.setRecordDelimiter('\n');

        String[] head = {
                "Dataset",
                "Encoding Algorithm",
                "Encoding Time",
                "Beta Selection Time",
                "Subcolumn Encode Time",
                "Decoding Time",
                "Points",
                "Compressed Size",
                "Compression Ratio"
        };
        writer.writeRecord(head);

        File directory = new File(input_parent_dir);
        File[] csvFiles = directory.listFiles((dir, name) -> name.endsWith(".csv"));

        for (File file : csvFiles) {
            String datasetName = extractFileName(file.toString());
            System.out.println(datasetName);

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

            long betaTime = 0;
            long subcolumnEncodeTime = 0;

            int length = 0;

            long[] betaTimeArr = new long[1];
            long[] subcolumnEncodeTimeArr = new long[1];

            long s = System.nanoTime();
            for (int repeat = 0; repeat < repeatTime; repeat++) {

                betaTimeArr[0] = 0;
                subcolumnEncodeTimeArr[0] = 0;
                length = Encoder(data2_arr, block_size, encoded_result, betaTimeArr, subcolumnEncodeTimeArr);

                betaTime += betaTimeArr[0];
                subcolumnEncodeTime += subcolumnEncodeTimeArr[0];
            }

            long e = System.nanoTime();
            encodeTime += ((e - s) / repeatTime);
            compressed_size += length;

            betaTime /= repeatTime;
            subcolumnEncodeTime /= repeatTime;

            double ratioTmp;

            ratioTmp = compressed_size / (double) (data1.size() * Long.BYTES);

            ratio += ratioTmp;

            System.out.println("Decode");

            int[] data2_arr_decoded = new int[data2_arr.length];

            s = System.nanoTime();

            for (int repeat = 0; repeat < repeatTime; repeat++) {
                data2_arr_decoded = Decoder(encoded_result);
            }

            e = System.nanoTime();
            decodeTime += ((e - s) / repeatTime);

            String[] record = {
                    datasetName,
                    "Sub-column",
                    String.valueOf(encodeTime),
                    String.valueOf(betaTime),
                    String.valueOf(subcolumnEncodeTime),
                    String.valueOf(decodeTime),
                    String.valueOf(data1.size()),
                    String.valueOf(compressed_size),
                    String.valueOf(ratio)
            };
            writer.writeRecord(record);
            System.out.println(ratio);
        }

        writer.close();
    }

}
