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

import static org.junit.Assert.assertEquals;

public class SubcolumnOnValuesTest {

    public static int bitWidth(int value) {
        if (value == 0)
            return 1;
        return 32 - Integer.numberOfLeadingZeros(value);
    }

    /**
     * Bit width for one signed int in a block: non-negative values use leading-zero
     * trimmed width; negative values use full 32 bits (two's complement).
     */
    public static int valueBitWidth(int value) {
        if (value < 0) {
            return 32;
        }
        return bitWidth(value);
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
        // remaining bits for the current unfinished Integer
        int leftBit = 0;

        while (valueIdx < 8 + offset) {
            // buffer is used for saving 32 bits as a part of result
            int buffer = 0;
            // remaining size of bits in the 'buffer'
            int leftSize = 32;

            // encode the left bits of current Integer to 'buffer'
            if (leftBit > 0) {
                buffer |= (values[valueIdx] << (32 - leftBit));
                leftSize -= leftBit;
                leftBit = 0;
                valueIdx++;
            }

            while (leftSize >= width && valueIdx < 8 + offset) {
                // encode one Integer to the 'buffer'
                buffer |= (values[valueIdx] << (leftSize - width));
                leftSize -= width;
                valueIdx++;
            }
            // If the remaining space of the buffer can not save the bits for one Integer,
            if (leftSize > 0 && valueIdx < 8 + offset) {
                // put the first 'leftSize' bits of the Integer into remaining space of the
                // buffer
                buffer |= (values[valueIdx] >>> (width - leftSize));
                leftBit = width - leftSize;
            }

            // put the buffer into the final result
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
        // total bits which have read from 'buf' to 'buffer'. i.e.,
        // number of available bits to be decoded.
        int totalBits = 0;
        int valueIdx = 0;

        while (valueIdx < 8) {
            // If current available bits are not enough to decode one Integer,
            // then add next byte from buf to 'buffer' until totalBits >= width
            while (totalBits < width) {
                buffer = (buffer << 8) | (encoded[byteIdx] & 0xFF);
                byteIdx++;
                totalBits += 8;
            }

            // If current available bits are enough to decode one Integer,
            // then decode one Integer one by one until left bits in 'buffer' is
            // not enough to decode one Integer.
            while (totalBits >= width && valueIdx < 8) {
                // result_list.add((int) (buffer >>> (totalBits - width)));
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
        // ArrayList<Integer> result_list = new ArrayList<>();
        // int[] result_list = new int[num_values];
        int block_num = num_values / 8;
        int remainder = num_values % 8;

        for (int i = 0; i < block_num; i++) { // bitpacking
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

    public static int Subcolumn(int[] x, int x_length, int m, int block_size) {

        int betaBest = 1;

        int cMin = Integer.MAX_VALUE;

        int[] bpe_cost_single = new int[m];
        int[] rle_cost_single = new int[m];
        int[] de_cost_single = new int[m];

        int cost0 = 0;

        for (int i = 0; i < m; i++) {
            int current_value = (x[0] >> i) & 1;

            int count = 1;

            de_cost_single[i] = 1;

            for (int j = 0; j < x_length; j++) {

                // if (count * (1 + (int) Math.ceil(Math.log(x_length))) >= x_length) {
                // rle_cost_single[i] = x_length + 1;
                // break;
                // }

                int subcolumn_ij = (x[j] >> i) & 1;
                if (subcolumn_ij == 1) {
                    bpe_cost_single[i] = x_length;
                }

                if (subcolumn_ij != current_value) {
                    count++;
                    current_value = subcolumn_ij;
                    de_cost_single[i] = x_length + 2 * (1 + 1);
                }

            }

            rle_cost_single[i] = count * (1 + (int) Math.ceil(Math.log(x_length)));

            cost0 += Math.min(bpe_cost_single[i], Math.min(rle_cost_single[i], de_cost_single[i]));
        }

        // int[] beta_list = {1, 2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31};
        // int[] beta_list = { 1, 2, 3, 5, 7, 11 };
        // int[] beta_list = { 1, 2, 3, 4 };
        int[] beta_list = { 2, 3, 4 };

        int bw = bitWidth(block_size);

        int[] bitWidthListList = new int[m];

        for (int beta : beta_list) {
            if (beta > m) {
                break;
            }
            // System.out.println("beta: " + beta);

            int l = (m + beta - 1) / beta;

            // System.out.println("l: " + l);

            // int[][] subcolumnList = new int[l][x_length];

            int cost = 0;

            // for (int i = 0; i < l; i++) {
            // int maxValuePart = 0;
            // for (int j = 0; j < x_length; j++) {
            // subcolumnList[i][j] = (x[j] >> (i * beta)) & ((1 << beta) - 1);
            // if (subcolumnList[i][j] > maxValuePart) {
            // maxValuePart = subcolumnList[i][j];
            // }
            // }
            // bitWidthListList[i] = bitWidth(maxValuePart);
            // }

            for (int i = 0; i < l; i++) {
                // int bpCost = bitWidthListList[i] * x_length;

                // int bpCost = bpe_cost_single[i * beta] * beta;
                int beta_start = (Math.min(m - 1, (i + 1) * beta - 1));
                while (beta_start - 1 >= i * beta && bpe_cost_single[beta_start - 1] == 0) {
                    beta_start--;
                }

                int bpCost = bpe_cost_single[beta_start] * (beta_start - i * beta + 1);

                int rleCost = 0;

                // int lowestBitIndex = 0;
                // int currentLowestBit = subcolumnList[i][0] & 1;

                // for (int j = 1; j < x_length; j++) {
                // int lowestBit = subcolumnList[i][j] & 1; // 获取当前元素的最低位
                // if (lowestBit != currentLowestBit) {
                // lowestBitIndex++;
                // currentLowestBit = lowestBit;
                // }
                // }

                // if (bw * lowestBitIndex + bitWidthListList[i] * lowestBitIndex >= bpCost) {
                // cost += bpCost;
                // continue;
                // }

                int index = 0;

                boolean bpBest = false;

                // int count = 1;
                // int currentNumber = subcolumnList[i][0];
                int currentNumber = (x[0] >> (i * beta)) & ((1 << beta) - 1);

                for (int j = 1; j < x_length; j++) {
                    int currentNumber_j = (x[j] >> (i * beta)) & ((1 << beta) - 1);
                    if (currentNumber_j != currentNumber) {
                        index++;
                        currentNumber = currentNumber_j;
                    }
                    if (bw * index + bitWidth(x_length) * index >= bpCost) {
                        bpBest = true;
                        break;
                    }

                    // if (subcolumnList[i][j] != currentNumber) {
                    // index++;
                    // currentNumber = subcolumnList[i][j];
                    // }

                    // if (bw * index + bitWidthListList[i] * index >= bpCost) {
                    // bpBest = true;
                    // break;
                    // }
                }

                if (bpBest) {
                    cost += bpCost;
                    continue;
                }

                index++;

                // System.out.println("index: " + index);

                rleCost = bw * index + bitWidth(x_length) * index;

                // System.out.println("bpCost: " + bpCost + " rleCost: " + rleCost);

                if (bpCost <= rleCost) {
                    cost += bpCost;
                } else {
                    cost += rleCost;
                }
            }

            // System.out.println("cost: " + cost);

            if (cost < cMin) {
                cMin = cost;
                betaBest = beta;
            }
        }

        return betaBest;
    }

    public static int SubcolumnEncoder(int[] list, int encode_pos, byte[] encoded_result, int[] beta, int block_size) {
        int list_length = list.length;
        int m = 1;
        for (int k : list) {
            int w = valueBitWidth(k);
            if (w > m) {
                m = w;
            }
        }

        intByte2Bytes(m, encode_pos, encoded_result);
        encode_pos += 1;

        if (m == 0) {
            return encode_pos;
        }

        // int[] bitWidthList = new int[m];
        // int[][] subcolumnList = new int[m][list_length];

        int l;

        // int betaBest = beta[0];
        // byte betaBest = (byte) beta[0];

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

        int[] encodingType = new int[l];

        // encoded_result 预留大小为 (l + 7) * 2 / 8 的大小，存储每个分列的类型
        int preTypePos = encode_pos;
        encode_pos += (l + 3) / 4;

        for (int i = l - 1; i >= 0; i--) {
            // 对于每个分列，计算使用 bit packing 还是 rle
            int bpCost = bitWidthList[i] * list_length;
            int rleCost = 0;

            int previous = subcolumnList[i][0];
            int index = 0;

            // uniqueValues.add(previous);

            for (int j = 1; j < list_length; j++) {
                int currentNumber = subcolumnList[i][j];

                // if(currentNumber == 6){
                // System.out.println("currentNumber == 6 && i==0");
                // System.out.println(uniqueValues);
                // }
                if (currentNumber != previous) {
                    index++;
                    previous = currentNumber;
                }

                if (bw * index + bitWidthList[i] * index >= bpCost) {
                    break;
                }
            }
            Set<Integer> uniqueValues = new HashSet<>();
            for (int j = 0; j < list_length; j++) {
                int currentNumber = subcolumnList[i][j];
                uniqueValues.add(currentNumber);
            }
            int cardinality = uniqueValues.size();

            index++;

            rleCost = bw * index + bitWidthList[i] * index;

            if (cardinality < Math.pow(2, bitWidthList[i] - 1)) {
                // test dictionary encoding
                int dict_bit_width = bitWidth(cardinality);
                int dicCost = dict_bit_width * list_length + cardinality * (bitWidthList[i] + dict_bit_width);
                if (dicCost < rleCost && dicCost < bpCost) {
                    // if dictionary encoding
                    // int dict_bit_width = bitWidth(cardinality) ;
                    encodingType[i] = 2;

                    // System.out.println(uniqueValues);
                    List<Integer> sortedUnique = new ArrayList<>(uniqueValues);
                    Collections.sort(sortedUnique);
                    Map<Integer, Integer> valueToCode = new HashMap<>();
                    int[] dict_key_list = new int[cardinality];
                    // int[] dict_value_list = new int[cardinality];
                    for (int j = 0; j < cardinality; j++) {
                        valueToCode.put(sortedUnique.get(j), j);
                        dict_key_list[j] = sortedUnique.get(j);
                        // dict_value_list[j] = j;
                    }
                    // System.out.println(valueToCode);
                    // System.out.println(list_length);
                    // System.out.println(beta[0]);
                    // System.out.println(Arrays.toString(subcolumnList[i]));
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
                    // encode_pos = bitPacking(dict_value_list, dict_bit_width, encode_pos,
                    // encoded_result, cardinality);

                    encode_pos = bitPacking(subcolumnList[i], dict_bit_width, encode_pos, encoded_result, list_length);
                    continue;
                }
            }

            if (bpCost <= rleCost) {
                encodingType[i] = 0;

                encode_pos = bitPacking(subcolumnList[i], bitWidthList[i], encode_pos, encoded_result, list_length);

            } else {
                encodingType[i] = 1;

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

        for (int i = l - 1; i >= 0; i--) {
            int type = encodingType[i];
            int bitWidth = bitWidthList[i];
            if (type == 0) {
                encode_pos = decodeBitPacking(encoded_result, encode_pos, bitWidth, list_length,
                        subcolumnList[i]);
            } else if (type == 1) {
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
            } else {
                int cardinality = ((encoded_result[encode_pos] & 0xFF) << 8) | (encoded_result[encode_pos + 1] & 0xFF);
                encode_pos += 2;
                int dict_bit_width = bitWidth(cardinality);
                int[] dict_key_list = new int[cardinality];
                int[] dict_value_list = new int[cardinality];

                for (int j = 0; j < cardinality; j++) {
                    dict_value_list[j] = j;
                }

                encode_pos = decodeBitPacking(encoded_result, encode_pos, bitWidthList[i], cardinality, dict_key_list);
                // encode_pos = decodeBitPacking(encoded_result, encode_pos, dict_bit_width,
                // cardinality, dict_value_list);

                encode_pos = decodeBitPacking(encoded_result, encode_pos, dict_bit_width, list_length,
                        subcolumnList[i]);
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

    public static int BlockEncoder(int[] data, int block_index, int block_size, int remainder,
            int encode_pos, byte[] encoded_result, int[] beta) {
        int[] data_block = new int[remainder];
        int base = block_index * block_size;
        for (int j = 0; j < remainder; j++) {
            data_block[j] = data[base + j];
        }

        if (block_index == 0) {
            int m = 1;
            for (int j = 0; j < remainder; j++) {
                int w = valueBitWidth(data_block[j]);
                if (w > m) {
                    m = w;
                }
            }

            beta[0] = Subcolumn(data_block, remainder, m, block_size);
        }

        encode_pos = SubcolumnEncoder(data_block, encode_pos,
                encoded_result, beta, block_size);

        return encode_pos;
    }

    public static int BlockDecoder(byte[] encoded_result, int block_index, int block_size, int remainder,
            int encode_pos, int[] data) {
        int[] block_data = new int[remainder];

        encode_pos = SubcolumnDecoder(encoded_result, encode_pos,
                block_data, block_size);

        for (int i = 0; i < remainder; i++) {
            data[block_index * block_size + i] = block_data[i];
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

        int[] beta = new int[1];
        beta[0] = 2;

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

        // System.out.println("beta: " + beta[0]);

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
        // 查找小数点的位置
        int decimalIndex = str.indexOf(".");

        // 如果没有小数点，精度为0
        if (decimalIndex == -1) {
            return 0;
        }

        // 获取小数点后的部分并返回其长度
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
        String parent_dir = "/Users/xiaojinzhao/Documents/GitHub/subcolumn/"; // "D:/github/xjz17/subcolumn/";
        // String parent_dir = "D:/encoding-subcolumn/";

        String input_parent_dir = parent_dir + "dataset/";

        String output_parent_dir = parent_dir + "result/"; // ""D:/encoding-subcolumn/result/";
        // String output_parent_dir = parent_dir + "result/";

        String outputPath = output_parent_dir + "subcolumn_dictionary_on_values.csv";

        // int block_size = 512;
        int block_size = 512;

        // int repeatTime = 100;
        int repeatTime = 500;

        // repeatTime = 1;

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

        File directory = new File(input_parent_dir);
        // File[] csvFiles = directory.listFiles();
        File[] csvFiles = directory.listFiles((dir, name) -> name.endsWith(".csv"));

        for (File file : csvFiles) {
            String datasetName = extractFileName(file.toString());
            System.out.println(datasetName);
            // if(! datasetName.equals("Stocks-UK")){
            // continue;
            // }

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

            int[] data2_arr_decoded = new int[data2_arr.length];

            s = System.nanoTime();

            for (int repeat = 0; repeat < repeatTime; repeat++) {
                data2_arr_decoded = Decoder(encoded_result);
            }

            e = System.nanoTime();
            decodeTime += ((e - s) / repeatTime);

            for (int i = 0; i < data2_arr_decoded.length; i++) {
                // assertEquals(data2_arr[i], data2_arr_decoded[i]);
            }

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
            System.out.println(ratio);
        }

        writer.close();
    }

}
