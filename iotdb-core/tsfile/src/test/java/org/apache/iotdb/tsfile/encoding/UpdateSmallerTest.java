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
import java.util.HashMap;
import java.util.List;
import java.util.Random;

public class UpdateSmallerTest {

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
    public static int decodeBitPacking(byte[] encoded, int bytePos, int bitWidth, int numValues, int[] result) {
        // 参数检查（可选，但推荐在 debug 时打开）
//        if (bitWidth <= 0 || bitWidth > 32) throw new IllegalArgumentException("bitWidth must be 1..32");
        if (numValues == 0) return bytePos;

        final long mask = (bitWidth == 32) ? 0xFFFFFFFFL : ((1L << bitWidth) - 1L);

        int valuesWritten = 0;
        int byteIndex = bytePos;
        long bitBuffer = 0L;      // buffer 存放尚未消费的 bits（放在低位或高位都可以，这里用“高位拼入、右移取值”方式）
        int bitsInBuffer = 0;     // buffer 中可用的位数

        int fullBlocks = numValues >>> 3; // 每 block 8 个值
        int rem = numValues & 7;

        // 处理每个 block（每 block 有 8 个值）
        for (int b = 0; b < fullBlocks; b++) {
            for (int v = 0; v < 8; v++) {
                // 保证 buffer 中至少有 bitWidth 位可供取值
                while (bitsInBuffer < bitWidth) {
                    // 按大端拼入：左移 8 位再或入下一个字节
                    bitBuffer = (bitBuffer << 8) | (encoded[byteIndex++] & 0xFFL);
                    bitsInBuffer += 8;
                }
                int shift = bitsInBuffer - bitWidth; // 从高位取出这次要的 bitWidth 位
                result[valuesWritten++] = (int) ((bitBuffer >>> shift) & mask);
                // 删除已消费的高位 bits
                bitsInBuffer -= bitWidth;
                if (bitsInBuffer == 0) {
                    bitBuffer = 0L;
                } else {
                    // 保留低 bitsInBuffer 位（把高位已经消费掉）
                    long keepMask = (bitsInBuffer == 64) ? ~0L : ((1L << bitsInBuffer) - 1L);
                    bitBuffer &= keepMask;
                }
            }
        }

        // 处理剩余值
        for (int v = 0; v < rem; v++) {
            while (bitsInBuffer < bitWidth) {
                bitBuffer = (bitBuffer << 8) | (encoded[byteIndex++] & 0xFFL);
                bitsInBuffer += 8;
            }
            int shift = bitsInBuffer - bitWidth;
            result[valuesWritten++] = (int) ((bitBuffer >>> shift) & mask);
            bitsInBuffer -= bitWidth;
            if (bitsInBuffer == 0) {
                bitBuffer = 0L;
            } else {
                long keepMask = (bitsInBuffer == 64) ? ~0L : ((1L << bitsInBuffer) - 1L);
                bitBuffer &= keepMask;
            }
        }

        // 返回已消费到的字节索引（下一个可读字节）
        return byteIndex;
    }

//    public static int decodeBitPacking(
//            byte[] encoded, int decode_pos, int bit_width, int num_values, int[] result_list) {
//        // ArrayList<Integer> result_list = new ArrayList<>();
//        // int[] result_list = new int[num_values];
//        int block_num = num_values>>3;
//        int remainder = num_values % 8;
//
//        for (int i = 0; i < block_num; i++) { // bitpacking
//            unpack8Values(encoded, decode_pos, bit_width, result_list, i * 8);
//            decode_pos += bit_width;
//        }
//
//        decode_pos *= 8;
//
//        for (int i = 0; i < remainder; i++) {
//            result_list[block_num * 8 + i] = bytesToInt(encoded, decode_pos, bit_width);
//            decode_pos += bit_width;
//        }
//
//        return (decode_pos + 7) >>3;
//    }

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

            int[][] subcolumnList = new int[l][x_length];

            int cost = 0;

            for (int i = 0; i < l; i++) {
                int maxValuePart = 0;
                for (int j = 0; j < x_length; j++) {
                    subcolumnList[i][j] = (x[j] >> (i * beta)) & ((1 << beta) - 1);
                    if (subcolumnList[i][j] > maxValuePart) {
                        maxValuePart = subcolumnList[i][j];
                    }
                }
                bitWidthListList[i] = bitWidth(maxValuePart);
            }

            for (int i = 0; i < l; i++) {
                int bpCost = bitWidthListList[i] * x_length;
                int rleCost = 0;

                // int count = 1;
                int currentNumber = subcolumnList[i][0];

                int index = 0;

                boolean bpBest = false;

                for (int j = 1; j < x_length; j++) {
                    if (subcolumnList[i][j] != currentNumber) {
                        index++;
                        currentNumber = subcolumnList[i][j];
                    }

                    if (bw * index + bitWidthListList[i] * index >= bpCost) {
                        bpBest = true;
                        break;
                    }
                }

                if (bpBest) {
                    cost += bpCost;
                    continue;
                }

                index++;

                // System.out.println("index: " + index);

                rleCost = bw * index + bitWidthListList[i] * index;

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
        int maxValue = 0;
        for (int i = 0; i < list_length; i++) {
            if (list[i] > maxValue) {
                maxValue = list[i];
            }
        }

        int m = bitWidth(maxValue);

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

        // encoded_result 预留大小为 (l + 7) / 8 的大小，存储每个分列的类型
        int preTypePos = encode_pos;
        encode_pos += (l + 7) / 8;

        for (int i = l - 1; i >= 0; i--) {
            // 对于每个分列，计算使用 bit packing 还是 rle
            int bpCost = bitWidthList[i] * list_length;
            int rleCost = 0;

            int previous = subcolumnList[i][0];
            int index = 0;

            for (int j = 1; j < list_length; j++) {
                int currentNumber = subcolumnList[i][j];
                if (currentNumber != previous) {
                    index++;
                    previous = currentNumber;
                }

                if (bw * index + bitWidthList[i] * index >= bpCost) {
                    break;
                }
            }

            index++;

            rleCost = bw * index + bitWidthList[i] * index;

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

        preTypePos = bitPacking(encodingType, 1, preTypePos, encoded_result, l);

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

        encode_pos = decodeBitPacking(encoded_result, encode_pos, 1, l, encodingType);

        for (int i = l - 1; i >= 0; i--) {
            int type = encodingType[i];
            int bitWidth = bitWidthList[i];
            if (type == 0) {
                encode_pos = decodeBitPacking(encoded_result, encode_pos, bitWidth, list_length,
                        subcolumnList[i]);
            } else {
                int index = ((encoded_result[encode_pos] & 0xFF) << 8) | (encoded_result[encode_pos + 1] & 0xFF);

                encode_pos += 2;

//                System.out.println("------------------------------------------------");

                int[] run_length = new int[index];
                int[] rle_values = new int[index];
//                System.out.println(encode_pos);

                encode_pos = decodeBitPacking(encoded_result, encode_pos, bw, index, run_length);
//                System.out.println(encode_pos);
                encode_pos = decodeBitPacking(encoded_result, encode_pos, bitWidth, index, rle_values);
//                System.out.println(encode_pos);

                int currentIndex = 0;
                for (int j = 0; j < index; j++) {
                    int endPos = run_length[j];
                    int value = rle_values[j];
                    while (currentIndex < endPos) {
                        subcolumnList[i][currentIndex] = value;
                        currentIndex++;
                    }
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
                                   int encode_pos, byte[] encoded_result, int[] beta) {
        int[] min_delta = new int[3];

        int[] data_delta = getAbsDeltaTsBlock(data, block_index, block_size,
                remainder, min_delta);

        int2Bytes(min_delta[0], encode_pos, encoded_result);
        encode_pos += 4;

        if (block_index == 0) {
            int maxValue = 0;
            for (int j = 0; j < remainder; j++) {
                if (data_delta[j] > maxValue) {
                    maxValue = data_delta[j];
                }
            }
            int m = bitWidth(maxValue);

            beta[0] = Subcolumn(data_delta, remainder, m, block_size);
        }

        encode_pos = SubcolumnEncoder(data_delta, encode_pos,
                encoded_result, beta, block_size);

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

    public static int Encoder(int[] data, int block_size, byte[] encoded_result, int[] beta) {
        int data_length = data.length;
        int encode_pos = 0;

        int2Bytes(data_length, encode_pos, encoded_result);
        encode_pos += 4;

        int2Bytes(block_size, encode_pos, encoded_result);
        encode_pos += 4;

        int num_blocks = data_length / block_size;
        int remainder = data_length % block_size;

//        int[] beta = new int[1];
        beta[0] = 2;

        for (int i = 0; i < num_blocks; i++) {
            encode_pos = BlockEncoder(data, i, block_size, block_size, encode_pos, encoded_result, beta);
        }

        int maximum = Integer.MIN_VALUE;
        for (int i = 0; i < remainder; i++) {
            int value = data[num_blocks * block_size + i];
            if(maximum<value) maximum = value;
        }
        int max_bit_width = bitWidth(maximum);
        int max_remainder = max_bit_width % beta[0];
        int max_m = max_bit_width / beta[0];
        if(max_remainder != 0){
            max_m += 1;
        }

        int lower_bound = (int) (Math.pow(2,max_m * beta[0]));
        beta[1] = lower_bound;
        beta[2] = encode_pos;
        beta[3] = remainder;

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
    public static boolean compareBits(byte[] encoded_result, int new_decode_pos, int index, int bitWidth, int value) {
        // 计算起始位位置
        int bit_pos = bitWidth * index;

        // 计算起始字节和位偏移
        int startByte = new_decode_pos + (bit_pos / 8);
        int bitOffset = bit_pos % 8;

        // 确保值不会超出指定位数范围
        int maskedValue = value & ((1 << bitWidth) - 1);

        // 读取指定位段
        int readValue = 0;
        int bitsRemaining = bitWidth;
        int currentByteIndex = startByte;
        int currentBitOffset = bitOffset;

        while (bitsRemaining > 0 && currentByteIndex < encoded_result.length) {
            // 计算当前字节中可以读取的位数
            int bitsInThisByte = Math.min(8 - currentBitOffset, bitsRemaining);

            // 从当前字节提取指定位
            int byteValue = encoded_result[currentByteIndex] & 0xFF;
            int extractedBits = (byteValue >> (8 - currentBitOffset - bitsInThisByte)) & ((1 << bitsInThisByte) - 1);

            // 将提取的位添加到结果中
            readValue = (readValue << bitsInThisByte) | extractedBits;

            // 更新计数器
            bitsRemaining -= bitsInThisByte;
            currentByteIndex++;
            currentBitOffset = 0; // 后续字节从第0位开始
        }

        // 比较读取的值与给定值的低位
        return readValue == maskedValue;
    }
    public static void updateBits(byte[] encoded_result, int new_decode_pos, int bit_pos, int bitWidth, int value) {
        // 计算起始字节和位偏移
        int startByte = new_decode_pos + (bit_pos / 8);
        int bitOffset = bit_pos % 8;

        // 确保值不会超出指定位数范围
        int maskedValue = value & ((1 << bitWidth) - 1);

        // 处理跨字节更新
        int bitsRemaining = bitWidth;
        int currentByteIndex = startByte;
        int currentBitOffset = bitOffset;

        while (bitsRemaining > 0) {
            // 计算当前字节中可以更新的位数
            int bitsInThisByte = Math.min(8 - currentBitOffset, bitsRemaining);

            // 创建掩码：清除目标位
            int clearMask = ~(((1 << bitsInThisByte) - 1) << (8 - currentBitOffset - bitsInThisByte));

            // 准备要设置的值（移位到正确位置）
            int valuePart = (maskedValue << (bitWidth - bitsRemaining)) >>> (bitWidth - bitsInThisByte);
            int shiftedValue = valuePart << (8 - currentBitOffset - bitsInThisByte);

            // 更新当前字节
            encoded_result[currentByteIndex] = (byte) ((encoded_result[currentByteIndex] & clearMask) | shiftedValue);

            // 更新计数器
            bitsRemaining -= bitsInThisByte;
            currentByteIndex++;
            currentBitOffset = 0; // 后续字节从第0位开始
        }
    }
    @Test
    public void testQuery() throws IOException {
        String parent_dir = "/Users/xiaojinzhao/Documents/GitHub/subcolumn/"; //"D:/github/xjz17/subcolumn/";
        // String parent_dir = "D:/encoding-subcolumn/";

        String input_parent_dir = parent_dir + "dataset/";
        String output_parent_dir = parent_dir + "result/update/";
        // String output_parent_dir = parent_dir + "result/query_vs_beta/";

        int block_size = 512;

        HashMap<String, Integer> updateRange = new HashMap<>();

        updateRange.put("Bird-migration", 2500000);
        updateRange.put("Bitcoin-price", 160000000);
        updateRange.put("City-temp", 480);
        updateRange.put("Dewpoint-temp", 9500);
        updateRange.put("IR-bio-temp", -300);
        updateRange.put("PM10-dust", 1000);
        updateRange.put("Stocks-DE", 40000);
        updateRange.put("Stocks-UK", 20000);
        updateRange.put("Stocks-USA", 5000);
        updateRange.put("Wind-Speed", 50);
        updateRange.put("Wine-Tasting", 0);

        int repeatTime = 500;

        // repeatTime = 1;

        List<String> integerDatasets = new ArrayList<>();
        integerDatasets.add("Wine-Tasting");

//        int beta = 1;
        String outputPath = output_parent_dir + "subcolumn_update_smaller.csv";

        CsvWriter writer = new CsvWriter(outputPath, ',', StandardCharsets.UTF_8);
        writer.setRecordDelimiter('\n');

        String[] head = {
                "Dataset",
                "Encoding Algorithm",
                "Encoding Time",
//                "Insert Time", // 拼接原始值的时间
                "Insert Time with Sub-column", //拼接压缩后的数据的时间
                "Points",
                "Remaining Points",
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
            if(datasetName.equals("POI-lon") || datasetName.equals("POI-lat"))
                continue;
//            if(!datasetName.equals("Stocks-USA")) continue;

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
                if (cur_decimal > max_decimal)
                    max_decimal = cur_decimal;
                data1.add(Float.valueOf(f_str));
            }
            inputStream.close();
            int[] data2_arr = new int[data1.size()+1];
            int max_mul = (int) Math.pow(10, max_decimal);
            for (int i = 0; i < data1.size(); i++) {
                data2_arr[i] = (int) (data1.get(i) * max_mul);
            }

            System.out.println(max_decimal);
            byte[] encoded_result = new byte[data2_arr.length * 4];

            long encodeTime = 0;
            long appendTime = 0;
            long insertGreaterTime = 0;
            double ratio = 0;
            double compressed_size = 0;

            int length = 0;
            int[] beta = new int[4];
            beta[0] = 2;

            long s = System.nanoTime();
            for (int repeat = 0; repeat < repeatTime; repeat++) {
                length = Encoder(data2_arr, block_size, encoded_result,beta);
            }

            long e = System.nanoTime();
            encodeTime += ((e - s) / repeatTime);
            compressed_size += length;

            double ratioTmp;

            if (integerDatasets.contains(datasetName)) {
                ratioTmp = compressed_size / (double) (data1.size() * Integer.BYTES);
            } else {
                ratioTmp = compressed_size / (double) (data1.size() * Long.BYTES);
            }

            System.out.println("Update");

            int data_length = data2_arr.length;
            int remainder = data_length % block_size;
            if(remainder < 4){
                continue;
            }
            Decoder(encoded_result);

//            s = System.nanoTime();

            int random_lower_bound = beta[1];
//            int[] remaining_values = new int[number_of_insert_values];

//            for (int i = 0; i < number_of_insert_values; i++) {
//                Random random = new Random();
//                int randomNumber = random.nextInt(Integer.MAX_VALUE - random_lower_bound) + random_lower_bound;
//                remaining_values[i] = randomNumber;
//            }
            Random random = new Random();
//            Random random = new Random();
            int randomNumber = random.nextInt(Math.min(Math.max(random_lower_bound/4,16),random_lower_bound));

//            for (int repeat = 0; repeat < repeatTime; repeat++) {
//                data2_arr[num_blocks * block_size + remainder] = randomNumber;
////                for (int i = 0; i < number_of_insert_values; i++) {
////                    data2_arr[num_blocks * block_size + remainder + i] = remaining_values[i];
////                }
////                data2_arr[data1.size()] = updateRange.get(datasetName);
////                UpdateAppendTest.Query(encoded_result, updateRange.get(datasetName));
//            }

//            e = System.nanoTime();
//            appendTime += ((e - s) / repeatTime);

            s = System.nanoTime();

            int new_decode_posencode_pos = length;

            int beta_o = beta[0];
            int remaining_pos = beta[3];

            //
            int new_block_encode_length = beta[2];
            int m = bytes2Integer(encoded_result, new_block_encode_length+4, 1);
            int bitwidth_random = bitWidth(randomNumber);
            int mask = (1 << beta_o) - 1;
            int l = (m + beta_o - 1) / beta_o;
            int number_of_sub_column_random = (bitwidth_random + beta_o - 1) / beta_o;
            int[] bitWidthList = new int[l];
            int bw = bitWidth(block_size);
            int new_decode_pos = decodeBitPacking(encoded_result, new_block_encode_length+6,
                    8, l, bitWidthList);

            int[] add_sub_columns = new int[l];
            for (int i = 0; i < number_of_sub_column_random; i++) {
                int shiftAmount = i * beta_o;
                add_sub_columns[i] = (randomNumber >> shiftAmount) & mask;
//                        int tmp_bit_width = bitWidth(add_sub_columns[i]);
//                        bit_width_add_sub_columns[i] = Math.max(tmp_bit_width, bitWidthList[i]);
//                        is_changed[i]= tmp_bit_width > bitWidthList[i];
            }
            long start_part3 = System.nanoTime();
            int[] encodingType = new int[l];
//                    int[] new_encodingType = new int[l];

           int start_new_decode_pos = decodeBitPacking(encoded_result, new_decode_pos, 1, l, encodingType);
//                    System.arraycopy(encodingType, 0, new_encodingType, 0, l);
//                    System.out.println(Arrays.toString(encodingType));

            long end_part3 = System.nanoTime();
//            part3_time += (end_part3-start_part3);
            long part1_time = 0;
            long part2_time = 0;
//            long part3_time = 0;
            for (int repeat = 0; repeat < repeatTime; repeat++) {
//                int encode_pos = length;


//                    int add_number_of_sub_column =  number_of_sub_column_random - m;


                new_decode_pos = start_new_decode_pos;


                    byte[] new_encoded_result = new byte[(remainder+1)*16];
                    int pos_of_new_encoded_result = 0;


                    for(int j=l-1;j>=number_of_sub_column_random;j--){
                        int bitWidth = bitWidthList[j];
                        if(encodingType[j] == 0){
                            new_decode_pos += ((remaining_pos*bitWidth+7)/8);
                        }
                        else {
                            int index = ((encoded_result[new_decode_pos] & 0xFF) << 8) | (encoded_result[new_decode_pos + 1] & 0xFF);
                            new_decode_pos += 2;
                            new_decode_pos += ((bw*index + 7) >>3);
                            new_decode_pos += ((bitWidth*index + 7)>>3);
                        }
                    }

                    for(int j = number_of_sub_column_random-1;j>=0;j--){
                        int bitWidth = bitWidthList[j];
                        if(encodingType[j] == 0){
                            long start = System.nanoTime();
                            // if bit-packing
                            int bit_pos = (remaining_pos-1)*bitWidth;
                            updateBits(encoded_result, new_decode_pos, bit_pos, bitWidth, add_sub_columns[j]);
                            new_decode_pos += ((bit_pos+bitWidth+7) >> 3);
                            long end = System.nanoTime();
                            part1_time += (end-start);
                        }
                        else {
//                            System.out.println(encoded_result[new_decode_pos]);
//                            System.out.println(encoded_result[new_decode_pos+1]);
                            // if rle
                            long start = System.nanoTime();
                            int index = ((encoded_result[new_decode_pos] & 0xFF) << 8) | (encoded_result[new_decode_pos + 1] & 0xFF);

                            new_decode_pos += 2;

                            int pre_new_decode_pos = new_decode_pos;


                            new_decode_pos += (bw*index + 7)/8;
                            if(!compareBits(encoded_result, (new_decode_pos-1),index, bitWidth, add_sub_columns[j]) ){
                                int[] run_length = new int[index];
                                int[] rle_values = new int[index];
//                                new_decode_pos = pre_new_decode_pos;
//                                new_decode_pos = decodeBitPacking(encoded_result, new_decode_pos, bw, index, run_length);
//                                new_decode_pos = decodeBitPacking(encoded_result, new_decode_pos, bitWidth, index, rle_values);
//                                int[] new_run_length = new int[index+1];
//                                int[] new_rle_values = new int[index+1];
//                                System.arraycopy( run_length, 0, new_run_length, 0, index);
//                                System.arraycopy( rle_values, 0, new_rle_values, 0, index);
//                                new_run_length[index] = 1;
//                                new_rle_values[index] = add_sub_columns[j];
//
//                                index ++;
//                                new_encoded_result[pos_of_new_encoded_result] = (byte) (index >> 8);
//                                pos_of_new_encoded_result += 1;
//                                new_encoded_result[pos_of_new_encoded_result] = (byte) (index & 0xFF);
//                                pos_of_new_encoded_result += 1;

//                                pos_of_new_encoded_result = bitPacking(new_run_length, bw, pos_of_new_encoded_result, new_encoded_result, index);
//                                pos_of_new_encoded_result = bitPacking(new_rle_values, bit_width_add_sub_columns[j], pos_of_new_encoded_result, new_encoded_result, index);
                                new_decode_pos += ((bitWidth*index + 7)>>3);
                            }else{
                                new_decode_pos += ((bitWidth*index + 7)>>3);
                            }
                            long end = System.nanoTime();
                            part2_time += (end-start);

                        }
                    }


            }
//            length = encode_pos ;

            e = System.nanoTime();
            insertGreaterTime += ((e - s) / repeatTime);
            System.out.println("part1: "+ part1_time/repeatTime);
            System.out.println("part2: "+ part2_time/repeatTime);
//            System.out.println("part3: "+ part3_time/repeatTime);
            System.out.println("insertSmallerTime: "+ insertGreaterTime);

            String[] record = {
                    datasetName,
                    "Sub-columns",
                    String.valueOf(encodeTime),
//                    String.valueOf(appendTime),
                    String.valueOf(insertGreaterTime),
                    String.valueOf(data1.size()),
                    String.valueOf(remainder),
                    String.valueOf(compressed_size),
                    String.valueOf(ratio)
            };
            writer.writeRecord(record);

//            System.out.println("beta: " + beta);

            System.out.println(ratio);
        }

        writer.close();

    }



}
