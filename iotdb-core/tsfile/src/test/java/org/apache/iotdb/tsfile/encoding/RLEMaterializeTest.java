package org.apache.iotdb.tsfile.encoding;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import javax.management.Query;

import org.junit.Test;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;

import static org.junit.Assert.assertEquals;

public class RLEMaterializeTest {

    public static int bitWidth(int value) {
        return 32 - Integer.numberOfLeadingZeros(value);
    }

    public static int bitWidth(long value) {
        return 64 - Long.numberOfLeadingZeros(value);
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

    public static void longToBytes(long srcNum, byte[] result, int pos, int width) {
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
            srcNum = srcNum & ~(-1L << width);
            if (cnt == 8) {
                index++;
                cnt = 0;
            }
        }
    }

    public static long bytesToLong(byte[] result, int pos, int width) {
        long ret = 0;
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

    public static void pack8Values(
            long[] values, int offset, int width, int encode_pos, byte[] encoded_result) {
        int bufIdx = 0;
        int valueIdx = offset;
        // remaining bits for the current unfinished Long
        int leftBit = 0;

        while (valueIdx < 8 + offset) {
            // buffer is used for saving 64 bits as a part of result
            long buffer = 0;
            // remaining size of bits in the 'buffer'
            int leftSize = 64;

            // encode the left bits of current Long to 'buffer'
            if (leftBit > 0) {
                buffer |= (values[valueIdx] << (64 - leftBit));
                leftSize -= leftBit;
                leftBit = 0;
                valueIdx++;
            }

            while (leftSize >= width && valueIdx < 8 + offset) {
                // encode one Long to the 'buffer'
                buffer |= (values[valueIdx] << (leftSize - width));
                leftSize -= width;
                valueIdx++;
            }
            // If the remaining space of the buffer can not save the bits for one Long
            if (leftSize > 0 && valueIdx < 8 + offset) {
                // put the first 'leftSize' bits of the Long into remaining space of the buffer
                buffer |= (values[valueIdx] >>> (width - leftSize));
                leftBit = width - leftSize;
            }

            // put the buffer into the final result
            for (int j = 0; j < 8; j++) {
                encoded_result[encode_pos] = (byte) ((buffer >>> ((7 - j) * 8)) & 0xFF);
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

    public static void unpack8Values(
            byte[] encoded, int offset, int width, long[] result_list, int result_offset) {
        int byteIdx = offset;
        long buffer = 0;
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
                result_list[result_offset + valueIdx] = buffer >>> (totalBits - width);
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

    public static int bitPacking(long[] numbers, int bit_width, int encode_pos,
            byte[] encoded_result, int num_values) {
        int block_num = num_values / 8;
        int remainder = num_values % 8;

        for (int i = 0; i < block_num; i++) {
            pack8Values(numbers, i * 8, bit_width, encode_pos, encoded_result);
            encode_pos += bit_width;
        }

        encode_pos *= 8;

        for (int i = 0; i < remainder; i++) {
            longToBytes(numbers[block_num * 8 + i], encoded_result, encode_pos, bit_width);
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

    public static int decodeBitPacking(
            byte[] encoded, int decode_pos, int bit_width, int num_values, long[] result_list) {
        int block_num = num_values / 8;
        int remainder = num_values % 8;

        for (int i = 0; i < block_num; i++) {
            unpack8Values(encoded, decode_pos, bit_width, result_list, i * 8);
            decode_pos += bit_width;
        }

        decode_pos *= 8;

        for (int i = 0; i < remainder; i++) {
            result_list[block_num * 8 + i] = bytesToLong(encoded, decode_pos, bit_width);
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

    public static void long2Bytes(long integer, int encode_pos, byte[] cur_byte) {
        cur_byte[encode_pos] = (byte) (integer >> 56);
        cur_byte[encode_pos + 1] = (byte) (integer >> 48);
        cur_byte[encode_pos + 2] = (byte) (integer >> 40);
        cur_byte[encode_pos + 3] = (byte) (integer >> 32);
        cur_byte[encode_pos + 4] = (byte) (integer >> 24);
        cur_byte[encode_pos + 5] = (byte) (integer >> 16);
        cur_byte[encode_pos + 6] = (byte) (integer >> 8);
        cur_byte[encode_pos + 7] = (byte) (integer);
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

    public static long bytes2Long(byte[] encoded, int start, int num) {
        long value = 0;

        for (int i = 0; i < num; i++) {
            value <<= 8;
            int b = encoded[i + start] & 0xFF;
            value |= b;
        }
        return value;
    }

    public static long[] getAbsDeltaTsBlock(
            long[] ts_block,
            int i,
            int block_size,
            int remaining,
            long[] min_delta) {
        long[] ts_block_delta = new long[remaining];

        long value_delta_min = Long.MAX_VALUE;
        long value_delta_max = Long.MIN_VALUE;
        int base = i * block_size;
        int end = i * block_size + remaining;

        for (int j = base; j < end; j++) {
            long cur = ts_block[j];
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

        min_delta[1] = value_delta_max;

        return ts_block_delta;
    }

    public static int BlockEncoder(long[] data, int block_index, int block_size, int remainder,
            int encode_pos, byte[] encoded_result) {

        long[] min_delta = new long[3];

        long[] data_delta = getAbsDeltaTsBlock(data, block_index, block_size,
        remainder, min_delta);

        long2Bytes(min_delta[0], encode_pos, encoded_result);
        encode_pos += 8;

        int maxBitWidth = bitWidth(min_delta[1] - min_delta[0]);

        int2Bytes(maxBitWidth, encode_pos, encoded_result);
        encode_pos += 4;

        // long[] data_delta = new long[remainder];

        // long min_value = Long.MAX_VALUE;
        // long max_value = Long.MIN_VALUE;

        // for (int j = 0; j < remainder; j++) {
        //     data_delta[j] = data[block_index * block_size + j];
        //     if (data_delta[j] < min_value) {
        //         min_value = data_delta[j];
        //     }
        //     if (data_delta[j] > max_value) {
        //         max_value = data_delta[j];
        //     }
        // }

        // int maxBitWidth = bitWidth(max_value);

        // if (min_value < 0) {
        //     maxBitWidth = 64;
        // }

        // int2Bytes(maxBitWidth, encode_pos, encoded_result);
        // encode_pos += 4;

        int bw = bitWidth(remainder);

        int rle_index = 0;
        int[] run_length = new int[remainder];
        long[] rle_values = new long[remainder];

        long previous = data_delta[0];

        for (int j = 1; j < remainder; j++) {
            if (data_delta[j] != previous) {
                run_length[rle_index] = j;
                rle_values[rle_index] = previous;
                rle_index++;
                previous = data_delta[j];
            }
        }

        run_length[rle_index] = remainder;
        rle_values[rle_index] = previous;
        rle_index++;

        int2Bytes(rle_index, encode_pos, encoded_result);
        encode_pos += 4;

        encode_pos = bitPacking(run_length, bw, encode_pos, encoded_result,
                rle_index);

        encode_pos = bitPacking(rle_values, maxBitWidth, encode_pos,
                encoded_result, rle_index);

        return encode_pos;

    }

    public static int BlockDecoder(byte[] encoded_result, int block_index, int block_size, int remainder,
            int encode_pos, long[] data) {
        long[] min_delta = new long[3];

        min_delta[0] = bytes2Long(encoded_result, encode_pos, 8);
        encode_pos += 8;

        int maxBitWidth = bytes2Integer(encoded_result, encode_pos, 4);
        encode_pos += 4;

        int rle_index = bytes2Integer(encoded_result, encode_pos, 4);
        encode_pos += 4;

        int bw = bitWidth(remainder);

        int[] run_length = new int[rle_index];
        long[] rle_values = new long[rle_index];

        encode_pos = decodeBitPacking(encoded_result, encode_pos, bw, rle_index, run_length);
        encode_pos = decodeBitPacking(encoded_result, encode_pos, maxBitWidth, rle_index, rle_values);

        long[] ts_block_delta = new long[remainder];

        int currentIndex = 0;
        for (int i = 0; i < rle_index; i++) {
            int end = run_length[i];
            long value = rle_values[i];
            while (currentIndex < end) {
                ts_block_delta[currentIndex] = value;
                currentIndex++;
            }
        }

        for (int i = 0; i < remainder; i++) {
            data[block_index * block_size + i] = ts_block_delta[i] + min_delta[0];
        }

        return encode_pos;
    }

    public static int Encoder(long[] data, int block_size, byte[] encoded_result) {
        int data_length = data.length;
        int encode_pos = 0;

        int2Bytes(data_length, encode_pos, encoded_result);
        encode_pos += 4;

        int2Bytes(block_size, encode_pos, encoded_result);
        encode_pos += 4;

        int num_blocks = data_length / block_size;

        int remainder = data_length % block_size;

        for (int i = 0; i < num_blocks; i++) {
            encode_pos = BlockEncoder(data, i, block_size, block_size, encode_pos, encoded_result);
        }

        if (remainder <= 3) {
            for (int i = 0; i < remainder; i++) {
                long value = data[num_blocks * block_size + i];
                long2Bytes(value, encode_pos, encoded_result);
                encode_pos += 8;
            }
        } else {
            encode_pos = BlockEncoder(data, num_blocks, block_size, remainder, encode_pos,
                    encoded_result);
        }

        // System.out.println("beta: " + beta[0]);

        return encode_pos;
    }

    public static long[] Decoder(byte[] encoded_result) {
        int encode_pos = 0;

        int data_length = bytes2Integer(encoded_result, encode_pos, 4);
        encode_pos += 4;

        int block_size = bytes2Integer(encoded_result, encode_pos, 4);
        encode_pos += 4;

        int num_blocks = data_length / block_size;

        long[] data = new long[data_length];

        for (int i = 0; i < num_blocks; i++) {
            encode_pos = BlockDecoder(encoded_result, i, block_size, block_size, encode_pos, data);
        }

        int remainder = data_length % block_size;

        if (remainder <= 3) {
            for (int i = 0; i < remainder; i++) {
                data[num_blocks * block_size + i] = bytes2Long(encoded_result, encode_pos, 8);
                encode_pos += 8;
            }
        } else {
            encode_pos = BlockDecoder(encoded_result, num_blocks, block_size, remainder,
                    encode_pos, data);
        }

        return data;
    }

    public static void Query(byte[] encoded_result, long upper_bound, int[] result, int[] result_length) {
        int encode_pos = 0;

        int data_length = ((encoded_result[encode_pos] & 0xFF) << 24) | ((encoded_result[encode_pos + 1] & 0xFF) << 16)
                |
                ((encoded_result[encode_pos + 2] & 0xFF) << 8) | (encoded_result[encode_pos + 3] & 0xFF);
        encode_pos += 4;

        int block_size = ((encoded_result[encode_pos] & 0xFF) << 24) | ((encoded_result[encode_pos + 1] & 0xFF) << 16) |
                ((encoded_result[encode_pos + 2] & 0xFF) << 8) | (encoded_result[encode_pos + 3] & 0xFF);
        encode_pos += 4;

        int num_blocks = data_length / block_size;

        result_length[0] = 0;

        for (int i = 0; i < num_blocks; i++) {
            encode_pos = BlockQueryIndex(encoded_result, i, block_size,
                    block_size, encode_pos, upper_bound,
                    result, result_length);
        }

        int remainder = data_length % block_size;

        if (remainder <= 3) {
            for (int i = 0; i < remainder; i++) {
                long value = bytes2Long(encoded_result, encode_pos, 8);
                encode_pos += 8;
                if (value < upper_bound) {
                    result[result_length[0]] = num_blocks * block_size + i;
                    result_length[0]++;
                }
            }
        } else {
            encode_pos = BlockQueryIndex(encoded_result, num_blocks, block_size,
                    remainder, encode_pos, upper_bound,
                    result, result_length);
        }

    }

    public static int BlockQueryIndex(byte[] encoded_result, int block_index, int block_size, int remainder,
            int encode_pos, long upper_bound, int[] result, int[] result_length) {
        long[] min_delta = new long[3];

        min_delta[0] = bytes2Long(encoded_result, encode_pos, 8);
        encode_pos += 8;

        int maxBitWidth = bytes2Integer(encoded_result, encode_pos, 4);
        encode_pos += 4;

        int rle_index = bytes2Integer(encoded_result, encode_pos, 4);
        encode_pos += 4;

        int bw = bitWidth(remainder);

        int[] run_length = new int[rle_index];
        long[] rle_values = new long[rle_index];

        encode_pos = decodeBitPacking(encoded_result, encode_pos, bw, rle_index, run_length);
        encode_pos = decodeBitPacking(encoded_result, encode_pos, maxBitWidth, rle_index, rle_values);

        long[] ts_block_delta = new long[remainder];

        int currentIndex = 0;
        for (int i = 0; i < rle_index; i++) {
            int end = run_length[i];
            long value = rle_values[i];
            while (currentIndex < end) {
                ts_block_delta[currentIndex] = value;
                currentIndex++;
            }
        }

        for (int i = 0; i < currentIndex; i++) {
            if (ts_block_delta[i] + min_delta[0] < upper_bound) {
                result[result_length[0]] = block_index * block_size + i;
                result_length[0]++;
            }
        }

        return encode_pos;

    }

    public static double computeSelectivity(long len1_0, long len2_0, long halfSize, long match) {
        double sA = Double.NaN, sB = Double.NaN, pAB = Double.NaN, lift = Double.NaN;

        if (halfSize <= 0)
            return lift;

        // 基本概率
        sA = (double) len1_0 / (double) halfSize;
        sB = (double) len2_0 / (double) halfSize;
        pAB = (double) match / (double) halfSize;

        // lift = P(A∧B) / (P(A) P(B))，仅在分母非零时计算
        if (sA > 0.0 && sB > 0.0) {
            lift = pAB / (sA * sB);
        }

        return lift;
    }

    public static double phiCoefficient(long len1_0, long len2_0, long halfSize, long match) {
        // 2x2 表格元素
        double a = (double) match; // A ∧ B
        double b = (double) (len1_0 - match); // A ∧ ¬B
        double c = (double) (len2_0 - match); // ¬A ∧ B
        double d = (double) (halfSize - (match + (len1_0 - match) + (len2_0 - match)));
        // 等价于: d = halfSize - (a + b + c)

        // 如果任何分量为负，输入可能不合法，返回 NaN
        if (a < 0 || b < 0 || c < 0 || d < 0) {
            return Double.NaN;
        }

        double numerator = a * d - b * c;
        double denomTerm1 = (a + b) * (c + d);
        double denomTerm2 = (a + c) * (b + d);

        // 分母为 sqrt( denomTerm1 * denomTerm2 )
        double denomProduct = denomTerm1 * denomTerm2;
        if (denomProduct <= 0.0) {
            return Double.NaN; // 避免除零或根号负数
        }

        double phi = numerator / Math.sqrt(denomProduct);
        return phi;
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
        // String parent_dir = "/Users/xiaojinzhao/Documents/GitHub/subcolumn/";
        String parent_dir = "D:/github/xjz17/subcolumn/";
        // String parent_dir = "D:/encoding-subcolumn/";

        String input_parent_dir = parent_dir + "dataset/";

        // String output_parent_dir = parent_dir + "result/";
        String output_parent_dir = "D:/encoding-subcolumn/result/materialization/";
        // String output_parent_dir = parent_dir + "result/";

        String outputPath = output_parent_dir + "cstore_materialization2.csv";

        HashMap<String, Integer> queryRange = new HashMap<>();

        queryRange.put("Bird-migration", 2600000);
        queryRange.put("Bitcoin-price", 170000000);
        queryRange.put("City-temp", 700);
        queryRange.put("Dewpoint-temp", 9600);
        queryRange.put("IR-bio-temp", -200);
        queryRange.put("PM10-dust", 2000);
        queryRange.put("Stocks-DE", 90000);
        queryRange.put("Stocks-UK", 75000);
        queryRange.put("Stocks-USA", 6000);
        queryRange.put("Wind-Speed", 60);
        queryRange.put("Wine-Tasting", 10);
        queryRange.put("Arade4", 10000000);
        queryRange.put("EPM-Education", 200);
        queryRange.put("POI-lat", 0);
        queryRange.put("Gov10", 100000);

        int block_size = 512;

        // int repeatTime = 100;
        int repeatTime = 500;

        // repeatTime = 1;

        CsvWriter writer = new CsvWriter(outputPath, ',', StandardCharsets.UTF_8);
        writer.setRecordDelimiter('\n');

        String[] head = {
                "Dataset",
                "Encoding Algorithm",
                // "Encoding Time",
                "Decoding Time",
                "Selectivity",
                "Phi",
                "Points",
        };
        writer.writeRecord(head);

        File directory = new File(input_parent_dir);
        // File[] csvFiles = directory.listFiles();
        File[] csvFiles = directory.listFiles((dir, name) -> name.endsWith(".csv"));

        for (File file : csvFiles) {
            String datasetName = extractFileName(file.toString());
            System.out.println(datasetName);

            InputStream inputStream = Files.newInputStream(file.toPath());

            CsvReader loader = new CsvReader(inputStream, StandardCharsets.UTF_8);
            ArrayList<Double> data1 = new ArrayList<>();

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
                data1.add(Double.valueOf(f_str));
            }
            inputStream.close();

            if (max_decimal > 17) {
                max_decimal = 17;
            }

            int totalSize = data1.size();
            int halfSize = totalSize / 2;

            long[] col1_data = new long[halfSize];
            long[] col2_data = new long[halfSize];
            long max_mul = (long) Math.pow(10, max_decimal);
            for (int i = 0; i < halfSize; i++)
                col1_data[i] = (long) (data1.get(i) * max_mul);
            for (int i = 0; i < halfSize; i++)
                col2_data[i] = (long) (data1.get(i + halfSize) * max_mul);

            // test
            // for (int i = 0; i < data2_arr.length; i++) {
            // System.out.print(data2_arr[i] + " ");
            // }
            // System.out.println();

            System.out.println(max_decimal);

            byte[] encoded_result1 = new byte[col1_data.length * 13];
            byte[] encoded_result2 = new byte[col2_data.length * 13];

            long encodeTime = 0;
            long decodeTime = 0;
            double ratio = 0;
            double compressed_size = 0;

            int length1 = 0;
            int length2 = 0;

            long tStart = System.nanoTime();
            for (int repeat = 0; repeat < repeatTime; repeat++) {
                length1 = Encoder(col1_data, block_size, encoded_result1);
                length2 = Encoder(col2_data, block_size, encoded_result2);
            }

            long tEnd = System.nanoTime();
            encodeTime += ((tEnd - tStart) / repeatTime);
            compressed_size += length1 + length2;

            double ratioTmp;

            ratioTmp = compressed_size / (double) (data1.size() * Long.BYTES);

            ratio += ratioTmp;

            int upper = queryRange.get(datasetName);

            int[] res1 = new int[encoded_result1.length];
            int[] len1 = new int[1];
            int[] res2 = new int[encoded_result2.length];
            int[] len2 = new int[1];

            // warm run to avoid JIT one-time overhead bias
            // Query(encoded_result1, upper, res1, len1);
            // Query(encoded_result2, upper, res2, len2);

            long[] data1_arr_decoded = new long[col1_data.length];
            long[] data2_arr_decoded = new long[col2_data.length];

            double selectivity = 0;
            double phi = 0;
            int match = 0;
            tStart = System.nanoTime();
            for (int r = 0; r < repeatTime; r++) {
                // run both queries (they are pure functions on encoded bytes)
                // CompletableFuture<Void> future1 = CompletableFuture.runAsync(() -> {
                // Query(encoded_result1, upper, res1, len1);
                // });

                // CompletableFuture<Void> future2 = CompletableFuture.runAsync(() -> {
                // Query(encoded_result2, upper, res2, len2);
                // });

                // // 等待两个查询都完成
                // try {
                // CompletableFuture.allOf(future1, future2).get();
                // } catch (InterruptedException | ExecutionException e) {
                // e.printStackTrace();
                // // 处理异常，可能需要中断循环或采取其他措施
                // Thread.currentThread().interrupt(); // 重新设置中断状态
                // break;
                // }

                Query(encoded_result1, upper, res1, len1);
                Query(encoded_result2, upper, res2, len2);

                long[] bits1 = new long[(halfSize + 63) / 64];
                long[] bits2 = new long[(halfSize + 63) / 64];

                // 设置bit
                for (int i = 0; i < len1[0]; i++) {
                    int pos = res1[i];
                    bits1[pos >> 6] |= (1L << (pos & 0x3F));
                }

                for (int i = 0; i < len2[0]; i++) {
                    int pos = res2[i];
                    bits2[pos >> 6] |= (1L << (pos & 0x3F));
                }

                // 求交集并计数
                match = 0;
                for (int i = 0; i < bits1.length; i++) {
                    long intersection = bits1[i] & bits2[i];
                    match += Long.bitCount(intersection);
                }
            }
            tEnd = System.nanoTime();

            selectivity = computeSelectivity(len1[0], len2[0], halfSize, match);
            phi = phiCoefficient(len1[0], len2[0], halfSize, match);
            System.out.println(len1[0] + "," + len2[0]);
            long lmParallelTime = (tEnd - tStart) / repeatTime;
            System.out.println("LM-parallel avg ns: " + lmParallelTime);

            // long[] data2_arr_decoded = new long[data2_arr.length];

            // s = System.nanoTime();

            // int[] result = new int[data2_arr.length];
            // int[] result_length = new int[1];

            // for (int repeat = 0; repeat < repeatTime; repeat++) {
            // // data2_arr_decoded = Decoder(encoded_result);
            // Query(encoded_result, queryRange.get(datasetName), result, result_length);
            // }

            String[] record = {
                    datasetName,
                    "RLE",
                    // String.valueOf(encodeTime),
                    String.valueOf(lmParallelTime),
                    String.valueOf(selectivity),
                    String.valueOf(phi),
                    String.valueOf(totalSize)
            };
            writer.writeRecord(record);
            System.out.println(ratio);
        }

        writer.close();
    }

}
