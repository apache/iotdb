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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static java.lang.Math.pow;

public class RLEBPTest {

    public static int getBitWith(int num) {
        if (num == 0)
            return 1;
        else
            return 32 - Integer.numberOfLeadingZeros(num);
    }

    public static int getCount(long long1, int mask) {
        return ((int) (long1 & mask));
    }

    public static int getUniqueValue(long long1, int left_shift) {
        return ((int) ((long1) >> left_shift));
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

    private static void long2intBytes(long integer, int encode_pos, byte[] cur_byte) {
        cur_byte[encode_pos] = (byte) (integer >> 24);
        cur_byte[encode_pos + 1] = (byte) (integer >> 16);
        cur_byte[encode_pos + 2] = (byte) (integer >> 8);
        cur_byte[encode_pos + 3] = (byte) (integer);
    }

    public static int bytes2Integer(byte[] encoded, int start, int num) {
        int value = 0;
        if (num > 4) {
            System.out.println("bytes2Integer error");
            return 0;
        }
        for (int i = 0; i < num; i++) {
            value <<= 8;
            int b = encoded[i + start] & 0xFF;
            value |= b;
        }
        return value;
    }

    private static long bytesLong2Integer(byte[] encoded, int decode_pos) {
        long value = 0;
        for (int i = 0; i < 4; i++) {
            value <<= 8;
            int b = encoded[i + decode_pos] & 0xFF;
            value |= b;
        }
        return value;
    }

    public static void pack8Values(ArrayList<Integer> values, int offset, int width, int encode_pos,
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
                buffer |= (values.get(valueIdx) << (32 - leftBit));
                leftSize -= leftBit;
                leftBit = 0;
                valueIdx++;
            }

            while (leftSize >= width && valueIdx < 8 + offset) {
                // encode one Integer to the 'buffer'
                buffer |= (values.get(valueIdx) << (leftSize - width));
                leftSize -= width;
                valueIdx++;
            }
            // If the remaining space of the buffer can not save the bits for one Integer,
            if (leftSize > 0 && valueIdx < 8 + offset) {
                // put the first 'leftSize' bits of the Integer into remaining space of the
                // buffer
                buffer |= (values.get(valueIdx) >>> (width - leftSize));
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

    public static void unpack8Values(byte[] encoded, int offset, int width, ArrayList<Integer> result_list) {
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
                result_list.add((int) (buffer >>> (totalBits - width)));
                valueIdx++;
                totalBits -= width;
                buffer = buffer & ((1L << totalBits) - 1);
            }
        }
    }

    public static int bitPacking(ArrayList<Integer> numbers, int start, int bit_width, int encode_pos,
            byte[] encoded_result) {
        int block_num = (numbers.size() - start) / 8;
        for (int i = 0; i < block_num; i++) {
            pack8Values(numbers, start + i * 8, bit_width, encode_pos, encoded_result);
            encode_pos += bit_width;
        }

        return encode_pos;

    }

    public static ArrayList<Integer> decodeBitPacking(
            byte[] encoded, int decode_pos, int bit_width, int block_size) {
        ArrayList<Integer> result_list = new ArrayList<>();
        int block_num = (block_size - 1) / 8;

        for (int i = 0; i < block_num; i++) { // bitpacking
            unpack8Values(encoded, decode_pos, bit_width, result_list);
            decode_pos += bit_width;
        }
        return result_list;
    }

    public static int[] getAbsDeltaTsBlock(
            int[] ts_block,
            int i,
            int block_size,
            int remaining,
            int[] min_delta,
            ArrayList<Integer> repeat_count) {
        int[] ts_block_delta = new int[remaining];

        int value_delta_min = Integer.MAX_VALUE;
        int value_delta_max = Integer.MIN_VALUE;
        int base = i * block_size;
        int end = i * block_size + remaining;
        for (int j = base; j < end; j++) {

            int integer = ts_block[j];
            if (integer < value_delta_min)
                value_delta_min = integer;
            if (integer > value_delta_max) {
                value_delta_max = integer;
            }
        }
        int pre_delta = ts_block[i * block_size] - value_delta_min;
        int pre_count = 1;

        min_delta[0] = (value_delta_min);
        int repeat_i = 0;
        int ts_block_delta_i = 0;
        for (int j = base + 1; j < end; j++) {
            int delta = ts_block[j] - value_delta_min;
            if (delta == pre_delta) {
                pre_count++;
            } else {
                if (pre_count > 7) {
                    repeat_count.add(repeat_i);
                    repeat_count.add(pre_count);
                    ts_block_delta[ts_block_delta_i] = pre_delta;
                    ts_block_delta_i++;
                } else {
                    for (int k = 0; k < pre_count; k++) {
                        ts_block_delta[ts_block_delta_i] = pre_delta;
                        ts_block_delta_i++;
                    }
                }
                pre_count = 1;
                repeat_i = j - i * block_size;
            }
            pre_delta = delta;

        }
        for (int j = 0; j < pre_count; j++) {
            ts_block_delta[ts_block_delta_i] = pre_delta;
            ts_block_delta_i++;
        }
        min_delta[1] = (ts_block_delta_i);
        min_delta[2] = (value_delta_max - value_delta_min);
        int[] new_ts_block_delta = new int[ts_block_delta_i];
        System.arraycopy(ts_block_delta, 0, new_ts_block_delta, 0, ts_block_delta_i);

        return new_ts_block_delta;
    }

    public static int encodeOutlier2Bytes(
            ArrayList<Integer> ts_block_delta,
            int bit_width,
            int encode_pos, byte[] encoded_result) {

        encode_pos = bitPacking(ts_block_delta, 0, bit_width, encode_pos, encoded_result);

        int n_k = ts_block_delta.size();
        int n_k_b = n_k / 8;
        long cur_remaining = 0; // encoded int
        int cur_number_bits = 0; // the bit width used of encoded int
        for (int i = n_k_b * 8; i < n_k; i++) {
            long cur_value = ts_block_delta.get(i);
            int cur_bit_width = bit_width; // remaining bit width of current value

            if (cur_number_bits + bit_width >= 32) {
                cur_remaining <<= (32 - cur_number_bits);
                cur_bit_width = bit_width - 32 + cur_number_bits;
                cur_remaining += ((cur_value >> cur_bit_width));
                long2intBytes(cur_remaining, encode_pos, encoded_result);
                encode_pos += 4;

                cur_remaining = 0;
                cur_number_bits = 0;
            }

            cur_remaining <<= cur_bit_width;
            cur_number_bits += cur_bit_width;
            cur_remaining += (((cur_value << (32 - cur_bit_width)) & 0xFFFFFFFFL) >> (32 - cur_bit_width));
        }
        cur_remaining <<= (32 - cur_number_bits);
        long2intBytes(cur_remaining, encode_pos, encoded_result);
        encode_pos += 4;
        return encode_pos;

    }

    public static ArrayList<Integer> decodeOutlier2Bytes(
            byte[] encoded,
            int decode_pos,
            int bit_width,
            int length,
            ArrayList<Integer> encoded_pos_result) {

        int n_k_b = length / 8;
        int remaining = length - n_k_b * 8;
        ArrayList<Integer> result_list = new ArrayList<>(
                decodeBitPacking(encoded, decode_pos, bit_width, n_k_b * 8 + 1));
        decode_pos += n_k_b * bit_width;

        ArrayList<Long> int_remaining = new ArrayList<>();
        int int_remaining_size = remaining * bit_width / 32 + 1;
        for (int j = 0; j < int_remaining_size; j++) {

            int_remaining.add(bytesLong2Integer(encoded, decode_pos));
            decode_pos += 4;
        }

        int cur_remaining_bits = 32; // remaining bit width of current value
        long cur_number = int_remaining.get(0);
        int cur_number_i = 1;
        for (int i = n_k_b * 8; i < length; i++) {
            if (bit_width < cur_remaining_bits) {
                int tmp = (int) (cur_number >> (32 - bit_width));
                result_list.add(tmp);
                cur_number <<= bit_width;
                cur_number &= 0xFFFFFFFFL;
                cur_remaining_bits -= bit_width;
            } else {
                int tmp = (int) (cur_number >> (32 - cur_remaining_bits));
                int remain_bits = bit_width - cur_remaining_bits;
                tmp <<= remain_bits;

                cur_number = int_remaining.get(cur_number_i);
                cur_number_i++;
                tmp += (cur_number >> (32 - remain_bits));
                result_list.add(tmp);
                cur_number <<= remain_bits;
                cur_number &= 0xFFFFFFFFL;
                cur_remaining_bits = 32 - remain_bits;
            }
        }
        encoded_pos_result.add(decode_pos);
        return result_list;
    }

    private static int BOSEncodeBits(int[] ts_block_delta,
            int init_block_size,
            int final_k_start_value,
            int final_x_l_plus,
            int final_k_end_value,
            int final_x_u_minus,
            int max_delta_value,
            int[] min_delta,
            ArrayList<Integer> repeat_count,
            int encode_pos,
            byte[] cur_byte) {
        int block_size = ts_block_delta.length;

        ArrayList<Integer> final_left_outlier_index = new ArrayList<>();
        ArrayList<Integer> final_right_outlier_index = new ArrayList<>();
        ArrayList<Integer> final_left_outlier = new ArrayList<>();
        ArrayList<Integer> final_right_outlier = new ArrayList<>();
        ArrayList<Integer> final_normal = new ArrayList<>();
        int k1 = 0;
        int k2 = 0;
        ArrayList<Integer> bitmap_outlier = new ArrayList<>();
        int index_bitmap_outlier = 0;
        int cur_index_bitmap_outlier_bits = 0;
        for (int i = 0; i < block_size; i++) {
            int cur_value = ts_block_delta[i];
            if (cur_value < final_k_start_value) {
                final_left_outlier.add(cur_value);
                final_left_outlier_index.add(i);
                if (cur_index_bitmap_outlier_bits % 8 != 7) {
                    index_bitmap_outlier <<= 2;
                    index_bitmap_outlier += 3;
                    cur_index_bitmap_outlier_bits += 2;
                } else {
                    index_bitmap_outlier <<= 1;
                    index_bitmap_outlier += 1;
                    bitmap_outlier.add(index_bitmap_outlier);
                    index_bitmap_outlier = 1;
                    cur_index_bitmap_outlier_bits = 1;
                }

                k1++;

            } else if (cur_value >= final_k_end_value) {
                final_right_outlier.add(cur_value - final_k_end_value);
                final_right_outlier_index.add(i);
                if (cur_index_bitmap_outlier_bits % 8 != 7) {
                    index_bitmap_outlier <<= 2;
                    index_bitmap_outlier += 2;
                    cur_index_bitmap_outlier_bits += 2;
                } else {
                    index_bitmap_outlier <<= 1;
                    index_bitmap_outlier += 1;
                    bitmap_outlier.add(index_bitmap_outlier);
                    index_bitmap_outlier = 0;
                    cur_index_bitmap_outlier_bits = 1;
                }
                k2++;

            } else {
                final_normal.add(cur_value - final_x_l_plus);
                index_bitmap_outlier <<= 1;
                cur_index_bitmap_outlier_bits += 1;
            }
            if (cur_index_bitmap_outlier_bits % 8 == 0) {
                bitmap_outlier.add(index_bitmap_outlier);
                index_bitmap_outlier = 0;
            }
        }
        if (cur_index_bitmap_outlier_bits % 8 != 0) {

            index_bitmap_outlier <<= (8 - cur_index_bitmap_outlier_bits % 8);

            index_bitmap_outlier &= 0xFF;
            bitmap_outlier.add(index_bitmap_outlier);
        }
        int final_alpha = ((k1 + k2) * getBitWith(block_size - 1)) <= (block_size + k1 + k2) ? 1 : 0;

        int k_byte = (k1 << 1);
        k_byte += final_alpha;
        k_byte += (k2 << 16);

        int2Bytes(k_byte, encode_pos, cur_byte);
        encode_pos += 4;

        int2Bytes(min_delta[0], encode_pos, cur_byte);
        encode_pos += 4;
        int size = repeat_count.size();
        intByte2Bytes(size, encode_pos, cur_byte);
        encode_pos += 1;

        if (size != 0)
            encode_pos = encodeOutlier2Bytes(repeat_count, getBitWith(init_block_size - 1), encode_pos, cur_byte);

        int2Bytes(final_x_l_plus, encode_pos, cur_byte);
        encode_pos += 4;
        int2Bytes(final_k_end_value, encode_pos, cur_byte);
        encode_pos += 4;

        int bit_width_final = getBitWith(final_x_u_minus - final_x_l_plus);
        intByte2Bytes(bit_width_final, encode_pos, cur_byte);
        encode_pos += 1;
        int left_bit_width = getBitWith(final_k_start_value);
        int right_bit_width = getBitWith(max_delta_value - final_k_end_value);
        intByte2Bytes(left_bit_width, encode_pos, cur_byte);
        encode_pos += 1;
        intByte2Bytes(right_bit_width, encode_pos, cur_byte);
        encode_pos += 1;
        if (final_alpha == 0) {

            for (int i : bitmap_outlier) {

                intByte2Bytes(i, encode_pos, cur_byte);
                encode_pos += 1;
            }
        } else {
            encode_pos = encodeOutlier2Bytes(final_left_outlier_index, getBitWith(block_size - 1), encode_pos,
                    cur_byte);
            encode_pos = encodeOutlier2Bytes(final_right_outlier_index, getBitWith(block_size - 1), encode_pos,
                    cur_byte);
        }
        encode_pos = encodeOutlier2Bytes(final_normal, bit_width_final, encode_pos, cur_byte);
        if (k1 != 0)
            encode_pos = encodeOutlier2Bytes(final_left_outlier, left_bit_width, encode_pos, cur_byte);
        if (k2 != 0)
            encode_pos = encodeOutlier2Bytes(final_right_outlier, right_bit_width, encode_pos, cur_byte);
        return encode_pos;

    }

    private static int BOSBlockEncoder(int[] ts_block, int block_i, int block_size, int remaining, int encode_pos,
            byte[] cur_byte) {

        ArrayList<Integer> repeat_count = new ArrayList<>();
        int init_block_size = block_size;

        int[] min_delta = new int[3];
        int[] ts_block_delta = getAbsDeltaTsBlock(ts_block, block_i, init_block_size, remaining, min_delta,
                repeat_count);
        block_size = min_delta[1];

        int max_delta_value = min_delta[2];

        return encode_pos;
    }

    public static int BOSEncoder(
            int[] data, int block_size, byte[] encoded_result) {

        int length_all = data.length;

        int encode_pos = 0;
        int2Bytes(length_all, encode_pos, encoded_result);
        encode_pos += 4;

        int block_num = length_all / block_size;
        int2Bytes(block_size, encode_pos, encoded_result);
        encode_pos += 4;

        for (int i = 0; i < block_num; i++) {
            encode_pos = BOSBlockEncoder(data, i, block_size, block_size, encode_pos, encoded_result);
            // System.out.println(encode_pos);
        }

        int remaining_length = length_all - block_num * block_size;
        if (remaining_length <= 3) {
            for (int i = remaining_length; i > 0; i--) {
                int2Bytes(data[data.length - i], encode_pos, encoded_result);
                encode_pos += 4;
            }

        } else {

            int start = block_num * block_size;
            int remaining = length_all - start;

            encode_pos = BOSBlockEncoder(data, block_num, block_size, remaining, encode_pos, encoded_result);

            // int[] ts_block = new int[length_all-start];
            // if (length_all - start >= 0) System.arraycopy(data, start, ts_block, 0,
            // length_all - start);
            //
            //
            // encode_pos = BOSBlockEncoder(ts_block, encode_pos,encoded_result);

        }

        return encode_pos;
    }

    public static int EncodeBits(int num,
            int bit_width,
            int encode_pos,
            byte[] cur_byte,
            int[] bit_index_list) {
        // 找到要插入的位的索引
        int bit_index = bit_index_list[0];// cur_byte[encode_pos + 1];

        // 计算数值的起始位位置
        int remaining_bits = bit_width;

        while (remaining_bits > 0) {
            // 计算在当前字节中可以使用的位数
            int available_bits = bit_index;
            int bits_to_write = Math.min(available_bits, remaining_bits);

            // 更新 bit_index
            bit_index = available_bits - bits_to_write;

            // 计算要写入的位的掩码和数值
            int mask = (1 << bits_to_write) - 1;
            int bits = (num >> (remaining_bits - bits_to_write)) & mask;

            // 写入到当前位置
            cur_byte[encode_pos] &= (byte) ~(mask << bit_index); // 清除对应位置的位
            cur_byte[encode_pos] |= (byte) (bits << bit_index);

            // 更新位宽和数值
            remaining_bits -= bits_to_write;
            if (bit_index == 0) {
                bit_index = 8;
                encode_pos++;
            }
        }
        bit_index_list[0] = bit_index;
        // cur_byte[encode_pos + 1] = (byte) bit_index;
        return encode_pos;
    }

    private static int BOSBlockEncoderImprove(int[] ts_block, int block_i, int block_size, int remaining,
            int encode_pos, byte[] cur_byte) {

        ArrayList<Integer> repeat_count = new ArrayList<>();
        int init_block_size = block_size;

        int[] min_delta = new int[3];
        int[] ts_block_delta = getAbsDeltaTsBlock(ts_block, block_i, init_block_size, remaining, min_delta,
                repeat_count);

        int max_delta_value = min_delta[2];

        int2Bytes(min_delta[0], encode_pos, cur_byte);
        encode_pos += 4;

        int size = repeat_count.size();
        intByte2Bytes(size, encode_pos, cur_byte);
        encode_pos += 1;

        int[] bit_index_list = new int[1];
        bit_index_list[0] = 8;
        if (size != 0) {
            int bit_width_init = getBitWith(init_block_size - 1);
            for (int repeat_count_v : repeat_count) {
                encode_pos = EncodeBits(repeat_count_v, bit_width_init, encode_pos, cur_byte, bit_index_list);
            }
            if (bit_index_list[0] != 8) {
                bit_index_list[0] = 8;
                encode_pos++;
            }
        }

        int bit_width_final = getBitWith(max_delta_value);
        intByte2Bytes(bit_width_final, encode_pos, cur_byte);
        encode_pos += 1;

        bit_index_list[0] = 8;
        for (int cur_value : ts_block_delta) {
            encode_pos = EncodeBits(cur_value, bit_width_final, encode_pos, cur_byte, bit_index_list);
            // final_normal.add(cur_value);
        }
        if (bit_index_list[0] != 8) {
            encode_pos++;
        }

        return encode_pos;
    }

    public static int BOSEncoderImprove(
            int[] data, int block_size, byte[] encoded_result) {

        int length_all = data.length;

        int encode_pos = 0;
        int2Bytes(length_all, encode_pos, encoded_result);
        encode_pos += 4;

        int block_num = length_all / block_size;
        int2Bytes(block_size, encode_pos, encoded_result);
        encode_pos += 4;

        for (int i = 0; i < block_num; i++) {

            encode_pos = BOSBlockEncoderImprove(data, i, block_size, block_size, encode_pos, encoded_result);
            // System.out.println(encode_pos);
        }

        int remaining_length = length_all - block_num * block_size;
        if (remaining_length <= 3) {
            for (int i = remaining_length; i > 0; i--) {
                int2Bytes(data[data.length - i], encode_pos, encoded_result);
                encode_pos += 4;
            }

        } else {

            int start = block_num * block_size;
            int remaining = length_all - start;

            encode_pos = BOSBlockEncoderImprove(data, block_num, block_size, remaining, encode_pos, encoded_result);

            // int[] ts_block = new int[length_all-start];
            // if (length_all - start >= 0) System.arraycopy(data, start, ts_block, 0,
            // length_all - start);
            //
            //
            // encode_pos = BOSBlockEncoder(ts_block, encode_pos,encoded_result);

        }

        return encode_pos;
    }

    public static int BOSBlockDecoder(byte[] encoded, int decode_pos, int[] value_list, int init_block_size,
            int block_size, int[] value_pos_arr) {

        int k_byte = bytes2Integer(encoded, decode_pos, 4);
        decode_pos += 4;
        int k1_byte = (int) (k_byte % pow(2, 16));
        int k1 = k1_byte / 2;
        int final_alpha = k1_byte % 2;

        int k2 = (int) (k_byte / pow(2, 16));

        int min_delta = bytes2Integer(encoded, decode_pos, 4);
        decode_pos += 4;

        int count_size = bytes2Integer(encoded, decode_pos, 1);
        decode_pos += 1;

        ArrayList<Integer> repeat_count = new ArrayList<>();
        if (count_size != 0) {
            ArrayList<Integer> repeat_count_result = new ArrayList<>();
            repeat_count = decodeOutlier2Bytes(encoded, decode_pos, getBitWith(init_block_size - 1), count_size,
                    repeat_count_result);
            decode_pos = repeat_count_result.get(0);

        }

        int cur_block_size = block_size;
        for (int i = 1; i < count_size; i += 2) {
            cur_block_size -= (repeat_count.get(i) - 1);
        }

        int final_k_start_value = bytes2Integer(encoded, decode_pos, 4);
        decode_pos += 4;

        int final_k_end_value = bytes2Integer(encoded, decode_pos, 4);
        decode_pos += 4;

        int bit_width_final = bytes2Integer(encoded, decode_pos, 1);
        decode_pos += 1;

        int left_bit_width = bytes2Integer(encoded, decode_pos, 1);
        decode_pos += 1;
        int right_bit_width = bytes2Integer(encoded, decode_pos, 1);
        decode_pos += 1;

        ArrayList<Integer> final_left_outlier_index = new ArrayList<>();
        ArrayList<Integer> final_right_outlier_index = new ArrayList<>();
        ArrayList<Integer> final_left_outlier = new ArrayList<>();
        ArrayList<Integer> final_right_outlier = new ArrayList<>();
        ArrayList<Integer> final_normal;
        ArrayList<Integer> bitmap_outlier = new ArrayList<>();

        if (final_alpha == 0) { // 0
            int bitmap_bytes = (int) Math.ceil((double) (cur_block_size + k1 + k2) / (double) 8);
            // System.out.println("bitmap_bytes:" + bitmap_bytes);
            for (int i = 0; i < bitmap_bytes; i++) {
                bitmap_outlier.add(bytes2Integer(encoded, decode_pos, 1));
                decode_pos += 1;
            }
            int bitmap_outlier_i = 0;
            int remaining_bits = 8;
            int tmp = bitmap_outlier.get(bitmap_outlier_i);
            bitmap_outlier_i++;
            int i = 0;
            while (i < cur_block_size) {
                if (remaining_bits > 1) {
                    int bit_i = (tmp >> (remaining_bits - 1)) & 0x1;
                    remaining_bits -= 1;
                    if (bit_i == 1) {
                        int bit_left_right = (tmp >> (remaining_bits - 1)) & 0x1;
                        remaining_bits -= 1;
                        if (bit_left_right == 1) {
                            final_left_outlier_index.add(i);
                        } else {
                            final_right_outlier_index.add(i);
                        }
                    }
                    if (remaining_bits == 0) {
                        remaining_bits = 8;
                        if (bitmap_outlier_i >= bitmap_bytes)
                            break;
                        tmp = bitmap_outlier.get(bitmap_outlier_i);
                        bitmap_outlier_i++;
                    }
                } else if (remaining_bits == 1) {
                    int bit_i = tmp & 0x1;
                    remaining_bits = 8;
                    if (bitmap_outlier_i >= bitmap_bytes)
                        break;
                    tmp = bitmap_outlier.get(bitmap_outlier_i);
                    bitmap_outlier_i++;
                    if (bit_i == 1) {
                        int bit_left_right = (tmp >> (remaining_bits - 1)) & 0x1;
                        remaining_bits -= 1;
                        if (bit_left_right == 1) {
                            final_left_outlier_index.add(i);
                        } else {
                            final_right_outlier_index.add(i);
                        }
                    }
                }
                i++;
            }
        } else {

            ArrayList<Integer> decode_pos_result_left = new ArrayList<>();
            final_left_outlier_index = decodeOutlier2Bytes(encoded, decode_pos, getBitWith(cur_block_size - 1), k1,
                    decode_pos_result_left);
            decode_pos = (decode_pos_result_left.get(0));

            ArrayList<Integer> decode_pos_result_right = new ArrayList<>();
            final_right_outlier_index = decodeOutlier2Bytes(encoded, decode_pos, getBitWith(cur_block_size - 1), k2,
                    decode_pos_result_right);
            decode_pos = (decode_pos_result_right.get(0));

        }

        ArrayList<Integer> decode_pos_normal = new ArrayList<>();
        final_normal = decodeOutlier2Bytes(encoded, decode_pos, bit_width_final, cur_block_size - k1 - k2,
                decode_pos_normal);

        decode_pos = decode_pos_normal.get(0);
        if (k1 != 0) {
            ArrayList<Integer> decode_pos_result_left = new ArrayList<>();
            final_left_outlier = decodeOutlier2Bytes(encoded, decode_pos, left_bit_width, k1, decode_pos_result_left);

            decode_pos = decode_pos_result_left.get(0);
        }
        if (k2 != 0) {
            ArrayList<Integer> decode_pos_result_right = new ArrayList<>();
            final_right_outlier = decodeOutlier2Bytes(encoded, decode_pos, right_bit_width, k2,
                    decode_pos_result_right);
            decode_pos = decode_pos_result_right.get(0);
        }
        int left_outlier_i = 0;
        int right_outlier_i = 0;
        int normal_i = 0;
        int pre_v;
        // int final_k_end_value = (int) (final_k_start_value + pow(2,
        // bit_width_final));

        int cur_i = 0;
        int repeat_i = 0;
        for (int i = 0; i < cur_block_size; i++) {

            int current_delta;
            if (left_outlier_i >= k1) {
                if (right_outlier_i >= k2) {
                    current_delta = final_normal.get(normal_i) + final_k_start_value + 1;
                    normal_i++;
                } else if (i == final_right_outlier_index.get(right_outlier_i)) {
                    current_delta = final_right_outlier.get(right_outlier_i) + final_k_end_value;
                    right_outlier_i++;
                } else {
                    current_delta = final_normal.get(normal_i) + final_k_start_value + 1;
                    normal_i++;
                }
            } else if (i == final_left_outlier_index.get(left_outlier_i)) {
                current_delta = final_left_outlier.get(left_outlier_i);
                left_outlier_i++;
            } else {

                if (right_outlier_i >= k2) {
                    current_delta = final_normal.get(normal_i) + final_k_start_value + 1;
                    normal_i++;
                } else if (i == final_right_outlier_index.get(right_outlier_i)) {
                    current_delta = final_right_outlier.get(right_outlier_i) + final_k_end_value;
                    right_outlier_i++;
                } else {
                    current_delta = final_normal.get(normal_i) + final_k_start_value + 1;
                    normal_i++;
                }
            }
            pre_v = current_delta + min_delta;
            if (repeat_i < count_size) {
                if (cur_i == repeat_count.get(repeat_i)) {
                    cur_i += (repeat_count.get(repeat_i + 1));

                    for (int j = 0; j < repeat_count.get(repeat_i + 1); j++) {
                        value_list[value_pos_arr[0]] = pre_v;
                        value_pos_arr[0]++;
                    }
                    repeat_i += 2;
                } else {
                    cur_i++;
                    value_list[value_pos_arr[0]] = pre_v;
                    value_pos_arr[0]++;
                }
            } else {
                cur_i++;
                value_list[value_pos_arr[0]] = pre_v;
                value_pos_arr[0]++;
            }
        }
        return decode_pos;
    }

    public static void BOSDecoder(byte[] encoded) {

        int decode_pos = 0;
        int length_all = bytes2Integer(encoded, decode_pos, 4);
        decode_pos += 4;
        int block_size = bytes2Integer(encoded, decode_pos, 4);
        decode_pos += 4;

        int block_num = length_all / block_size;
        int remain_length = length_all - block_num * block_size;

        int[] value_list = new int[length_all + block_size];
        int[] value_pos_arr = new int[1];

        for (int k = 0; k < block_num; k++) {
            decode_pos = BOSBlockDecoder(encoded, decode_pos, value_list, block_size, block_size, value_pos_arr);
        }

        if (remain_length <= 3) {
            for (int i = 0; i < remain_length; i++) {
                int value_end = bytes2Integer(encoded, decode_pos, 4);
                decode_pos += 4;
                value_list[value_pos_arr[0]] = value_end;
                value_pos_arr[0]++;
            }
        } else {
            BOSBlockDecoder(encoded, decode_pos, value_list, block_size, remain_length, value_pos_arr);
        }
    }

    public static int DecodeBits(byte[] cur_byte, int bit_width, int[] decode_pos_list) {
        int decode_pos = decode_pos_list[0];
        int bit_index = decode_pos_list[1]; // cur_byte[decode_pos + 1];
        int remaining_bits = bit_width;
        int num = 0;

        while (remaining_bits > 0) {
            int available_bits = bit_index;
            int bits_to_read = Math.min(available_bits, remaining_bits);

            // 计算要读取的位的掩码
            int mask = (1 << bits_to_read) - 1;
            int bits = (cur_byte[decode_pos] >> (available_bits - bits_to_read)) & mask;

            // 将读取的位合并到结果中
            num = (num << bits_to_read) | bits;

            // 更新位宽和 bit_index
            remaining_bits -= bits_to_read;
            bit_index = available_bits - bits_to_read;

            if (bit_index == 0) {
                bit_index = 8;
                decode_pos++;
            }
        }
        decode_pos_list[0] = decode_pos;
        decode_pos_list[1] = bit_index;

        return num;
    }

    public static int BOSBlockDecoderImprove(byte[] encoded, int decode_pos, int[] value_list, int init_block_size,
            int block_size, int[] value_pos_arr) {

        int min_delta = bytes2Integer(encoded, decode_pos, 4);
        decode_pos += 4;

        int count_size = bytes2Integer(encoded, decode_pos, 1);
        decode_pos += 1;

        int[] decode_list = new int[2];
        decode_list[0] = decode_pos;
        decode_list[1] = 8;

        ArrayList<Integer> repeat_count = new ArrayList<>();
        if (count_size != 0) {
            int bit_width_init = getBitWith(init_block_size - 1);
            for (int i = 0; i < count_size; i++) {
                int repeat_count_v = DecodeBits(encoded, bit_width_init, decode_list);
                repeat_count.add(repeat_count_v);
            }

            if (decode_list[1] != 8) {
                decode_list[1] = 8;
                decode_list[0]++;
            }
            // repeat_count = decodeOutlier2Bytes(encoded, decode_pos,
            // getBitWith(init_block_size-1), count_size, repeat_count_result);
            decode_pos = decode_list[0];
            // decode_list[1]= 8;
        }

        int cur_block_size = block_size;
        for (int i = 1; i < count_size; i += 2) {
            cur_block_size -= (repeat_count.get(i) - 1);
        }

        int bit_width_final = bytes2Integer(encoded, decode_pos, 1);
        decode_pos += 1;

        int pre_v;
        int cur_i = 0;
        int repeat_i = 0;

        decode_list[0] = decode_pos;
        decode_list[1] = 8;
        for (int i = 0; i < cur_block_size; i++) {
            pre_v = min_delta + DecodeBits(encoded, bit_width_final, decode_list);
            // value_list[value_pos_arr[0]++] = pre_v;
            if (repeat_i < count_size && cur_i == repeat_count.get(repeat_i)) {
                cur_i += (repeat_count.get(repeat_i + 1));

                for (int j = 0; j < repeat_count.get(repeat_i + 1); j++) {
                    value_list[value_pos_arr[0]++] = pre_v;
                }
                repeat_i += 2;
            } else {
                cur_i++;
                value_list[value_pos_arr[0]++] = pre_v;
            }
        }
        if (decode_list[1] != 8) {
            decode_list[1] = 8;
            decode_list[0]++;
        }

        return decode_list[0];

    }

    public static void BOSDecoderImprove(byte[] encoded) {

        int decode_pos = 0;
        int length_all = bytes2Integer(encoded, decode_pos, 4);
        decode_pos += 4;
        int block_size = bytes2Integer(encoded, decode_pos, 4);
        decode_pos += 4;

        int block_num = length_all / block_size;
        int remain_length = length_all - block_num * block_size;

        int[] value_list = new int[length_all + block_size];
        int[] value_pos_arr = new int[1];

        for (int k = 0; k < block_num; k++) {
            // System.out.println(k);
            decode_pos = BOSBlockDecoderImprove(encoded, decode_pos, value_list, block_size, block_size, value_pos_arr);
            // System.out.println(decode_pos);
        }
        if (remain_length <= 3) {
            for (int i = 0; i < remain_length; i++) {
                int value_end = bytes2Integer(encoded, decode_pos, 4);
                decode_pos += 4;
                value_list[value_pos_arr[0]] = value_end;
                value_pos_arr[0]++;
            }
        } else {
            BOSBlockDecoderImprove(encoded, decode_pos, value_list, block_size, remain_length, value_pos_arr);
        }
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
    public void testSubcolumn() throws IOException {
        String parent_dir = "D:/github/xjz17/subcolumn/";

        String input_parent_dir = parent_dir + "dataset/";

        String output_parent_dir = "D:/encoding-subcolumn/result/";
        // String output_parent_dir = parent_dir + "result/";

        String outputPath = output_parent_dir + "rle.csv";

        int block_size = 1024;

        int repeatTime = 100;

        // repeatTime = 1;

        List<String> integerDatasets = new ArrayList<>();
        integerDatasets.add("Wine-Tasting");

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
        writer.writeRecord(head); // write header to output file
        File directory = new File(input_parent_dir);
        // File[] csvFiles = directory.listFiles();
        File[] csvFiles = directory.listFiles((dir, name) -> name.endsWith(".csv"));

        for (File file : csvFiles) {
            // System.out.println(f);
            String datasetName = extractFileName(file.toString());
            System.out.println(datasetName);
            InputStream inputStream = Files.newInputStream(file.toPath());

            CsvReader loader = new CsvReader(inputStream, StandardCharsets.UTF_8);
            ArrayList<Float> data1 = new ArrayList<>();
            // ArrayList<Integer> data2 = new ArrayList<>();

            // loader.readHeaders();
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
                // String value = loader.getValues()[index];
                data1.add(Float.valueOf(f_str));
                // data2.add(Integer.valueOf(loader.getValues()[1]));
                // data.add(Integer.valueOf(value));
            }

            inputStream.close();
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
                length = BOSEncoderImprove(data2_arr, block_size, encoded_result);
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

            ratio += ratioTmp;

            s = System.nanoTime();

            for (int repeat = 0; repeat < repeatTime; repeat++) {
                BOSDecoderImprove(encoded_result);
            }

            e = System.nanoTime();
            decodeTime += ((e - s) / repeatTime);

            String[] record = {
                    datasetName,
                    "RLE",
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

    @Test
    public void testTransData() throws IOException {
        String parent_dir = "D:/github/xjz17/subcolumn/";

        String output_parent_dir = "D:/encoding-subcolumn/trans_data_result/";
        // String output_parent_dir = parent_dir + "trans_data_result/";

        String input_parent_dir = parent_dir + "trans_data/";

        ArrayList<String> input_path_list = new ArrayList<>();
        ArrayList<String> output_path_list = new ArrayList<>();
        ArrayList<String> dataset_name = new ArrayList<>();
        ArrayList<Integer> dataset_block_size = new ArrayList<>();

        try (Stream<Path> paths = Files.walk(Paths.get(input_parent_dir))) {
            paths.filter(Files::isDirectory)
                    .filter(path -> !path.equals(Paths.get(input_parent_dir)))
                    .forEach(dir -> {
                        String name = dir.getFileName().toString();
                        dataset_name.add(name);
                        input_path_list.add(dir.toString());
                        dataset_block_size.add(1024);
                    });
        }

        String outputPath = output_parent_dir + "rle.csv";
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

        int repeatTime = 100;

        for (int file_i = 0; file_i < input_path_list.size(); file_i++) {

            String inputPath = input_path_list.get(file_i);
            System.out.println(inputPath);

            File file = new File(inputPath);
            File[] tempList = file.listFiles();

            long totalEncodeTime = 0;
            long totalDecodeTime = 0;
            double totalCompressedSize = 0;
            int totalPoints = 0;

            for (File f : tempList) {
                String datasetName = extractFileName(f.toString());
                InputStream inputStream = Files.newInputStream(f.toPath());

                CsvReader loader = new CsvReader(inputStream, StandardCharsets.UTF_8);
                ArrayList<Integer> data1 = new ArrayList<>();
                ArrayList<Integer> data2 = new ArrayList<>();

                loader.readHeaders();
                while (loader.readRecord()) {
                    // String value = loader.getValues()[index];
                    data1.add(Integer.valueOf(loader.getValues()[0]));
                    data2.add(Integer.valueOf(loader.getValues()[1]));
                    // data.add(Integer.valueOf(value));
                }
                inputStream.close();
                int[] data2_arr = new int[data1.size()];
                for (int i = 0; i < data2.size(); i++) {
                    data2_arr[i] = data2.get(i);
                }
                byte[] encoded_result = new byte[data2_arr.length * 4];
                long encodeTime = 0;
                long decodeTime = 0;
                double ratio = 0;
                double compressed_size = 0;

                int length = 0;

                long s = System.nanoTime();
                for (int repeat = 0; repeat < repeatTime; repeat++) {
                    length = BOSEncoderImprove(data2_arr, dataset_block_size.get(file_i), encoded_result);
                }

                long e = System.nanoTime();
                encodeTime += ((e - s) / repeatTime);
                compressed_size += length;
                double ratioTmp = compressed_size / (double) (data1.size() * Integer.BYTES);
                ratio += ratioTmp;
                s = System.nanoTime();

                for (int repeat = 0; repeat < repeatTime; repeat++) {
                    BOSDecoderImprove(encoded_result);
                }

                e = System.nanoTime();
                decodeTime += ((e - s) / repeatTime);

                totalEncodeTime += encodeTime;
                totalDecodeTime += decodeTime;
                totalCompressedSize += compressed_size;
                totalPoints += data1.size();
                
            }

            double compressionRatio = totalCompressedSize / (totalPoints * Integer.BYTES);

            String[] record = {
                    dataset_name.get(file_i),
                    "RLE",
                    String.valueOf(totalEncodeTime),
                    String.valueOf(totalDecodeTime),
                    String.valueOf(totalPoints),
                    String.valueOf(totalCompressedSize),
                    String.valueOf(compressionRatio)
            };

            writer.writeRecord(record);
            System.out.println(compressionRatio);
        }
        writer.close();
    }

}
