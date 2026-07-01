package org.apache.iotdb.tsfile.encoding;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;
import org.apache.commons.compress.archivers.sevenz.SevenZArchiveEntry;
import org.apache.commons.compress.archivers.sevenz.SevenZOutputFile;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.apache.iotdb.tsfile.compress.ICompressor;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.junit.Test;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

public class TSDIFFTest {

    private static final int BIT_IO_STEP = 2;

    public static long combine2Int(int int1, int int2) {
        return ((long) int1 << 32) | (int2 & 0xFFFFFFFFL);
    }

    public static int getTime(long long1) {
        return ((int) (long1 >> 32));
    }

    public static int getValue(long long1) {
        return ((int) (long1));
    }

    public static int getCount(long long1, int mask) {
        return ((int) (long1 & mask));
    }

    public static int getUniqueValue(long long1, int left_shift) {
        return ((int) ((long1) >> left_shift));
    }

    public static int getBitWith(int num) {
        if (num == 0)
            return 1;
        else
            return 32 - Integer.numberOfLeadingZeros(num);
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
        int leftBit = 0;

        while (valueIdx < 8 + offset) {
            int buffer = 0;
            int leftSize = 32;

            if (leftBit > 0) {
                buffer |= (values.get(valueIdx) << (32 - leftBit));
                leftSize -= leftBit;
                leftBit = 0;
                valueIdx++;
            }

            while (leftSize >= width && valueIdx < 8 + offset) {
                buffer |= (values.get(valueIdx) << (leftSize - width));
                leftSize -= width;
                valueIdx++;
            }
            if (leftSize > 0 && valueIdx < 8 + offset) {
                buffer |= (values.get(valueIdx) >>> (width - leftSize));
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

    public static void unpack8Values(byte[] encoded, int offset, int width, ArrayList<Integer> result_list) {
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

        for (int i = 0; i < block_num; i++) {
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
            int[] min_delta) {
        int[] ts_block_delta = new int[remaining - 1];

        int value_delta_min = Integer.MAX_VALUE;
        int value_delta_max = Integer.MIN_VALUE;
        int base = i * block_size + 1;
        int end = i * block_size + remaining;

        int tmp_j_1 = ts_block[base - 1];
        min_delta[0] = tmp_j_1;
        int j = base;
        int tmp_j;

        while (j < end) {
            tmp_j = ts_block[j];
            int epsilon_v = tmp_j - tmp_j_1;
            ts_block_delta[j - base] = epsilon_v;
            if (epsilon_v < value_delta_min) {
                value_delta_min = epsilon_v;
            }
            if (epsilon_v > value_delta_max) {
                value_delta_max = epsilon_v;
            }
            tmp_j_1 = tmp_j;
            j++;
        }
        j = 0;
        end = remaining - 1;
        while (j < end) {
            ts_block_delta[j] = ts_block_delta[j] - value_delta_min;
            j++;
        }

        min_delta[1] = value_delta_min;
        min_delta[2] = (value_delta_max - value_delta_min);

        return ts_block_delta;
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
            int cur_bit_width = bit_width;

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
            cur_remaining += (((cur_value << (32 - cur_bit_width)) & 0xFFFFFFFFL) >> (32 - cur_bit_width)); //
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

        int cur_remaining_bits = 32;
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
                tmp += (int) (cur_number >> (32 - remain_bits));
                result_list.add(tmp);
                cur_number <<= remain_bits;
                cur_number &= 0xFFFFFFFFL;
                cur_remaining_bits = 32 - remain_bits;
            }
        }
        encoded_pos_result.add(decode_pos);
        return result_list;
    }

    private static int BOSBlockEncoder(int[] ts_block, int block_i, int block_size, int remaining, int encode_pos,
            byte[] cur_byte) {

        int[] min_delta = new int[3];
        int[] ts_block_delta = getAbsDeltaTsBlock(ts_block, block_i, block_size, remaining, min_delta);

        int2Bytes(min_delta[0], encode_pos, cur_byte);
        encode_pos += 4;
        int2Bytes(min_delta[1], encode_pos, cur_byte);
        encode_pos += 4;
        int bit_width_final = getBitWith(min_delta[2]);
        intByte2Bytes(bit_width_final, encode_pos, cur_byte);
        encode_pos += 1;
        ArrayList<Integer> final_normal = new ArrayList<>();
        for (int value : ts_block_delta) {
            final_normal.add(value);
        }
        encode_pos = encodeOutlier2Bytes(final_normal, bit_width_final, encode_pos, cur_byte);
        return encode_pos;
    }

    public static int BOSEncoder(
            int[] data, int block_size, byte[] encoded_result) {
        block_size++;

        int length_all = data.length;

        int encode_pos = 0;
        int2Bytes(length_all, encode_pos, encoded_result);
        encode_pos += 4;

        int block_num = length_all / block_size;
        int2Bytes(block_size, encode_pos, encoded_result);
        encode_pos += 4;

        for (int i = 0; i < block_num; i++) {
            encode_pos = BOSBlockEncoder(data, i, block_size, block_size, encode_pos, encoded_result);
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

        }

        return encode_pos;
    }

    public static int EncodeBits(int num,
            int bit_width,
            int encode_pos,
            byte[] cur_byte,
            int[] bit_index_list) {
        int bit_index = bit_index_list[0];

        int remaining_bits = bit_width;

        while (remaining_bits > 0) {
            int available_bits = bit_index;
            int bits_to_write = Math.min(BIT_IO_STEP, Math.min(available_bits, remaining_bits));

            bit_index = available_bits - bits_to_write;

            int mask = (1 << bits_to_write) - 1;
            int bits = (num >> (remaining_bits - bits_to_write)) & mask;

            cur_byte[encode_pos] &= (byte) ~(mask << bit_index);
            cur_byte[encode_pos] |= (byte) (bits << bit_index);

            remaining_bits -= bits_to_write;
            if (bit_index == 0) {
                bit_index = 8;
                encode_pos++;
            }
        }
        bit_index_list[0] = bit_index;
        return encode_pos;
    }

    private static int BOSBlockEncoderImprove(int[] ts_block, int block_i, int block_size, int remaining,
            int encode_pos, byte[] cur_byte) {

        int[] min_delta = new int[3];
        int[] ts_block_delta = getAbsDeltaTsBlock(ts_block, block_i, block_size, remaining, min_delta);

        int2Bytes(min_delta[0], encode_pos, cur_byte);
        encode_pos += 4;
        int2Bytes(min_delta[1], encode_pos, cur_byte);
        encode_pos += 4;
        int bit_width_final = getBitWith(min_delta[2]);
        intByte2Bytes(bit_width_final, encode_pos, cur_byte);
        encode_pos += 1;
        int[] bit_index_list = new int[1];
        bit_index_list[0] = 8;
        for (int value : ts_block_delta) {
            encode_pos = EncodeBits(value, bit_width_final, encode_pos, cur_byte, bit_index_list);
        }
        if (bit_index_list[0] != 8) {
            encode_pos++;
        }
        return encode_pos;
    }

    public static int BOSEncoderImprove(
            int[] data, int block_size, byte[] encoded_result) {
        block_size++;

        int length_all = data.length;

        int encode_pos = 0;
        int2Bytes(length_all, encode_pos, encoded_result);
        encode_pos += 4;

        int block_num = length_all / block_size;
        int2Bytes(block_size, encode_pos, encoded_result);
        encode_pos += 4;

        for (int i = 0; i < block_num; i++) {
            encode_pos = BOSBlockEncoderImprove(data, i, block_size, block_size, encode_pos, encoded_result);
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

        }

        return encode_pos;
    }

    public static int BOSBlockDecoder(byte[] encoded, int decode_pos, int[] value_list, int block_size,
            int[] value_pos_arr) {

        int value0 = bytes2Integer(encoded, decode_pos, 4);
        decode_pos += 4;
        value_list[value_pos_arr[0]] = value0;
        value_pos_arr[0]++;

        int min_delta = bytes2Integer(encoded, decode_pos, 4);
        decode_pos += 4;

        int bit_width_final = bytes2Integer(encoded, decode_pos, 1);
        decode_pos += 1;

        ArrayList<Integer> decode_pos_normal = new ArrayList<>();
        ArrayList<Integer> final_normal = decodeOutlier2Bytes(encoded, decode_pos, bit_width_final, block_size,
                decode_pos_normal);

        decode_pos = decode_pos_normal.get(0);
        int normal_i = 0;
        int pre_v = value0;

        for (int i = 0; i < block_size; i++) {
            int current_delta = min_delta + final_normal.get(normal_i);
            pre_v = current_delta + pre_v;
            value_list[value_pos_arr[0]] = pre_v;
            value_pos_arr[0]++;
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
        block_size--;

        int[] value_pos_arr = new int[1];
        for (int k = 0; k < block_num; k++) {

            decode_pos = BOSBlockDecoder(encoded, decode_pos, value_list, block_size, value_pos_arr);

        }

        if (remain_length <= 3) {
            for (int i = 0; i < remain_length; i++) {
                int value_end = bytes2Integer(encoded, decode_pos, 4);
                decode_pos += 4;
                value_list[value_pos_arr[0]] = value_end;
                value_pos_arr[0]++;
            }
        } else {
            remain_length--;
            BOSBlockDecoder(encoded, decode_pos, value_list, remain_length, value_pos_arr);
        }
    }

    public static int[] decodeToIntArray(byte[] encoded) {
        int decode_pos = 0;
        int length_all = bytes2Integer(encoded, decode_pos, 4);
        decode_pos += 4;
        int block_size = bytes2Integer(encoded, decode_pos, 4);
        decode_pos += 4;

        int block_num = length_all / block_size;
        int remain_length = length_all - block_num * block_size;

        int[] value_list = new int[length_all + block_size];
        block_size--;

        int[] value_pos_arr = new int[1];
        for (int k = 0; k < block_num; k++) {
            decode_pos = BOSBlockDecoder(encoded, decode_pos, value_list, block_size, value_pos_arr);
        }

        if (remain_length <= 3) {
            for (int i = 0; i < remain_length; i++) {
                int value_end = bytes2Integer(encoded, decode_pos, 4);
                decode_pos += 4;
                value_list[value_pos_arr[0]] = value_end;
                value_pos_arr[0]++;
            }
        } else {
            remain_length--;
            decode_pos = BOSBlockDecoder(encoded, decode_pos, value_list, remain_length, value_pos_arr);
        }
        return Arrays.copyOf(value_list, length_all);
    }

    public static int[] decodeToIntArrayImprove(byte[] encoded) {
        int decode_pos = 0;
        int length_all = bytes2Integer(encoded, decode_pos, 4);
        decode_pos += 4;
        int block_size = bytes2Integer(encoded, decode_pos, 4);
        decode_pos += 4;

        int block_num = length_all / block_size;
        int remain_length = length_all - block_num * block_size;

        int[] value_list = new int[length_all + block_size];
        block_size--;

        int[] value_pos_arr = new int[1];
        for (int k = 0; k < block_num; k++) {
            decode_pos =
                BOSBlockDecoderImprove(encoded, decode_pos, value_list, block_size, value_pos_arr);
        }

        if (remain_length <= 3) {
            for (int i = 0; i < remain_length; i++) {
                int value_end = bytes2Integer(encoded, decode_pos, 4);
                decode_pos += 4;
                value_list[value_pos_arr[0]] = value_end;
                value_pos_arr[0]++;
            }
        } else {
            remain_length--;
            decode_pos =
                BOSBlockDecoderImprove(encoded, decode_pos, value_list, remain_length, value_pos_arr);
        }
        return Arrays.copyOf(value_list, length_all);
    }

    public static int DecodeBits(byte[] cur_byte, int bit_width, int[] decode_pos_list) {
        int decode_pos = decode_pos_list[0];
        int bit_index = decode_pos_list[1];
        int remaining_bits = bit_width;
        int num = 0;

        while (remaining_bits > 0) {
            int available_bits = bit_index;
            int bits_to_read = Math.min(BIT_IO_STEP, Math.min(available_bits, remaining_bits));

            int mask = (1 << bits_to_read) - 1;
            int bits = (cur_byte[decode_pos] >> (available_bits - bits_to_read)) & mask;

            num = (num << bits_to_read) | bits;

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

    public static int BOSBlockDecoderImprove(byte[] encoded, int decode_pos, int[] value_list, int block_size,
            int[] value_pos_arr) {

        int value0 = bytes2Integer(encoded, decode_pos, 4);
        decode_pos += 4;
        value_list[value_pos_arr[0]] = value0;
        value_pos_arr[0]++;

        int min_delta = bytes2Integer(encoded, decode_pos, 4);
        decode_pos += 4;

        int bit_width_final = bytes2Integer(encoded, decode_pos, 1);
        decode_pos += 1;

        int[] decode_list = new int[2];
        decode_list[0] = decode_pos;
        decode_list[1] = 8;
        int pre_v = value0;
        for (int i = 0; i < block_size; i++) {
            int cur_delta = min_delta + DecodeBits(encoded, bit_width_final, decode_list);
            pre_v += cur_delta;
            value_list[value_pos_arr[0]++] = pre_v;
        }
        if (decode_list[1] != 8) {
            return decode_list[0] + 1;
        } else {
            return decode_list[0];
        }
        // value_pos_arr[0] = valuePos;
        // return decode_list[0];

        // ArrayList<Integer> decode_pos_normal = new ArrayList<>();
        // ArrayList<Integer> final_normal = decodeOutlier2Bytes(encoded, decode_pos,
        // bit_width_final, block_size, decode_pos_normal);
        //
        // decode_pos = decode_pos_normal.get(0);
        // int normal_i = 0;
        //// int pre_v = value0;
        //
        // for (int i = 0; i < block_size; i++) {
        // int current_delta = min_delta + final_normal.get(normal_i) ;
        // pre_v = current_delta + pre_v;
        // value_list[value_pos_arr[0]] = pre_v;
        // value_pos_arr[0]++;
        // }
        //
        // return decode_pos;
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
        block_size--;

        int[] value_pos_arr = new int[1];
        for (int k = 0; k < block_num; k++) {

            decode_pos = BOSBlockDecoderImprove(encoded, decode_pos, value_list, block_size, value_pos_arr);

        }

        if (remain_length <= 3) {
            for (int i = 0; i < remain_length; i++) {
                int value_end = bytes2Integer(encoded, decode_pos, 4);
                decode_pos += 4;
                value_list[value_pos_arr[0]] = value_end;
                value_pos_arr[0]++;
            }
        } else {
            remain_length--;
            BOSBlockDecoderImprove(encoded, decode_pos, value_list, remain_length, value_pos_arr);
        }
    }

    private static void addToArchiveCompression(SevenZOutputFile out, File file, String dir) {
        String name = dir + File.separator + file.getName();
        if (dir.equals(".")) {
            name = file.getName();
        }
        if (file.isFile()) {
            SevenZArchiveEntry entry = null;
            FileInputStream in = null;
            try {
                entry = out.createArchiveEntry(file, name);
                out.putArchiveEntry(entry);
                in = new FileInputStream(file);
                byte[] b = new byte[1024];
                int count = 0;
                while ((count = in.read(b)) > 0) {
                    out.write(b, 0, count);
                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    out.closeArchiveEntry();
                    in.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }

            }
        } else if (file.isDirectory()) {
            File[] children = file.listFiles();
            if (children != null) {
                for (File child : children) {
                    addToArchiveCompression(out, child, name);
                }
            }
        } else {
            System.out.println(file.getName() + " is not supported");
        }
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

        String outputPath = output_parent_dir + "ts2diff.csv";

        int block_size = 1024;

        int repeatTime = 500;


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
        File[] csvFiles = directory.listFiles((dir, name) -> name.endsWith(".csv"));

        for (File file : csvFiles) {
            // f = tempList[1];
            // System.out.println(f);
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
            int[] data2_arr = new int[data1.size()];
            int max_mul = (int) Math.pow(10, max_decimal);
            for (int i = 0; i < data1.size(); i++) {
                data2_arr[i] = (int) (data1.get(i) * max_mul);
            }

            System.out.println(max_decimal);
            byte[] encoded_result = new byte[data2_arr.length * 8];
            long encodeTime = 0;
            long decodeTime = 0;
            double ratio = 0;
            double compressed_size = 0;

            int length = 0;

            long s = System.nanoTime();
            for (int repeat = 0; repeat < repeatTime; repeat++) {
                // length = BOSEncoder(data2_arr, block_size, encoded_result);
                length = BOSEncoderImprove(data2_arr, block_size, encoded_result);
            }

            long e = System.nanoTime();
            encodeTime += ((e - s) / repeatTime);
            compressed_size += length;

            double ratioTmp;

            ratioTmp = compressed_size / (double) (data1.size() * Long.BYTES);

            ratio += ratioTmp;

            s = System.nanoTime();

            for (int repeat = 0; repeat < repeatTime; repeat++) {
                // BOSDecoder(encoded_result);
                BOSDecoderImprove(encoded_result);
            }

            e = System.nanoTime();
            decodeTime += ((e - s) / repeatTime);

            String[] record = {
                    datasetName,
                    "TS2DIFF",
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
