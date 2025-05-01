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

import static java.lang.Math.pow;

public class TSDIFFTest {

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
        // return encode_pos;
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

        int[] min_delta = new int[3];
        int[] ts_block_delta = getAbsDeltaTsBlock(ts_block, block_i, block_size, remaining, min_delta);

        int2Bytes(min_delta[0], encode_pos, cur_byte);
        encode_pos += 4;
        int2Bytes(min_delta[1], encode_pos, cur_byte);
        encode_pos += 4;
        int bit_width_final = getBitWith(min_delta[2]);
        intByte2Bytes(bit_width_final, encode_pos, cur_byte);
        encode_pos += 1;
        // ArrayList<Integer> final_normal = new ArrayList<>();
        int[] bit_index_list = new int[1];
        bit_index_list[0] = 8;
        for (int value : ts_block_delta) {
            encode_pos = EncodeBits(value, bit_width_final, encode_pos, cur_byte, bit_index_list);
        }
        if (bit_index_list[0] != 8) {
            encode_pos++;
        }
        // encode_pos = encodeOutlier2Bytes(final_normal,
        // bit_width_final,encode_pos,cur_byte);
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
    public void testTSDIFF() throws IOException {
        String parent_dir = "D:/github/xjz17/subcolumn/dataset/";
        // String parent_dir = "D:/encoding-subcolumn/";

        // String output_parent_dir = "D:/encoding-subcolumn/result/";
        String output_parent_dir = "D:/github/xjz17/subcolumn/result/";

        String outputPath = output_parent_dir + "ts2diff.csv";

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
        File directory = new File(parent_dir);
        // File[] csvFiles = directory.listFiles();
        File[] csvFiles = directory.listFiles((dir, name) -> name.endsWith(".csv"));

        for (File file : csvFiles) {
            // f = tempList[1];
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
                length = BOSEncoder(data2_arr, block_size, encoded_result);
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
                BOSDecoder(encoded_result);
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

    @Test
    public void testTransData() throws IOException {
        // String parent_dir = "D:/github/xjz17/subcolumn/";

        String parent_dir = "D:/encoding-subcolumn/";

        String output_parent_dir = "D:/encoding-subcolumn/trans_data_result/";

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

        String outputPath = output_parent_dir + "ts2diff.csv";
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
                    length = BOSEncoder(data2_arr, dataset_block_size.get(file_i), encoded_result);
                }

                long e = System.nanoTime();
                encodeTime += ((e - s) / repeatTime);
                compressed_size += length;
                double ratioTmp = compressed_size / (double) (data1.size() * Integer.BYTES);
                ratio += ratioTmp;
                s = System.nanoTime();

                for (int repeat = 0; repeat < repeatTime; repeat++) {
                    BOSDecoder(encoded_result);
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
                    "TS2DIFF",
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

    @Test
    public void ExpTest() throws IOException {
        String parent_dir = "/Users/xiaojinzhao/Documents/GitHub/encoding-outlier/";// your data path
        // String parent_dir =
        // "/Users/zihanguo/Downloads/R/outlier/outliier_code/encoding-outlier/";
        String output_parent_dir = parent_dir
                + "icde0802/supply_experiment/R2O3_lower_outlier_compare/compression_ratio/tsdiff";
        String input_parent_dir = parent_dir + "trans_data/";
        ArrayList<String> input_path_list = new ArrayList<>();
        ArrayList<String> output_path_list = new ArrayList<>();
        ArrayList<String> dataset_name = new ArrayList<>();
        ArrayList<Integer> dataset_block_size = new ArrayList<>();

        dataset_name.add("Synthetic_Exp_100");
        dataset_name.add("Synthetic_Exp_1000");
        dataset_name.add("Synthetic_Exp_10000");
        dataset_name.add("Synthetic_Exp_100000");
        dataset_name.add("Synthetic_Exp_1000000");

        for (String value : dataset_name) {
            input_path_list.add(input_parent_dir + value);
            dataset_block_size.add(1024);
        }

        output_path_list.add(output_parent_dir + "/Exp_100.csv"); // 0
        // dataset_block_size.add(1024);
        output_path_list.add(output_parent_dir + "/Exp_1000.csv");// 1
        // dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/Exp_10000.csv");// 2
        output_path_list.add(output_parent_dir + "/Exp_100000.csv");// 2
        output_path_list.add(output_parent_dir + "/Exp_1000000.csv");// 2

        int repeatTime2 = 1000;
        // for (int file_i = 8; file_i < 9; file_i++) {

        for (int file_i = input_path_list.size() - 1; file_i >= 0; file_i--) {

            String inputPath = input_path_list.get(file_i);
            System.out.println(inputPath);
            String Output = output_path_list.get(file_i);

            File file = new File(inputPath);
            File[] tempList = file.listFiles();

            CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);

            String[] head = {
                    "Input Direction",
                    "Encoding Algorithm",
                    "Encoding Time",
                    "Decoding Time",
                    "Points",
                    "Compressed Size",
                    "Compression Ratio"
            };
            writer.writeRecord(head); // write header to output file

            assert tempList != null;

            for (File f : tempList) {
                System.out.println(f);
                InputStream inputStream = Files.newInputStream(f.toPath());

                CsvReader loader = new CsvReader(inputStream, StandardCharsets.UTF_8);
                ArrayList<Integer> data1 = new ArrayList<>();
                ArrayList<Integer> data2 = new ArrayList<>();

                loader.readHeaders();
                while (loader.readRecord()) {
                    data1.add(Integer.valueOf(loader.getValues()[0]));
                    data2.add(Integer.valueOf(loader.getValues()[1]));
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
                for (int repeat = 0; repeat < repeatTime2; repeat++) {
                    length = BOSEncoder(data2_arr, dataset_block_size.get(file_i), encoded_result);
                }

                long e = System.nanoTime();
                encodeTime += ((e - s) / repeatTime2);
                compressed_size += length;
                double ratioTmp = compressed_size / (double) (data1.size() * Integer.BYTES);
                ratio += ratioTmp;
                s = System.nanoTime();
                for (int repeat = 0; repeat < repeatTime2; repeat++)
                    BOSDecoder(encoded_result);
                e = System.nanoTime();
                decodeTime += ((e - s) / repeatTime2);

                String[] record = {
                        f.toString(),
                        "RLE+BOS-V",
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

    @Test
    public void compressBPTest() throws IOException {
        // String parent_dir =
        // "/Users/xiaojinzhao/Documents/GitHub/encoding-outlier/";// your data path
        String parent_dir = "/Users/zihanguo/Downloads/R/outlier/outliier_code/encoding-outlier/";
        String output_parent_dir = parent_dir
                + "icde0802/supply_experiment/R3O2_compare_compression/compression_ratio/bp_comp";
        String input_parent_dir = parent_dir + "trans_data/";
        ArrayList<String> input_path_list = new ArrayList<>();
        ArrayList<String> output_path_list = new ArrayList<>();
        ArrayList<String> dataset_name = new ArrayList<>();
        ArrayList<Integer> dataset_block_size = new ArrayList<>();
        dataset_name.add("CS-Sensors");
        dataset_name.add("Metro-Traffic");
        dataset_name.add("USGS-Earthquakes");
        dataset_name.add("YZ-Electricity");
        dataset_name.add("GW-Magnetic");
        dataset_name.add("TY-Fuel");
        dataset_name.add("Cyber-Vehicle");
        dataset_name.add("Vehicle-Charge");
        dataset_name.add("Nifty-Stocks");
        dataset_name.add("TH-Climate");
        dataset_name.add("TY-Transport");
        dataset_name.add("EPM-Education");

        for (String value : dataset_name) {
            input_path_list.add(input_parent_dir + value);
            dataset_block_size.add(1024);
        }

        output_path_list.add(output_parent_dir + "/CS-Sensors_ratio.csv"); // 0
        // dataset_block_size.add(1024);
        output_path_list.add(output_parent_dir + "/Metro-Traffic_ratio.csv");// 1
        // dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/USGS-Earthquakes_ratio.csv");// 2
        // dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/YZ-Electricity_ratio.csv"); // 3
        // dataset_block_size.add(256);
        output_path_list.add(output_parent_dir + "/GW-Magnetic_ratio.csv"); // 4
        // dataset_block_size.add(1024);
        output_path_list.add(output_parent_dir + "/TY-Fuel_ratio.csv");// 5
        // dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/Cyber-Vehicle_ratio.csv"); // 6
        // dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/Vehicle-Charge_ratio.csv");// 7
        // dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/Nifty-Stocks_ratio.csv");// 8
        // dataset_block_size.add(1024);
        output_path_list.add(output_parent_dir + "/TH-Climate_ratio.csv");// 9
        // dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/TY-Transport_ratio.csv");// 10
        // dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/EPM-Education_ratio.csv");// 11
        // dataset_block_size.add(1024);

        int repeatTime2 = 50;
        // for (int file_i = 8; file_i < 9; file_i++) {

        for (int file_i = 0; file_i < input_path_list.size(); file_i++) {
            // for (int file_i = input_path_list.size()-1; file_i >=0 ; file_i--) {

            String inputPath = input_path_list.get(file_i);
            // System.out.println(inputPath);
            String Output = output_path_list.get(file_i);

            File file = new File(inputPath);
            File[] tempList = file.listFiles();

            CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);

            String[] head = {
                    "Input Direction",
                    "Encoding Algorithm",
                    // "Compress Algorithm",
                    "Encoding Time",
                    "Decoding Time",
                    "Points",
                    "Compressed Size",
                    "Compression Ratio"
            };
            writer.writeRecord(head); // write header to output file

            assert tempList != null;

            for (File f : tempList) {
                // System.out.println(f);
                InputStream inputStream = Files.newInputStream(f.toPath());

                CsvReader loader = new CsvReader(inputStream, StandardCharsets.UTF_8);
                ArrayList<Integer> data1 = new ArrayList<>();
                ArrayList<Integer> data2 = new ArrayList<>();

                loader.readHeaders();
                while (loader.readRecord()) {
                    data1.add(Integer.valueOf(loader.getValues()[0]));
                    data2.add(Integer.valueOf(loader.getValues()[1]));
                }
                inputStream.close();

                int[] data2_arr = new int[data1.size()];
                for (int i = 0; i < data2.size(); i++) {
                    data2_arr[i] = data2.get(i);
                }
                byte[] encoded_result = new byte[data2_arr.length * 4];
                long encodeTime = 0;
                long decodeTime = 0;
                int length = 0;

                long s = System.nanoTime();
                for (int repeat = 0; repeat < repeatTime2; repeat++) {
                    length = BOSEncoder(data2_arr, dataset_block_size.get(file_i), encoded_result);
                }
                long e = System.nanoTime();
                encodeTime += ((e - s) / repeatTime2);
                double ratio = 0;
                double compressed_size = 0;
                long compressTime = encodeTime;
                s = System.nanoTime();
                byte[] compressed = new byte[0];
                for (int repeat = 0; repeat < repeatTime2; repeat++) {
                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    BZip2CompressorOutputStream bzip2Out = new BZip2CompressorOutputStream(baos);
                    bzip2Out.write(encoded_result);
                    bzip2Out.finish();
                    compressed = baos.toByteArray();
                }
                e = System.nanoTime();
                compressTime += ((e - s) / repeatTime2);

                // test compression ratio and compressed size
                compressed_size += compressed.length;
                double ratioTmp = compressed_size / (double) (data1.size() * Integer.BYTES);
                ratio += ratioTmp;
                s = System.nanoTime();
                // for (int repeat = 0; repeat < repeatTime2; repeat++)
                // BOSDecoderImprove(encoded_result);
                e = System.nanoTime();
                decodeTime += ((e - s) / repeatTime2);

                String[] record = {
                        f.toString(),
                        "7-Zip",
                        String.valueOf(compressTime),
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

    @Test
    public void compressBPTest2() throws IOException {
        // String parent_dir =
        // "/Users/xiaojinzhao/Documents/GitHub/encoding-outlier/";// your data path
        String parent_dir = "/Users/zihanguo/Downloads/R/outlier/outliier_code/encoding-outlier/";
        String output_parent_dir = parent_dir
                + "icde0802/supply_experiment/R3O2_compare_compression/compression_ratio/lz4_comp";
        String input_parent_dir = parent_dir + "trans_data/";
        ArrayList<String> input_path_list = new ArrayList<>();
        ArrayList<String> output_path_list = new ArrayList<>();
        ArrayList<String> dataset_name = new ArrayList<>();
        ArrayList<Integer> dataset_block_size = new ArrayList<>();
        dataset_name.add("CS-Sensors");
        dataset_name.add("Metro-Traffic");
        dataset_name.add("USGS-Earthquakes");
        dataset_name.add("YZ-Electricity");
        dataset_name.add("GW-Magnetic");
        dataset_name.add("TY-Fuel");
        dataset_name.add("Cyber-Vehicle");
        dataset_name.add("Vehicle-Charge");
        dataset_name.add("Nifty-Stocks");
        dataset_name.add("TH-Climate");
        dataset_name.add("TY-Transport");
        dataset_name.add("EPM-Education");

        for (String value : dataset_name) {
            input_path_list.add(input_parent_dir + value);
            dataset_block_size.add(1024);
        }

        output_path_list.add(output_parent_dir + "/CS-Sensors_ratio.csv"); // 0
        // dataset_block_size.add(1024);
        output_path_list.add(output_parent_dir + "/Metro-Traffic_ratio.csv");// 1
        // dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/USGS-Earthquakes_ratio.csv");// 2
        // dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/YZ-Electricity_ratio.csv"); // 3
        // dataset_block_size.add(256);
        output_path_list.add(output_parent_dir + "/GW-Magnetic_ratio.csv"); // 4
        // dataset_block_size.add(1024);
        output_path_list.add(output_parent_dir + "/TY-Fuel_ratio.csv");// 5
        // dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/Cyber-Vehicle_ratio.csv"); // 6
        // dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/Vehicle-Charge_ratio.csv");// 7
        // dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/Nifty-Stocks_ratio.csv");// 8
        // dataset_block_size.add(1024);
        output_path_list.add(output_parent_dir + "/TH-Climate_ratio.csv");// 9
        // dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/TY-Transport_ratio.csv");// 10
        // dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/EPM-Education_ratio.csv");// 11
        // dataset_block_size.add(1024);

        int repeatTime2 = 5;
        // for (int file_i = 8; file_i < 9; file_i++) {
        CompressionType[] compressList = {
                CompressionType.LZ4,
        };

        for (int file_i = input_path_list.size() - 1; file_i >= 0; file_i--) {

            String inputPath = input_path_list.get(file_i);
            System.out.println(inputPath);
            String Output = output_path_list.get(file_i);

            File file = new File(inputPath);
            File[] tempList = file.listFiles();

            CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);

            String[] head = {
                    "Input Direction",
                    "Encoding Algorithm",
                    // "Compress Algorithm",
                    "Encoding Time",
                    "Decoding Time",
                    "Points",
                    "Compressed Size",
                    "Compression Ratio"
            };
            writer.writeRecord(head); // write header to output file

            assert tempList != null;

            for (File f : tempList) {
                System.out.println(f);
                InputStream inputStream = Files.newInputStream(f.toPath());

                CsvReader loader = new CsvReader(inputStream, StandardCharsets.UTF_8);
                ArrayList<Integer> data1 = new ArrayList<>();
                ArrayList<Integer> data2 = new ArrayList<>();

                loader.readHeaders();
                while (loader.readRecord()) {
                    data1.add(Integer.valueOf(loader.getValues()[0]));
                    data2.add(Integer.valueOf(loader.getValues()[1]));
                }
                inputStream.close();

                int[] data2_arr = new int[data1.size()];
                for (int i = 0; i < data2.size(); i++) {
                    data2_arr[i] = data2.get(i);
                }
                byte[] encoded_result = new byte[data2_arr.length * 4];
                long encodeTime = 0;
                long decodeTime = 0;
                int length = 0;

                long s = System.nanoTime();
                for (int repeat = 0; repeat < repeatTime2; repeat++) {
                    length = BOSEncoder(data2_arr, dataset_block_size.get(file_i), encoded_result);
                }
                long e = System.nanoTime();
                encodeTime += ((e - s) / repeatTime2);

                for (CompressionType comp : compressList) {
                    double ratio = 0;
                    double compressed_size = 0;
                    long compressTime = encodeTime;
                    System.out.println(compressTime);

                    s = System.nanoTime();
                    ICompressor compressor = ICompressor.getCompressor(comp);
                    byte[] compressed = new byte[0];
                    for (int repeat = 0; repeat < repeatTime2; repeat++) {
                        compressed = compressor.compress(encoded_result);
                    }
                    e = System.nanoTime();
                    compressTime += ((e - s) / repeatTime2);
                    System.out.println(compressTime - encodeTime);
                    // test compression ratio and compressed size
                    compressed_size += compressed.length;
                    double ratioTmp = compressed_size / (double) (data1.size() * Integer.BYTES);
                    ratio += ratioTmp;
                    s = System.nanoTime();
                    for (int repeat = 0; repeat < repeatTime2; repeat++)
                        BOSDecoder(encoded_result);
                    e = System.nanoTime();
                    decodeTime += ((e - s) / repeatTime2);

                    String[] record = {
                            f.toString(),
                            comp.toString(),
                            String.valueOf(compressTime),
                            String.valueOf(decodeTime),
                            String.valueOf(data1.size()),
                            String.valueOf(compressed_size),
                            String.valueOf(ratio)
                    };
                    writer.writeRecord(record);
                    // System.out.println(ratio);
                }
            }
            writer.close();
        }
    }

    @Test
    public void BOSQueryTest() throws IOException, InterruptedException {
        String parent_dir = "/Users/xiaojinzhao/Documents/GitHub/encoding-outlier/"; // your data path
        // String parent_dir =
        // "/Users/zihanguo/Downloads/R/outlier/outliier_code/encoding-outlier/";
        String output_parent_dir = parent_dir + "icde0802/supply_experiment/R2O2_query_processing/time/tsdiff";
        String input_parent_dir = parent_dir + "trans_data/";
        ArrayList<String> input_path_list = new ArrayList<>();
        ArrayList<String> output_path_list = new ArrayList<>();
        ArrayList<String> dataset_name = new ArrayList<>();
        ArrayList<Integer> dataset_block_size = new ArrayList<>();
        dataset_name.add("CS-Sensors");
        dataset_name.add("Metro-Traffic");
        dataset_name.add("USGS-Earthquakes");
        dataset_name.add("YZ-Electricity");
        dataset_name.add("GW-Magnetic");
        dataset_name.add("TY-Fuel");
        dataset_name.add("Cyber-Vehicle");
        dataset_name.add("Vehicle-Charge");
        dataset_name.add("Nifty-Stocks");
        dataset_name.add("TH-Climate");
        dataset_name.add("TY-Transport");
        dataset_name.add("EPM-Education");

        for (String value : dataset_name) {
            input_path_list.add(input_parent_dir + value);
            dataset_block_size.add(1024);
        }

        output_path_list.add(output_parent_dir + "/CS-Sensors_ratio.csv"); // 0
        // dataset_block_size.add(1024);
        output_path_list.add(output_parent_dir + "/Metro-Traffic_ratio.csv");// 1
        // dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/USGS-Earthquakes_ratio.csv");// 2
        // dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/YZ-Electricity_ratio.csv"); // 3
        // dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/GW-Magnetic_ratio.csv"); // 4
        // dataset_block_size.add(1024);
        output_path_list.add(output_parent_dir + "/TY-Fuel_ratio.csv");// 5
        // dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/Cyber-Vehicle_ratio.csv"); // 6
        // dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/Vehicle-Charge_ratio.csv");// 7
        // dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/Nifty-Stocks_ratio.csv");// 8
        // dataset_block_size.add(1024);
        output_path_list.add(output_parent_dir + "/TH-Climate_ratio.csv");// 9
        // dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/TY-Transport_ratio.csv");// 10
        // dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/EPM-Education_ratio.csv");// 11
        // dataset_block_size.add(1024);

        int repeatTime2 = 75;
        // for (int file_i = 1; file_i < 2; file_i++) {

        for (int file_i = 0; file_i < input_path_list.size(); file_i++) {

            String inputPath = input_path_list.get(file_i);
            System.out.println(inputPath);
            String Output = output_path_list.get(file_i);

            File file = new File(inputPath);
            File[] tempList = file.listFiles();

            CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);

            String[] head = {
                    "Input Direction",
                    "Encoding Algorithm",
                    "Encoding Time",
                    "Decoding Time",
                    "Insert Time",
                    "Query Time",
                    "Points",
                    "Compressed Size",
                    "Compression Ratio"
            };
            writer.writeRecord(head); // write header to output file

            assert tempList != null;

            for (File f : tempList) {
                // f=tempList[2];
                System.out.println(f);
                // Thread.sleep(5000);
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

                String file_bin_str = "icde0802/supply_experiment/R2O2_query_processing/time/str_1.bin";
                long input_time = 0;
                long s = System.nanoTime();
                for (int repeat = 0; repeat < repeatTime2; repeat++) {
                    long start_encode = System.nanoTime();
                    length = BOSEncoder(data2_arr, dataset_block_size.get(file_i), encoded_result);
                    long end_encode = System.nanoTime();
                    try (FileOutputStream fos = new FileOutputStream(parent_dir + file_bin_str)) {
                        // 只写入前length个元素
                        fos.write(encoded_result, 0, length);
                    } catch (IOException ioe) {
                        ioe.printStackTrace();
                    }
                    long end_io = System.nanoTime();
                    encodeTime += (end_encode - start_encode);
                    input_time += (end_io - end_encode);
                }

                long e = System.nanoTime();
                encodeTime = (encodeTime / repeatTime2);
                input_time = (input_time / repeatTime2);
                compressed_size += length;
                double ratioTmp = compressed_size / (double) (data1.size() * Integer.BYTES);
                ratio += ratioTmp;

                long output_time = 0;
                s = System.nanoTime();
                for (int repeat = 0; repeat < repeatTime2; repeat++) {
                    long start_io = System.nanoTime();

                    try (FileInputStream fis = new FileInputStream(parent_dir + file_bin_str)) {
                        // 读取数据到byte数组中
                        int bytesRead = fis.read(encoded_result);
                        // System.out.println("Number of bytes read: " + bytesRead);
                    } catch (IOException ioe) {
                        ioe.printStackTrace();
                    }
                    long start_decode = System.nanoTime();
                    BOSDecoder(encoded_result);
                    long end_decode = System.nanoTime();
                    decodeTime += (end_decode - start_decode);
                    output_time += (start_decode - start_io);
                }

                e = System.nanoTime();
                decodeTime = (decodeTime / repeatTime2);
                output_time = (output_time / repeatTime2);

                String[] record = {
                        f.toString(),
                        "TS_2DIFF",
                        String.valueOf(encodeTime),
                        String.valueOf(decodeTime),
                        String.valueOf(input_time),
                        String.valueOf(output_time),
                        String.valueOf(data1.size()),
                        String.valueOf(compressed_size),
                        String.valueOf(ratio)
                };
                writer.writeRecord(record);
                System.out.println(ratio);
                // break;
            }
            writer.close();
        }
    }

}
