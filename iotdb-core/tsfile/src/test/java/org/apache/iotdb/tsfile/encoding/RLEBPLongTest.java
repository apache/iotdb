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

public class RLEBPLongTest {

    private static final int BIT_IO_STEP = 4;

    public static int getBitWith(int num) {
        if (num == 0)
            return 1;
        else
            return 32 - Integer.numberOfLeadingZeros(num);
    }

    public static int getBitWith(long num) {
        if (num == 0)
            return 1;
        else
            return 64 - Long.numberOfLeadingZeros(num);
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

    public static long bytes2Long(byte[] encoded, int start, int num) {
        long value = 0;

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

    public static long[] getAbsDeltaTsBlock(
            long[] ts_block,
            int i,
            int block_size,
            int remaining,
            long[] min_delta,
            ArrayList<Integer> repeat_count) {
        long[] ts_block_delta = new long[remaining];

        long value_delta_min = Long.MAX_VALUE;
        long value_delta_max = Long.MIN_VALUE;
        int base = i * block_size;
        int end = i * block_size + remaining;
        for (int j = base; j < end; j++) {

            long integer = ts_block[j];
            if (integer < value_delta_min)
                value_delta_min = integer;
            if (integer > value_delta_max) {
                value_delta_max = integer;
            }
        }
        long pre_delta = ts_block[i * block_size] - value_delta_min;
        int pre_count = 1;

        min_delta[0] = (value_delta_min);
        int repeat_i = 0;
        int ts_block_delta_i = 0;
        for (int j = base + 1; j < end; j++) {
            long delta = ts_block[j] - value_delta_min;
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
        long[] new_ts_block_delta = new long[ts_block_delta_i];
        System.arraycopy(ts_block_delta, 0, new_ts_block_delta, 0, ts_block_delta_i);

        return new_ts_block_delta;
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

    public static int EncodeBits(long num,
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
            int bits = (int) (num >> (remaining_bits - bits_to_write)) & mask;

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

    private static int BOSBlockEncoderImprove(long[] ts_block, int block_i, int block_size, int remaining,
            int encode_pos, byte[] cur_byte) {

        ArrayList<Integer> repeat_count = new ArrayList<>();
        int init_block_size = block_size;

        long[] min_delta = new long[3];
        long[] ts_block_delta = getAbsDeltaTsBlock(ts_block, block_i, init_block_size, remaining, min_delta,
                repeat_count);

        long max_delta_value = min_delta[2];

        long2Bytes(min_delta[1], encode_pos, cur_byte);
        encode_pos += 8;

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
        for (long cur_value : ts_block_delta) {
            encode_pos = EncodeBits(cur_value, bit_width_final, encode_pos, cur_byte, bit_index_list);
        }
        if (bit_index_list[0] != 8) {
            encode_pos++;
        }

        return encode_pos;
    }

    public static int BOSEncoderImprove(
            long[] data, int block_size, byte[] encoded_result) {

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
                long2Bytes(data[data.length - i], encode_pos, encoded_result);
                encode_pos += 8;
            }

        } else {

            int start = block_num * block_size;
            int remaining = length_all - start;

            encode_pos = BOSBlockEncoderImprove(data, block_num, block_size, remaining, encode_pos, encoded_result);

        }

        return encode_pos;
    }

    public static int DecodeBits(byte[] cur_byte, int bit_width, int[] decode_pos_list) {
        int decode_pos = decode_pos_list[0];
        int bit_index = decode_pos_list[1];
        int remaining_bits = bit_width;
        int num = 0;

        while (remaining_bits > 0) {
            int available_bits = bit_index;
            int bits_to_read = Math.min(available_bits, remaining_bits);

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

    public static long DecodeBitsLong(byte[] cur_byte, int bit_width, int[] decode_pos_list) {
        int decode_pos = decode_pos_list[0];
        int bit_index = decode_pos_list[1];
        int remaining_bits = bit_width;
        long num = 0;

        while (remaining_bits > 0) {
            int available_bits = bit_index;
            int bits_to_read = Math.min(available_bits, remaining_bits);

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

    public static int BOSBlockDecoderImprove(byte[] encoded, int decode_pos, long[] value_list, int init_block_size,
            int block_size, int[] value_pos_arr) {

        long min_delta = bytes2Long(encoded, decode_pos, 8);
        decode_pos += 8;

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
            decode_pos = decode_list[0];
        }

        int cur_block_size = block_size;
        for (int i = 1; i < count_size; i += 2) {
            cur_block_size -= (repeat_count.get(i) - 1);
        }

        int bit_width_final = bytes2Integer(encoded, decode_pos, 1);
        decode_pos += 1;

        long pre_v;
        int cur_i = 0;
        int repeat_i = 0;

        decode_list[0] = decode_pos;
        decode_list[1] = 8;
        for (int i = 0; i < cur_block_size; i++) {
            pre_v = min_delta + DecodeBitsLong(encoded, bit_width_final, decode_list);
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

        long[] value_list = new long[length_all + block_size];
        int[] value_pos_arr = new int[1];

        for (int k = 0; k < block_num; k++) {
            decode_pos = BOSBlockDecoderImprove(encoded, decode_pos, value_list, block_size, block_size, value_pos_arr);        }
        if (remain_length <= 3) {
            for (int i = 0; i < remain_length; i++) {
                long value_end = bytes2Long(encoded, decode_pos, 8);
                decode_pos += 8;
                value_list[value_pos_arr[0]] = value_end;
                value_pos_arr[0]++;
            }
        } else {
            BOSBlockDecoderImprove(encoded, decode_pos, value_list, block_size, remain_length, value_pos_arr);
        }
    }

    public static long[] decodeToLongArray(byte[] encoded) {
        int decode_pos = 0;
        int length_all = bytes2Integer(encoded, decode_pos, 4);
        decode_pos += 4;
        int block_size = bytes2Integer(encoded, decode_pos, 4);
        decode_pos += 4;

        int block_num = length_all / block_size;
        int remain_length = length_all - block_num * block_size;

        long[] value_list = new long[length_all + block_size];
        int[] value_pos_arr = new int[1];

        for (int k = 0; k < block_num; k++) {
            decode_pos =
                BOSBlockDecoderImprove(
                    encoded, decode_pos, value_list, block_size, block_size, value_pos_arr);
        }
        if (remain_length <= 3) {
            for (int i = 0; i < remain_length; i++) {
                long value_end = bytes2Long(encoded, decode_pos, 8);
                decode_pos += 8;
                value_list[value_pos_arr[0]] = value_end;
                value_pos_arr[0]++;
            }
        } else {
            decode_pos =
                BOSBlockDecoderImprove(
                    encoded, decode_pos, value_list, block_size, remain_length, value_pos_arr);
        }
        return Arrays.copyOf(value_list, length_all);
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
        // String parent_dir = "/Users/xiaojinzhao/Documents/GitHub/subcolumn/";
        String parent_dir = "D:/github/xjz17/subcolumn/";

        String input_parent_dir = parent_dir + "dataset/";

        String output_parent_dir = parent_dir + "result/";

        String outputPath = output_parent_dir + "rle_long.csv";

        int block_size = 256;

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

            long[] data2_arr = new long[data1.size()];
            long max_mul = (long) Math.pow(10, max_decimal);
            for (int i = 0; i < data1.size(); i++) {
                data2_arr[i] = (long) (data1.get(i) * max_mul);
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

}
