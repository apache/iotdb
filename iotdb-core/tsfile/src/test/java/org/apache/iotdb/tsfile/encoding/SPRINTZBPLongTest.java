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
import java.util.List;
import java.util.stream.Stream;

public class SPRINTZBPLongTest {

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

    public static int findMedian(int[] arr) {
        if (arr == null || arr.length == 0) {
            throw new IllegalArgumentException("Array is null or empty");
        }
        int n = arr.length;
        return quickSelect(arr, 0, n - 1, n / 2);
    }

    private static int quickSelect(int[] arr, int left, int right, int k) {
        if (left == right) {
            return arr[left];
        }

        int pivotIndex = partition(arr, left, right);
        if (k == pivotIndex) {
            return arr[k];
        } else if (k < pivotIndex) {
            return quickSelect(arr, left, pivotIndex - 1, k);
        } else {
            return quickSelect(arr, pivotIndex + 1, right, k);
        }
    }

    private static int partition(int[] arr, int left, int right) {
        int pivot = arr[right];
        int i = left;
        for (int j = left; j < right; j++) {
            if (arr[j] <= pivot) {
                swap(arr, i, j);
                i++;
            }
        }
        swap(arr, i, right);
        return i;
    }

    private static void swap(int[] arr, int i, int j) {
        int temp = arr[i];
        arr[i] = arr[j];
        arr[j] = temp;
    }

    public static int zigzag(int num) {
        if (num < 0)
            return ((-num) << 1) - 1;
        else
            return num << 1;
    }

    public static long zigzag(long num) {
        if (num < 0)
            return ((-num) << 1) - 1;
        else
            return num << 1;
    }

    public static int deZigzag(int num) {
        if (num % 2 == 0)
            return num >> 1;
        else
            return -((num + 1) >> 1);
    }

    public static long deZigzag(long num) {
        if (num % 2 == 0)
            return num >> 1;
        else
            return -((num + 1) >> 1);
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

    public static void pack8ValuesLong(ArrayList<Long> values, int offset, int width, int encode_pos,
            byte[] encoded_result) {
        int bufIdx = 0;
        int valueIdx = offset;
        int leftBit = 0;

        while (valueIdx < 8 + offset) {
            long buffer = 0;
            int leftSize = 64;

            if (leftBit > 0) {
                buffer |= (values.get(valueIdx) << (64 - leftBit));
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

    public static void unpack8ValuesLong(byte[] encoded, int offset, int width, ArrayList<Long> result_list) {
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
                result_list.add((buffer >>> (totalBits - width)));
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

    public static int bitPackingLong(ArrayList<Long> numbers, int start, int bit_width, int encode_pos,
            byte[] encoded_result) {
        int block_num = (numbers.size() - start) / 8;
        for (int i = 0; i < block_num; i++) {
            pack8ValuesLong(numbers, start + i * 8, bit_width, encode_pos, encoded_result);
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

    public static ArrayList<Long> decodeBitPackingLong(
            byte[] encoded, int decode_pos, int bit_width, int block_size) {
        ArrayList<Long> result_list = new ArrayList<>();
        int block_num = (block_size - 1) / 8;

        for (int i = 0; i < block_num; i++) {
            unpack8ValuesLong(encoded, decode_pos, bit_width, result_list);
            decode_pos += bit_width;
        }
        return result_list;
    }

    public static long[] getAbsDeltaTsBlock(
            long[] ts_block,
            int i,
            int block_size,
            int remaining,
            long[] min_delta) {
        long[] ts_block_delta = new long[remaining - 1];

        int base = i * block_size + 1;
        int end = i * block_size + remaining;
        min_delta[0] = ts_block[base - 1];
        long value_delta_min = Long.MAX_VALUE;
        long value_delta_max = Long.MIN_VALUE;
        for (int j = base; j < end; j++) {
            long epsilon_v = ts_block[j] - ts_block[j - 1];
            epsilon_v = zigzag(epsilon_v);
            if (epsilon_v < value_delta_min) {
                value_delta_min = epsilon_v;
            }
            if (epsilon_v > value_delta_max) {
                value_delta_max = epsilon_v;
            }
            ts_block_delta[j - base] = epsilon_v;

        }
        for (int j = 0; j < remaining - 1; j++) {
            ts_block_delta[j] = ts_block_delta[j] - value_delta_min;

        }
        min_delta[1] = (value_delta_min);
        min_delta[2] = (value_delta_max - value_delta_min);

        return ts_block_delta;
    }

    public static int encodeOutlier2Bytes(
            ArrayList<Long> ts_block_delta,
            int bit_width,
            int encode_pos, byte[] encoded_result) {

        encode_pos = bitPackingLong(ts_block_delta, 0, bit_width, encode_pos, encoded_result);

        int n_k = ts_block_delta.size();
        int n_k_b = n_k / 8;
        long cur_remaining = 0;
        int cur_number_bits = 0;
        for (int i = n_k_b * 8; i < n_k; i++) {
            long cur_value = ts_block_delta.get(i);
            int cur_bit_width = bit_width;

            if (cur_number_bits + bit_width >= 64) {
                cur_remaining <<= (64 - cur_number_bits);
                cur_bit_width = bit_width - 64 + cur_number_bits;
                cur_remaining += ((cur_value >> cur_bit_width));
                long2Bytes(cur_remaining, encode_pos, encoded_result);
                encode_pos += 8;

                cur_remaining = 0;
                cur_number_bits = 0;
            }

            cur_remaining <<= cur_bit_width;
            cur_number_bits += cur_bit_width;
            cur_remaining += (((cur_value << (64 - cur_bit_width)) & 0xFFFFFFFFFFFFFFFFL) >> (64 - cur_bit_width));
        }
        cur_remaining <<= (64 - cur_number_bits);
        long2Bytes(cur_remaining, encode_pos, encoded_result);
        encode_pos += 8;
        return encode_pos;

    }

    public static ArrayList<Long> decodeOutlier2Bytes(
            byte[] encoded,
            int decode_pos,
            int bit_width,
            int length,
            ArrayList<Integer> encoded_pos_result) {

        int n_k_b = length / 8;
        int remaining = length - n_k_b * 8;
        ArrayList<Long> result_list = new ArrayList<>(
                decodeBitPackingLong(encoded, decode_pos, bit_width, n_k_b * 8 + 1));
        decode_pos += n_k_b * bit_width;

        ArrayList<Long> int_remaining = new ArrayList<>();
        int int_remaining_size = remaining * bit_width / 32 + 1;
        for (int j = 0; j < int_remaining_size; j++) {
            int_remaining.add(bytes2Long(encoded, decode_pos, 8));
            decode_pos += 8;
        }

        int cur_remaining_bits = 64;
        long cur_number = int_remaining.get(0);
        int cur_number_i = 1;
        for (int i = n_k_b * 8; i < length; i++) {
            if (bit_width < cur_remaining_bits) {
                long tmp = (long) (cur_number >> (64 - bit_width));
                result_list.add(tmp);
                cur_number <<= bit_width;
                cur_number &= 0xFFFFFFFFFFFFFFFFL;
                cur_remaining_bits -= bit_width;
            } else {
                long tmp = (long) (cur_number >> (64 - cur_remaining_bits));
                int remain_bits = bit_width - cur_remaining_bits;
                tmp <<= remain_bits;

                cur_number = int_remaining.get(cur_number_i);
                cur_number_i++;
                tmp += (cur_number >> (64 - remain_bits));
                result_list.add(tmp);
                cur_number <<= remain_bits;
                cur_number &= 0xFFFFFFFFFFFFFFFFL;
                cur_remaining_bits = 64 - remain_bits;
            }
        }
        encoded_pos_result.add(decode_pos);
        return result_list;
    }

    private static int BOSBlockEncoder(long[] ts_block, int block_i, int block_size, int remaining, int encode_pos,
            byte[] cur_byte) {

        long[] min_delta = new long[3];
        long[] ts_block_delta = getAbsDeltaTsBlock(ts_block, block_i, block_size, remaining, min_delta);

        block_size = remaining - 1;
        long max_delta_value = min_delta[2];

        long2Bytes(min_delta[0], encode_pos, cur_byte);
        encode_pos += 8;
        long2Bytes(min_delta[1], encode_pos, cur_byte);
        encode_pos += 8;

        int bit_width_final = getBitWith(max_delta_value);
        intByte2Bytes(bit_width_final, encode_pos, cur_byte);
        encode_pos += 1;

        ArrayList<Long> final_normal = new ArrayList<>();
        for (long i : ts_block_delta) {
            final_normal.add(i);
        }
        encode_pos = encodeOutlier2Bytes(final_normal, bit_width_final, encode_pos, cur_byte);

        return encode_pos;
    }

    public static int BOSEncoder(
            long[] data, int block_size, byte[] encoded_result) {
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
                long2Bytes(data[data.length - i], encode_pos, encoded_result);
                encode_pos += 8;
            }

        } else {

            int start = block_num * block_size;
            int remaining = length_all - start;

            encode_pos = BOSBlockEncoder(data, block_num, block_size, remaining, encode_pos, encoded_result);
        }

        return encode_pos;
    }

    public static int BOSBlockDecoder(byte[] encoded, int decode_pos, long[] value_list, int block_size,
            int[] value_pos_arr) {

        ArrayList<Long> final_normal = new ArrayList<>();
        ;
        ArrayList<Integer> bitmap_outlier = new ArrayList<>();

        int bit_width_final = 0;
        long value0 = bytes2Long(encoded, decode_pos, 8);
        decode_pos += 8;

        value_list[value_pos_arr[0]] = value0;
        value_pos_arr[0]++;

        long min_delta = bytes2Long(encoded, decode_pos, 8);
        decode_pos += 8;

        bit_width_final = bytes2Integer(encoded, decode_pos, 1);
        decode_pos += 1;
        ArrayList<Integer> decode_pos_normal = new ArrayList<>();
        final_normal = decodeOutlier2Bytes(encoded, decode_pos, bit_width_final, block_size, decode_pos_normal);

        decode_pos = decode_pos_normal.get(0);

        int normal_i = 0;
        long pre_v = value0;

        for (int i = 0; i < block_size; i++) {
            long current_delta = final_normal.get(normal_i);
            pre_v = deZigzag(current_delta + min_delta) + pre_v;
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

        long[] value_list = new long[length_all + block_size];
        block_size--;

        int[] value_pos_arr = new int[1];
        for (int k = 0; k < block_num; k++) {
            decode_pos = BOSBlockDecoder(encoded, decode_pos, value_list, block_size, value_pos_arr);

        }

        if (remain_length <= 3) {
            for (int i = 0; i < remain_length; i++) {
                long value_end = bytes2Long(encoded, decode_pos, 8);
                decode_pos += 8;
                value_list[value_pos_arr[0]] = value_end;
                value_pos_arr[0]++;
            }
        } else {
            remain_length--;
            BOSBlockDecoder(encoded, decode_pos, value_list, remain_length, value_pos_arr);
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
        String parent_dir = "path/to/your/directory/";

        String input_parent_dir = parent_dir + "dataset/";

        String output_parent_dir = parent_dir + "result/";

        String outputPath = output_parent_dir + "sprintz_long0.csv";

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
                length = BOSEncoder(data2_arr, block_size, encoded_result);
            }

            long e = System.nanoTime();
            encodeTime += ((e - s) / repeatTime);
            compressed_size += length;

            double ratioTmp;

            ratioTmp = compressed_size / (double) (data1.size() * Long.BYTES);

            ratio += ratioTmp;

            s = System.nanoTime();

            for (int repeat = 0; repeat < repeatTime; repeat++) {
                BOSDecoder(encoded_result);
            }

            e = System.nanoTime();
            decodeTime += ((e - s) / repeatTime);

            String[] record = {
                    datasetName,
                    "SPRINTZ",
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
