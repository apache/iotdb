package org.apache.iotdb.tsfile.encoding;

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

import org.junit.Test;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;

public class BPLongTest {

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

    public static void pack8Values(
            long[] values, int offset, int width, int encode_pos, byte[] encoded_result) {
        int bufIdx = 0;
        int valueIdx = offset;
        int leftBit = 0;

        while (valueIdx < 8 + offset) {
            long buffer = 0;
            int leftSize = 64;

            if (leftBit > 0) {
                buffer |= (values[valueIdx] << (64 - leftBit));
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

            for (int j = 0; j < 8; j++) {
                encoded_result[encode_pos] = (byte) ((buffer >>> ((8 - j - 1) * 8)) & 0xFF);
                encode_pos++;
                bufIdx++;
                if (bufIdx >= width * 8 / 8) {
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

    public static void unpack8Values(
            byte[] encoded, int offset, int width, long[] result_list, int result_offset) {
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

    public static int BPEncoder(long[] list, int encode_pos, byte[] encoded_result) {
        int list_length = list.length;
        long maxValue = 0;
        for (int i = 0; i < list_length; i++) {
            if (list[i] > maxValue) {
                maxValue = list[i];
            }
        }

        int m = bitWidth(maxValue);

        encoded_result[encode_pos] = (byte) m;
        encode_pos += 1;

        encode_pos = bitPacking(list, m, encode_pos, encoded_result, list_length);

        return encode_pos;
    }

    public static int BPDecoder(byte[] encoded_result, int encode_pos, long[] list) {
        int list_length = list.length;

        int m = encoded_result[encode_pos];
        encode_pos += 1;

        long[] new_list = new long[list_length];
        encode_pos = decodeBitPacking(encoded_result, encode_pos, m, list_length, new_list);

        for (int i = 0; i < list_length; i++) {
            list[i] = new_list[i];
        }

        return encode_pos;
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

        return ts_block_delta;
    }

    public static int BlockEncoder(long[] data, int block_index, int block_size, int remainder,
            int encode_pos, byte[] encoded_result) {
        long[] min_delta = new long[3];

        long[] data_delta = getAbsDeltaTsBlock(data, block_index, block_size,
                remainder, min_delta);

        long2Bytes(min_delta[0], encode_pos, encoded_result);
        encode_pos += 8;

        encode_pos = BPEncoder(data_delta, encode_pos,
                encoded_result);

        return encode_pos;
    }

    public static int BlockDecoder(byte[] encoded_result, int block_index, int block_size, int remainder,
            int encode_pos, long[] data) {
        long[] min_delta = new long[3];

        min_delta[0] = bytes2Long(encoded_result, encode_pos, 8);
        encode_pos += 8;

        long[] block_data = new long[remainder];

        encode_pos = BPDecoder(encoded_result, encode_pos,
                block_data);

        for (int i = 0; i < remainder; i++) {
            data[block_index * block_size + i] = block_data[i] + min_delta[0];
        }

        return encode_pos;
    }

    public static int Encoder(long[] data, int block_size, byte[] encoded_result) {
        int data_length = data.length;
        int encode_pos = 0;

        encoded_result[0] = (byte) (data_length >> 24);
        encoded_result[1] = (byte) (data_length >> 16);
        encoded_result[2] = (byte) (data_length >> 8);
        encoded_result[3] = (byte) data_length;
        encode_pos += 4;

        encoded_result[4] = (byte) (block_size >> 24);
        encoded_result[5] = (byte) (block_size >> 16);
        encoded_result[6] = (byte) (block_size >> 8);
        encoded_result[7] = (byte) block_size;
        encode_pos += 4;

        int num_blocks = data_length / block_size;

        for (int i = 0; i < num_blocks; i++) {
            encode_pos = BlockEncoder(data, i, block_size, block_size, encode_pos, encoded_result);
        }

        int remainder = data_length % block_size;

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
        // String parent_dir = "path/to/your/directory/";
        String parent_dir = "D:/encoding-subcolumn/";

        // String input_parent_dir = parent_dir + "dataset/";
        String input_parent_dir = parent_dir + "ElfTestData_camel/";

        String output_parent_dir = parent_dir + "result/";

        // String outputPath = output_parent_dir + "bp_long.csv";
        String outputPath = output_parent_dir + "bp_long2.csv";

        int block_size = 1024;

        int repeatTime = 50;

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
                length = Encoder(data2_arr, block_size, encoded_result);
            }

            long e = System.nanoTime();
            encodeTime += ((e - s) / repeatTime);
            compressed_size += length;

            double ratioTmp;

            ratioTmp = compressed_size / (double) (data1.size() * Long.BYTES);

            ratio += ratioTmp;

            System.out.println("Decode");

            long[] data2_arr_decoded = new long[data1.size()];

            s = System.nanoTime();

            for (int repeat = 0; repeat < repeatTime; repeat++) {
                data2_arr_decoded = Decoder(encoded_result);
            }

            e = System.nanoTime();
            decodeTime += ((e - s) / repeatTime);

            String[] record = {
                    datasetName,
                    "BP",
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
