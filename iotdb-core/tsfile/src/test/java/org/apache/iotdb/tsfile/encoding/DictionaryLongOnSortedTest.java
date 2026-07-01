package org.apache.iotdb.tsfile.encoding;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.*;

import static org.junit.Assert.assertEquals;

public class DictionaryLongOnSortedTest {

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

    public static int[] getAbsDeltaTsBlock(
            long[] ts_block,
            int i,
            int block_size,
            int remaining,
            long[] min_delta,
            Map<Long, Integer> valueToIndex,
            List<Long> dictionary) {
        int[] encodedIndices = new int[remaining];

        int dictIndex = 0;

        for (int j = 0; j < remaining; j++) {
            long value = ts_block[i * block_size + j];

            if (!valueToIndex.containsKey(value)) {
                valueToIndex.put(value, dictIndex);
                dictionary.add(value);
                encodedIndices[j] = dictIndex;
                dictIndex++;
            } else {
                encodedIndices[j] = valueToIndex.get(value);
            }
        }

        return encodedIndices;
    }

    public static int BlockEncoder(long[] data, int block_index, int block_size, int remainder,
            int encode_pos, byte[] encoded_result) {
        Map<Long, Integer> valueToIndex = new LinkedHashMap<>();
        List<Long> dictionary = new ArrayList<>();

        long[] min_delta = new long[3];

        int[] encodedIndices = getAbsDeltaTsBlock(data, block_index, block_size,
                remainder, min_delta, valueToIndex, dictionary);

        int dictSize = dictionary.size();
        int2Bytes(dictSize, encode_pos, encoded_result);
        encode_pos += 4;

        long[] dictArray = new long[dictSize];

        long min_dict_value = Long.MAX_VALUE;
        long max_dict_value = Long.MIN_VALUE;

        for (int i = 0; i < dictSize; i++) {
            dictArray[i] = dictionary.get(i);
            if (dictArray[i] < min_dict_value) {
                min_dict_value = dictArray[i];
            }
            if (dictArray[i] > max_dict_value) {
                max_dict_value = dictArray[i];
            }
        }

        for (int i = 0; i < dictSize; i++) {
            dictArray[i] = dictArray[i] - min_dict_value;
        }

        long2Bytes(min_dict_value, encode_pos, encoded_result);
        encode_pos += 8;

        int bw = bitWidth(max_dict_value - min_dict_value);
        int2Bytes(bw, encode_pos, encoded_result);
        encode_pos += 4;

        encode_pos = bitPacking(dictArray, bw, encode_pos, encoded_result, dictSize);

        int maxIndex = dictSize - 1;
        int indexBitWidth = maxIndex > 0 ? bitWidth(maxIndex) : 1;

        encode_pos = bitPacking(encodedIndices, indexBitWidth, encode_pos, encoded_result, remainder);

        // encode_pos = SubcolumnEncoder(data_delta, encode_pos,
        // encoded_result, block_size);

        return encode_pos;
    }

    public static int BlockDecoder(byte[] encoded_result, int block_index, int block_size, int remainder,
            int encode_pos, long[] data) {
        long[] min_delta = new long[3];

        int dictSize = bytes2Integer(encoded_result, encode_pos, 4);
        encode_pos += 4;

        long min_dict_value = bytes2Long(encoded_result, encode_pos, 8);
        encode_pos += 8;

        int dictBitWidth = bytes2Integer(encoded_result, encode_pos, 4);
        encode_pos += 4;

        long[] dictionary = new long[dictSize];
        encode_pos = decodeBitPacking(encoded_result, encode_pos, dictBitWidth, dictSize, dictionary);

        for (int i = 0; i < dictSize; i++) {
            dictionary[i] = dictionary[i] + min_dict_value;
        }

        int maxIndex = dictSize - 1;
        int indexBitWidth = maxIndex > 0 ? bitWidth(maxIndex) : 1;

        int[] encodedIndices = new int[remainder];
        encode_pos = decodeBitPacking(encoded_result, encode_pos, indexBitWidth, remainder, encodedIndices);

        for (int i = 0; i < remainder; i++) {
            data[block_index * block_size + i] = dictionary[encodedIndices[i]];
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
//        String parent_dir = "D:/github/xjz17/subcolumn/";
//        // String parent_dir = "D:/encoding-subcolumn/";
//
//        String input_parent_dir = parent_dir + "dataset/";
//
//        // String output_parent_dir = parent_dir + "result/";
//        String output_parent_dir = "D:/encoding-subcolumn/result/";
//        // String output_parent_dir = parent_dir + "result/";
//
//        String outputPath = output_parent_dir + "dictionary_long.csv";

        String parent_dir = "/Users/xiaojinzhao/Documents/GitHub/subcolumn/"; //"D:/github/xjz17/subcolumn/";
        // String parent_dir = "D:/encoding-subcolumn/";

        String input_parent_dir = parent_dir + "dataset/";

        String output_parent_dir = parent_dir + "result/"; //""D:/encoding-subcolumn/result/";
        // String output_parent_dir = parent_dir + "result/";

        String outputPath = output_parent_dir + "dictionary_long_on_sorted.csv";


        int block_size = 512;

        // int repeatTime = 100;
        int repeatTime = 500;

        // repeatTime = 1;

        CsvWriter writer = new CsvWriter(outputPath, ',', StandardCharsets.UTF_8);
        writer.setRecordDelimiter('\n');

        String[] head = {
                "Dataset",
                "Encoding Algorithm",
                "Points",
                "Encoding Time",
                "Decoding Time",
                "Compressed Size",
                "Compression Ratio",
                "Encoding Time Sort",
                "Decoding Time Sort",
                "Compressed Size Sort",
                "Compression Ratio Sort"
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
            long[] data1_arr = new long[data1.size()];
            long[] data2_arr = new long[data1.size()];

            long max_mul = (long) Math.pow(10, max_decimal);
            for (int i = 0; i < data1.size(); i++) {
                data2_arr[i] = (long) (data1.get(i) * max_mul);
                data1_arr[i] = i;
            }

            // test
            // for (int i = 0; i < data2_arr.length; i++) {
            // System.out.print(data2_arr[i] + " ");
            // }
            // System.out.println();

            System.out.println(max_decimal);
            byte[] encoded_result = new byte[data2_arr.length * 17];
            byte[] encoded_result1 = new byte[data2_arr.length * 17];


            long encodeTime = 0;
            long decodeTime = 0;
            double ratio = 0;
            double compressed_size = 0;

            int length = 0;
            int length1 = 0;

            long s = System.nanoTime();
            for (int repeat = 0; repeat < repeatTime; repeat++) {
                length1 = Encoder(data1_arr, block_size, encoded_result1);
                length = Encoder(data2_arr, block_size, encoded_result);
            }

            long e = System.nanoTime();
            encodeTime += ((e - s) / repeatTime);
            compressed_size += length;

            double ratioTmp;

            ratioTmp = compressed_size / (double) (data1.size() * Long.BYTES);

            ratio += ratioTmp;

            System.out.println("Decode");

            long[] data1_arr_decoded = new long[data2_arr.length];
            long[] data2_arr_decoded = new long[data2_arr.length];

            s = System.nanoTime();

            for (int repeat = 0; repeat < repeatTime; repeat++) {
                data1_arr_decoded = Decoder(encoded_result1);
                data2_arr_decoded = Decoder(encoded_result);
            }

            e = System.nanoTime();
            decodeTime += ((e - s) / repeatTime);

            for (int i = 0; i < data2_arr_decoded.length; i++) {
                // assertEquals(data2_arr[i], data2_arr_decoded[i]);
            }

            Integer[] indices = new Integer[data1_arr.length];
            for (int i = 0; i < indices.length; i++) {
                indices[i] = i;
            }
            long[] finalData2_arr = data2_arr;
            Arrays.sort(indices, (i, j) -> Long.compare(finalData2_arr[i], finalData2_arr[j]));
            long[] sortedData1 = new long[data1_arr.length];
            long[] sortedData2 = new long[data2_arr.length];
            for (int i = 0; i < indices.length; i++) {
                sortedData1[i] = data1_arr[indices[i]];
                sortedData2[i] = data2_arr[indices[i]];
            }

            System.out.println(max_decimal);
            encoded_result = new byte[data2_arr.length * 8];
            encoded_result1 = new byte[data2_arr.length * 8];

            long encodeTime_sort = 0;
            long decodeTime_sort = 0;
            double ratio_sort = 0;
            double compressed_size_sort = 0;

            int length_sort = 0;
            int length1_sort = 0;

            long s_sort = System.nanoTime();
            for (int repeat = 0; repeat < repeatTime; repeat++) {
                length1 = Encoder(sortedData1, block_size, encoded_result1);
                length = Encoder(sortedData2, block_size, encoded_result);
            }

            long e_sort = System.nanoTime();
            encodeTime_sort += ((e_sort - s_sort) / repeatTime);
            length += length1;
            compressed_size_sort += length;

            double ratioTmp_sort;

            ratioTmp_sort = compressed_size_sort / (double) (data1.size() * Long.BYTES*2);

            ratio_sort += ratioTmp_sort;

            System.out.println("Decode");

            long[] data1_arr_decoded_sort = new long[data2_arr.length];
            long[] data2_arr_decoded_sort = new long[data2_arr.length];

            s_sort = System.nanoTime();

            for (int repeat = 0; repeat < repeatTime; repeat++) {
                data1_arr_decoded_sort = Decoder(encoded_result1);
                data2_arr_decoded_sort = Decoder(encoded_result);
            }

            e_sort = System.nanoTime();
            decodeTime_sort += ((e_sort - s_sort) / repeatTime);

            for (int i = 0; i < data2_arr_decoded_sort.length; i++) {
                assertEquals(sortedData2[i], data2_arr_decoded_sort[i]);
            }
            String[] record = {
                    datasetName,
                    "Dictionary",
                    String.valueOf(data1.size()),
                    String.valueOf(encodeTime),
                    String.valueOf(decodeTime),
                    String.valueOf(compressed_size),
                    String.valueOf(ratio),
                    String.valueOf(encodeTime_sort),
                    String.valueOf(decodeTime_sort),
                    String.valueOf(compressed_size_sort),
                    String.valueOf(ratio_sort),
            };

//            String[] record = {
//                    datasetName,
//                    "Sub-columns",
//                    String.valueOf(encodeTime),
//                    String.valueOf(decodeTime),
//                    String.valueOf(data1.size()),
//                    String.valueOf(compressed_size),
//                    String.valueOf(ratio)
//            };
            writer.writeRecord(record);
            System.out.println(ratio);
        }

        writer.close();
    }

}
