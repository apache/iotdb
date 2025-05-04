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

public class BUFFTest {

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

    public static int[] bits_needed = { 0, 5, 8, 11, 15, 18, 21, 25 };

    public static int BlockEncoder(float[] data, int block_index, int block_size, int remainder, int max_decimal,
            int encode_pos, byte[] encoded_result) {

        int[] sign_bits = new int[remainder];
        int[] integer_parts = new int[remainder];
        int[] decimal_parts = new int[remainder];

        int min_integer_part = Integer.MAX_VALUE;
        int max_integer_part = Integer.MIN_VALUE;

        // for (int i = 0; i < remainder; i++) {
        // System.out.print(data[block_index * block_size + i] + " ");
        // }
        // System.out.println();

        for (int i = 0; i < remainder; i++) {
            float value = data[block_index * block_size + i];

            if (value < 0) {
                sign_bits[i] = 1;
            }

            int currentInt = (int) Math.abs(value);
            integer_parts[i] = currentInt;

            if (currentInt < min_integer_part) {
                min_integer_part = currentInt;
            }

            if (currentInt > max_integer_part) {
                max_integer_part = currentInt;
            }

            int bits = Float.floatToIntBits(value);

            // int sign = (bits >> 31) & 1;
            int exponent = (bits >> 23) & 0xFF;
            int mantissa = bits & 0x7FFFFF;

            int actualExponent = exponent - 127;

            if (actualExponent >= 0) {
                int mask = (1 << (23 - actualExponent)) - 1;
                mantissa &= mask;
            } else {
                mantissa += 1 << 23;
            }

            int shift = 23 - actualExponent - bits_needed[max_decimal];

            if (shift < 0) {
                mantissa <<= -shift;
            } else {
                mantissa >>= shift;
            }

            if (exponent == 0) {
                mantissa = 0;
            }

            decimal_parts[i] = mantissa;
        }

        encoded_result[encode_pos] = (byte) (min_integer_part >> 24);
        encoded_result[encode_pos + 1] = (byte) (min_integer_part >> 16);
        encoded_result[encode_pos + 2] = (byte) (min_integer_part >> 8);
        encoded_result[encode_pos + 3] = (byte) min_integer_part;
        encode_pos += 4;

        // System.out.println("min_integer_part: " + min_integer_part);
        // System.out.println("max_integer_part: " + max_integer_part);

        int bw = bitWidth(max_integer_part - min_integer_part);

        encoded_result[encode_pos] = (byte) bw;
        encode_pos += 1;

        for (int i = 0; i < remainder; i++) {
            integer_parts[i] -= min_integer_part;
        }

        // int[] combined = new int[remainder];
        // for (int i = 0; i < remainder; i++) {
        // combined[i] = (sign_bits[i] << (bw + bits_needed[max_decimal])) |
        // (integer_parts[i] << bits_needed[max_decimal]) | decimal_parts[i];
        // }

        // int totalBitWidth = 1 + bw + bits_needed[max_decimal];

        // encode_pos = bitPacking(combined, totalBitWidth, encode_pos, encoded_result,
        // remainder);

        int totalBitWidth = 1 + bw + bits_needed[max_decimal];

        int intArrayCount = (totalBitWidth + 7) / 8;

        int[][] combinedArrays = new int[intArrayCount][remainder];

        for (int i = 0; i < intArrayCount; i++) {
            for (int j = 0; j < remainder; j++) {
                long combined = (sign_bits[j] << (bw + bits_needed[max_decimal]))
                        | (integer_parts[j] << bits_needed[max_decimal]) | decimal_parts[j];
                combinedArrays[i][j] = (int) ((combined >> (i * 8)) & 0xFF);
            }
        }

        for (int i = 0; i < intArrayCount; i++) {
            int currentBitWidth = Math.min(8, totalBitWidth - i * 8);
            encode_pos = bitPacking(combinedArrays[i], currentBitWidth, encode_pos, encoded_result, remainder);
        }

        return encode_pos;
    }

    public static int BlockDecoder(byte[] encoded_result, int block_index, int block_size, int remainder,
            int max_decimal, int encode_pos, float[] data) {

        int[] sign_bits = new int[remainder];
        int[] integer_parts = new int[remainder];
        int[] decimal_parts = new int[remainder];

        int min_integer_part = ((encoded_result[encode_pos] & 0xFF) << 24)
                | ((encoded_result[encode_pos + 1] & 0xFF) << 16)
                |
                ((encoded_result[encode_pos + 2] & 0xFF) << 8) | (encoded_result[encode_pos + 3] & 0xFF);
        encode_pos += 4;

        // System.out.println("min_integer_part: " + min_integer_part);

        int bw = encoded_result[encode_pos];
        encode_pos += 1;

        // int[] combined = new int[remainder];

        // encode_pos = decodeBitPacking(encoded_result, encode_pos, 1 + bw +
        // bits_needed[max_decimal], remainder, combined);

        // for (int i = 0; i < remainder; i++) {
        // int value = combined[i];
        // sign_bits[i] = (value >> (bw + bits_needed[max_decimal])) & 1;
        // integer_parts[i] = (value >> bits_needed[max_decimal]) & ((1 << bw) - 1);
        // integer_parts[i] += min_integer_part;
        // decimal_parts[i] = value & ((1 << bits_needed[max_decimal]) - 1);
        // }

        int totalBitWidth = 1 + bw + bits_needed[max_decimal];

        int intArrayCount = (totalBitWidth + 7) / 8;

        int[][] combinedArrays = new int[intArrayCount][remainder];

        long[] combined = new long[remainder];

        for (int i = 0; i < intArrayCount; i++) {
            int currentBitWidth = Math.min(8, totalBitWidth - i * 8);
            encode_pos = decodeBitPacking(encoded_result, encode_pos, currentBitWidth, remainder, combinedArrays[i]);
            for (int j = 0; j < remainder; j++) {
                combined[j] |= ((long) combinedArrays[i][j]) << (i * 8);
            }
        }

        for (int i = 0; i < remainder; i++) {
            sign_bits[i] = (int) ((combined[i] >> (bw + bits_needed[max_decimal])) & 1);
            integer_parts[i] = (int) ((combined[i] >> bits_needed[max_decimal]) & ((1 << bw) - 1));
            integer_parts[i] += min_integer_part;
            decimal_parts[i] = (int) (combined[i] & ((1 << bits_needed[max_decimal]) - 1));
        }

        for (int i = 0; i < remainder; i++) {
            float decimal = decimal_parts[i];
            for (int j = 0; j < bits_needed[max_decimal]; j++) {
                decimal /= 2;
            }
            float value = (float) (integer_parts[i] + decimal);
            value = sign_bits[i] == 1 ? -value : value;
            data[block_index * block_size + i] = value;
        }

        // for (int i = 0; i < remainder; i++) {
        // System.out.print(data[block_index * block_size + i] + " ");
        // }
        // System.out.println();

        return encode_pos;
    }

    public static int Encoder(float[] data, int block_size, int max_decimal, byte[] encoded_result) {
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

        encoded_result[8] = (byte) max_decimal;
        encode_pos += 1;

        int num_blocks = data_length / block_size;

        int remainder = data_length % block_size;

        for (int i = 0; i < num_blocks; i++) {
            encode_pos = BlockEncoder(data, i, block_size, block_size, max_decimal, encode_pos, encoded_result);
        }

        if (remainder > 0) {
            encode_pos = BlockEncoder(data, num_blocks, block_size, remainder, max_decimal, encode_pos, encoded_result);
        }

        // if (remainder <= 3) {
        // for (int i = 0; i < remainder; i++) {
        // int value = data[num_blocks * block_size + i];
        // encoded_result[encode_pos] = (byte) (value >> 24);
        // encoded_result[encode_pos + 1] = (byte) (value >> 16);
        // encoded_result[encode_pos + 2] = (byte) (value >> 8);
        // encoded_result[encode_pos + 3] = (byte) value;
        // encode_pos += 4;
        // }
        // } else {
        // encode_pos = BlockEncoder(data, num_blocks, block_size, remainder,
        // max_decimal, encode_pos,
        // encoded_result);
        // }

        return encode_pos;
    }

    public static float[] Decoder(byte[] encoded_result) {
        int encode_pos = 0;

        int data_length = ((encoded_result[encode_pos] & 0xFF) << 24) | ((encoded_result[encode_pos + 1] & 0xFF) << 16)
                |
                ((encoded_result[encode_pos + 2] & 0xFF) << 8) | (encoded_result[encode_pos + 3] & 0xFF);
        encode_pos += 4;

        int block_size = ((encoded_result[encode_pos] & 0xFF) << 24) | ((encoded_result[encode_pos + 1] & 0xFF) << 16) |
                ((encoded_result[encode_pos + 2] & 0xFF) << 8) | (encoded_result[encode_pos + 3] & 0xFF);
        encode_pos += 4;

        int max_decimal = encoded_result[encode_pos];
        encode_pos += 1;

        int num_blocks = data_length / block_size;

        int remainder = data_length % block_size;

        float[] data = new float[data_length];

        for (int i = 0; i < num_blocks; i++) {
            encode_pos = BlockDecoder(encoded_result, i, block_size, block_size, max_decimal, encode_pos, data);
        }

        if (remainder > 0) {
            encode_pos = BlockDecoder(encoded_result, num_blocks, block_size, remainder, max_decimal, encode_pos, data);
        }

        // if (remainder <= 3) {
        // for (int i = 0; i < remainder; i++) {
        // data[num_blocks * block_size + i] = ((encoded_result[encode_pos] & 0xFF) <<
        // 24) |
        // ((encoded_result[encode_pos + 1] & 0xFF) << 16) |
        // ((encoded_result[encode_pos + 2] & 0xFF) << 8) | (encoded_result[encode_pos +
        // 3] & 0xFF);
        // encode_pos += 4;
        // }
        // } else {
        // encode_pos = BlockDecoder(encoded_result, num_blocks, block_size, remainder,
        // encode_pos, data);
        // }

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
    public void testSubcolumn() throws IOException {
        String parent_dir = "D:/github/xjz17/subcolumn/";

        String input_parent_dir = parent_dir + "dataset/";

        String output_parent_dir = "D:/encoding-subcolumn/result/";
        // String output_parent_dir = parent_dir + "result/";

        String outputPath = output_parent_dir + "buff.csv";

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
        writer.writeRecord(head);

        File directory = new File(input_parent_dir);
        // File[] csvFiles = directory.listFiles();
        File[] csvFiles = directory.listFiles((dir, name) -> name.endsWith(".csv"));

        for (File file : csvFiles) {
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
                if (cur_decimal > max_decimal)
                    max_decimal = cur_decimal;
                data1.add(Float.valueOf(f_str));
            }
            inputStream.close();
            // int[] data2_arr = new int[data1.size()];
            float[] data2_arr = new float[data1.size()];

            // int max_mul = (int) Math.pow(10, max_decimal);
            for (int i = 0; i < data1.size(); i++) {
                // data2_arr[i] = (int) (data1.get(i) * max_mul);
                data2_arr[i] = data1.get(i);
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
                length = Encoder(data2_arr, block_size, max_decimal, encoded_result);
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

            System.out.println("Decode");

            float[] data2_arr_decoded = new float[data1.size()];

            s = System.nanoTime();

            for (int repeat = 0; repeat < repeatTime; repeat++) {
                data2_arr_decoded = Decoder(encoded_result);
            }

            e = System.nanoTime();
            decodeTime += ((e - s) / repeatTime);

            String[] record = {
                    datasetName,
                    "BUFF",
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

        String outputPath = output_parent_dir + "buff.csv";
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

                int max_decimal = 0;
                loader.readHeaders();
                while (loader.readRecord()) {
                    // String value = loader.getValues()[index];
                    data1.add(Integer.valueOf(loader.getValues()[0]));
                    data2.add(Integer.valueOf(loader.getValues()[1]));
                    int cur_decimal = getDecimalPrecision(loader.getValues()[1]);
                    max_decimal = Math.max(max_decimal, cur_decimal);
                    // data.add(Integer.valueOf(value));
                }
                inputStream.close();
                float[] data2_arr = new float[data1.size()];
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
                    length = Encoder(data2_arr, dataset_block_size.get(file_i), max_decimal, encoded_result);
                }

                long e = System.nanoTime();
                encodeTime += ((e - s) / repeatTime);
                compressed_size += length;
                double ratioTmp = compressed_size / (double) (data1.size() * Integer.BYTES);
                ratio += ratioTmp;
                s = System.nanoTime();

                float[] data2_arr_decoded = new float[data1.size()];

                for (int repeat = 0; repeat < repeatTime; repeat++) {
                    data2_arr_decoded = Decoder(encoded_result);
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
                    "BUFF",
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
