package org.apache.iotdb.tsfile.encoding;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;

import org.junit.Test;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;

public class HBPIndexLongTest {

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

    public static int bitWidth(int value) {
        return 32 - Integer.numberOfLeadingZeros(value);
    }

    public static int bitWidth(long value) {
        return 64 - Long.numberOfLeadingZeros(value);
    }

    public static int BlockEncoder(long[] data, int block_index, int block_size, int remainder,
            int encode_pos, ArrayList<HBPIndexLong> indexList, byte[] encoded_result) {

        long[] block_data = new long[remainder];
        System.arraycopy(data, block_index * block_size, block_data, 0, remainder);

        long min_value = Long.MAX_VALUE;
        long max_value = Long.MIN_VALUE;
        for (long value : block_data) {
            if (value < min_value) {
                min_value = value;
            }
            if (value > max_value) {
                max_value = value;
            }
        }

        for (int i = 0; i < remainder; i++) {
            block_data[i] -= min_value;
        }

        long2Bytes(min_value, encode_pos, encoded_result);
        encode_pos += 8;

        int bw = bitWidth(max_value - min_value);

        int2Bytes(bw, encode_pos, encoded_result);
        encode_pos += 4;

        HBPIndexLong idx = new HBPIndexLong(bw, block_data);
        indexList.add(idx);

        return encode_pos;

    }

    public static int BlockDecoder(byte[] encoded_result, int block_index, int block_size, int remainder,
            int encode_pos, ArrayList<HBPIndexLong> indexList, long[] data) {

        long min_value = bytes2Long(encoded_result, encode_pos, 8);
        encode_pos += 8;

        int bw = bytes2Integer(encoded_result, encode_pos, 4);
        encode_pos += 4;

        HBPIndexLong idx = indexList.get(block_index);

        for (int i = 0; i < remainder; i++) {
            long value = idx.getCode(i);

            data[block_index * block_size + i] = value + min_value;
        }

        return encode_pos;

    }

    public static int Encoder(long[] data, int block_size, ArrayList<HBPIndexLong> indexList, byte[] encoded_result) {
        int data_length = data.length;
        int encode_pos = 0;

        int2Bytes(data_length, encode_pos, encoded_result);
        encode_pos += 4;

        int2Bytes(block_size, encode_pos, encoded_result);
        encode_pos += 4;

        int num_blocks = data_length / block_size;

        int remainder = data_length % block_size;

        for (int i = 0; i < num_blocks; i++) {
            encode_pos = BlockEncoder(data, i, block_size, block_size, encode_pos, indexList, encoded_result);
        }

        encode_pos = BlockEncoder(data, num_blocks, block_size, remainder, encode_pos, indexList,
                encoded_result);

        return encode_pos;
    }

    public static long[] Decoder(byte[] encoded_result, ArrayList<HBPIndexLong> indexList) {
        int encode_pos = 0;

        int data_length = bytes2Integer(encoded_result, encode_pos, 4);
        encode_pos += 4;

        int block_size = bytes2Integer(encoded_result, encode_pos, 4);
        encode_pos += 4;

        int num_blocks = data_length / block_size;

        long[] data = new long[data_length];

        for (int i = 0; i < num_blocks; i++) {
            encode_pos = BlockDecoder(encoded_result, i, block_size, block_size, encode_pos, indexList, data);
        }

        int remainder = data_length % block_size;

        encode_pos = BlockDecoder(encoded_result, num_blocks, block_size, remainder,
                encode_pos, indexList, data);

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
        String parent_dir = "D://github/xjz17/subcolumn/";

        String input_parent_dir = parent_dir + "dataset/";

        String output_parent_dir = parent_dir + "result/";

        String outputPath = output_parent_dir + "hbp.csv";

        int block_size = 512;

        int repeatTime = 100;

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

            ArrayList<HBPIndexLong> indexList = new ArrayList<>();

            long s = System.nanoTime();
            for (int repeat = 0; repeat < repeatTime; repeat++) {
                indexList.clear();

                length = Encoder(data2_arr, block_size, indexList, encoded_result);
            }

            long e = System.nanoTime();
            encodeTime += ((e - s) / repeatTime);
            compressed_size += length;

            for (HBPIndexLong idx : indexList) {
                compressed_size += idx.segments * (idx.k + 1) * Long.BYTES;
            }

            double ratioTmp;

            ratioTmp = compressed_size / (double) (data1.size() * Long.BYTES);

            ratio += ratioTmp;

            System.out.println("Decode");

            long[] data2_arr_decoded = new long[data2_arr.length];

            s = System.nanoTime();

            for (int repeat = 0; repeat < repeatTime; repeat++) {
                data2_arr_decoded = Decoder(encoded_result, indexList);
            }

            e = System.nanoTime();
            decodeTime += ((e - s) / repeatTime);

            String[] record = {
                    datasetName,
                    "HBP",
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
