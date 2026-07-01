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


public class SPRINTZSubcolumnLongTest {

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

    public static long bytes2Long(byte[] encoded, int start, int num) {
        long value = 0;

        for (int i = 0; i < num; i++) {
            value <<= 8;
            int b = encoded[i + start] & 0xFF;
            value |= b;
        }
        return value;
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

        int remainder = data_length % block_size;

        int[] beta = new int[1];
        beta[0] = 2;

        for (int i = 0; i < num_blocks; i++) {
            encode_pos = BlockEncoder(data, i, block_size, block_size, encode_pos, encoded_result, beta);
        }

        if (remainder <= 3) {
            for (int i = 0; i < remainder; i++) {
                long value = data[num_blocks * block_size + i];
                long2Bytes(value, encode_pos, encoded_result);
                encode_pos += 8;
            }
        } else {
            encode_pos = BlockEncoder(data, num_blocks, block_size, remainder, encode_pos,
                    encoded_result, beta);
        }

        return encode_pos;
    }

    public static long[] Decoder(byte[] encoded_result) {
        int encode_pos = 0;

        int data_length = ((encoded_result[encode_pos] & 0xFF) << 24) | ((encoded_result[encode_pos + 1] & 0xFF) << 16) |
                ((encoded_result[encode_pos + 2] & 0xFF) << 8) | (encoded_result[encode_pos + 3] & 0xFF);
        encode_pos += 4;

        int block_size = ((encoded_result[encode_pos] & 0xFF) << 24) | ((encoded_result[encode_pos + 1] & 0xFF) << 16) |
                ((encoded_result[encode_pos + 2] & 0xFF) << 8) | (encoded_result[encode_pos + 3] & 0xFF);
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

    public static int zigzag(int num) {
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

    public static long zigzag(long num) {
        if (num < 0)
            return ((-num) << 1) - 1;
        else
            return num << 1;
    }

    public static long deZigzag(long num) {
        if (num % 2 == 0)
            return num >> 1;
        else
            return -((num + 1) >> 1);
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

    public static int BlockEncoder(long[] data, int block_index, int block_size, int remainder,
            int encode_pos, byte[] encoded_result, int[] beta) {
        long[] min_delta = new long[3];

        long[] data_delta = getAbsDeltaTsBlock(data, block_index, block_size, remainder, min_delta);

        long2Bytes(min_delta[0], encode_pos, encoded_result);
        encode_pos += 8;

        long2Bytes(min_delta[1], encode_pos, encoded_result);
        encode_pos += 8;

        if (block_index == 0) {
            long maxValue = 0;
            for (int j = 0; j < remainder - 1; j++) {
                if (data_delta[j] > maxValue) {
                    maxValue = data_delta[j];
                }
            }
            int m = SubcolumnLongTest.bitWidth(maxValue);

            beta[0] = SubcolumnLongTest.Subcolumn(data_delta, remainder - 1, m, block_size);
        }

        encode_pos = SubcolumnLongTest.SubcolumnEncoder(data_delta, encode_pos, encoded_result, beta, block_size);

        return encode_pos;
    }

    public static int BlockDecoder(byte[] encoded_result, int block_index, int block_size, int remainder,
            int encode_pos, long[] data) {
        long[] min_delta = new long[3];

        min_delta[0] = bytes2Long(encoded_result, encode_pos, 8);
        encode_pos += 8;

        min_delta[1] = bytes2Long(encoded_result, encode_pos, 8);
        encode_pos += 8;

        long[] data_delta = new long[remainder - 1];

        encode_pos = SubcolumnLongTest.SubcolumnDecoder(encoded_result, encode_pos, data_delta, block_size);

        for (int i = 0; i < remainder - 1; i++) {
            data_delta[i] = data_delta[i] + min_delta[1];
        }

        for (int i = 0; i < remainder - 1; i++) {
            data_delta[i] = deZigzag(data_delta[i]);
        }

        data[block_index * block_size] = min_delta[0];

        for (int i = 0; i < remainder - 1; i++) {
            data[block_index * block_size + i + 1] = data[block_index * block_size + i] + data_delta[i];
        }

        return encode_pos;
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

        String outputPath = output_parent_dir + "sprintz_subcolumn_long.csv";

        int block_size = 512;

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
                length = Encoder(data2_arr, block_size, encoded_result);
            }

            long e = System.nanoTime();
            encodeTime += ((e - s) / repeatTime);

            compressed_size += length;
            
            double ratioTmp;

            ratioTmp = compressed_size / (double) (data1.size() * Long.BYTES);

            ratio += ratioTmp;

            long[] data2_arr_decoded = new long[data2_arr.length];

            s = System.nanoTime();

            for (int repeat = 0; repeat < repeatTime; repeat++) {
                data2_arr_decoded = Decoder(encoded_result);
            }

            e = System.nanoTime();
            decodeTime += ((e - s) / repeatTime);

            String[] record = {
                    datasetName,
                    "SPRINTZ+Sub-columns",
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
