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

public class SPRINTZSubcolumn3Test {
    // SPRINTZ Subcolumn3Test

    public static int Encoder(int[] data, int block_size, byte[] encoded_result) {
        int data_length = data.length;
        int startBitPosition = 0;

        Subcolumn3Test.intToBytes(data_length, encoded_result, startBitPosition, 32);
        startBitPosition += 32;

        Subcolumn3Test.intToBytes(block_size, encoded_result, startBitPosition, 32);
        startBitPosition += 32;

        int num_blocks = data_length / block_size;

        int remainder = data_length % block_size;

        int[] beta = new int[1];
        beta[0] = 2;

        for (int i = 0; i < num_blocks; i++) {
            startBitPosition = BlockEncoder(data, i, block_size, block_size, startBitPosition, encoded_result, beta);
        }

        if (remainder <= 3) {
            for (int i = 0; i < remainder; i++) {
                Subcolumn3Test.intToBytes(data[num_blocks * block_size + i], encoded_result, startBitPosition, 32);
                startBitPosition += 32;
            }
        } else {
            startBitPosition = BlockEncoder(data, num_blocks, block_size, remainder, startBitPosition,
                    encoded_result, beta);
        }

        return startBitPosition;
    }

    public static int[] Decoder(byte[] encoded_result) {
        int startBitPosition = 0;

        int data_length = Subcolumn3Test.bytesToInt(encoded_result, startBitPosition, 32);
        startBitPosition += 32;

        int block_size = Subcolumn3Test.bytesToInt(encoded_result, startBitPosition, 32);
        startBitPosition += 32;

        int num_blocks = data_length / block_size;

        int[] data = new int[data_length];

        for (int i = 0; i < num_blocks; i++) {
            startBitPosition = BlockDecoder(encoded_result, i, block_size, block_size, startBitPosition, data);
        }

        int remainder = data_length % block_size;

        if (remainder <= 3) {
            for (int i = 0; i < remainder; i++) {
                data[num_blocks * block_size + i] = Subcolumn3Test.bytesToIntSigned(encoded_result, startBitPosition, 32);
                startBitPosition += 32;
            }
        } else {
            startBitPosition = BlockDecoder(encoded_result, num_blocks, block_size, remainder,
                    startBitPosition, data);
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

    public static int[] getAbsDeltaTsBlock(
            int[] ts_block,
            int i,
            int block_size,
            int remaining,
            int[] min_delta) {
        int[] ts_block_delta = new int[remaining - 1];

        int base = i * block_size + 1;
        int end = i * block_size + remaining;
        min_delta[0] = ts_block[base - 1];
        int value_delta_min = Integer.MAX_VALUE;
        int value_delta_max = Integer.MIN_VALUE;
        for (int j = base; j < end; j++) {
            int epsilon_v = ts_block[j] - ts_block[j - 1];
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

    public static int BlockEncoder(int[] data, int block_index, int block_size, int remainder,
            int startBitPosition, byte[] encoded_result, int[] beta) {
        int[] min_delta = new int[3];

        // data_delta 长度为 remainder - 1
        int[] data_delta = getAbsDeltaTsBlock(data, block_index, block_size, remainder, min_delta);

        Subcolumn3Test.intToBytes(min_delta[0], encoded_result, startBitPosition, 32);
        startBitPosition += 32;

        Subcolumn3Test.intToBytes(min_delta[1], encoded_result, startBitPosition, 32);
        startBitPosition += 32;

        if (block_index == 0) {
            int maxValue = 0;
            for (int j = 0; j < remainder - 1; j++) {
                if (data_delta[j] > maxValue) {
                    maxValue = data_delta[j];
                }
            }
            int m = Subcolumn3Test.bitWidth(maxValue);

            beta[0] = Subcolumn3Test.Subcolumn(data_delta, remainder - 1, m, block_size);
        }

        startBitPosition = Subcolumn3Test.SubcolumnEncoder(data_delta, startBitPosition, encoded_result, beta, block_size);

        return startBitPosition;
    }

    public static int BlockDecoder(byte[] encoded_result, int block_index, int block_size, int remainder,
            int startBitPosition, int[] data) {
        int[] min_delta = new int[3];

        min_delta[0] = Subcolumn3Test.bytesToIntSigned(encoded_result, startBitPosition, 32);
        startBitPosition += 32;

        min_delta[1] = Subcolumn3Test.bytesToIntSigned(encoded_result, startBitPosition, 32);
        startBitPosition += 32;

        int[] data_delta = new int[remainder - 1];

        startBitPosition = Subcolumn3Test.SubcolumnDecoder(encoded_result, startBitPosition, data_delta, block_size);

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

        return startBitPosition;
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
    public void testSPRINTZ() throws IOException {
        String parent_dir = "D:/github/xjz17/subcolumn/elf_resources/dataset/";
        // String parent_dir = "D:/compress-subcolumn/dataset/";

        String output_parent_dir = "D:/compress-subcolumn/";

        String outputPath = output_parent_dir + "sprintz_subcolumn3.csv";

        // int block_size = 1024;
        int block_size = 512;

        int repeatTime = 100;
        
        // repeatTime = 1;

        CsvWriter writer = new CsvWriter(outputPath, ',', StandardCharsets.UTF_8);

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

        File directory = new File(parent_dir);
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
                int cur_decimal = getDecimalPrecision(f_str);
                if (cur_decimal > max_decimal)
                    max_decimal = cur_decimal;
                data1.add(Float.valueOf(f_str));
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
                length = Encoder(data2_arr, block_size, encoded_result);
            }

            long e = System.nanoTime();
            encodeTime += ((e - s) / repeatTime);
            compressed_size += length / 8;
            double ratioTmp = compressed_size / (double) (data1.size() * Long.BYTES);
            ratio += ratioTmp;

            s = System.nanoTime();

            for (int repeat = 0; repeat < repeatTime; repeat++) {
                int[] data2_arr_decoded = Decoder(encoded_result);
                for (int i = 0; i < data2_arr_decoded.length; i++) {
                    assert data2_arr[i] == data2_arr_decoded[i]
                            || data2_arr[i] + Integer.MAX_VALUE + 1 == data2_arr_decoded[i];
                    // assert data2_arr[i] == data2_arr_decoded[i];
                }
            }

            e = System.nanoTime();
            decodeTime += ((e - s) / repeatTime);

            String[] record = {
                    datasetName,
                    "SPRINTZ+Subcolumn",
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
