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

import static org.junit.Assert.assertEquals;

public class SubcolumnBetaTest {

    public static int[] getAbsDeltaTsBlock(
            int[] ts_block,
            int i,
            int block_size,
            int remaining,
            int[] min_delta) {
        int[] ts_block_delta = new int[remaining];

        int value_delta_min = Integer.MAX_VALUE;
        int value_delta_max = Integer.MIN_VALUE;
        int base = i * block_size;
        int end = i * block_size + remaining;

        for (int j = base; j < end; j++) {
            int cur = ts_block[j];
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

    public static int BlockEncoder(int[] data, int block_index, int block_size, int remainder,
            int encode_pos, byte[] encoded_result, int[] beta) {
        int[] min_delta = new int[3];

        int[] data_delta = getAbsDeltaTsBlock(data, block_index, block_size,
                remainder, min_delta);

        encoded_result[encode_pos] = (byte) (min_delta[0] >> 24);
        encoded_result[encode_pos + 1] = (byte) (min_delta[0] >> 16);
        encoded_result[encode_pos + 2] = (byte) (min_delta[0] >> 8);
        encoded_result[encode_pos + 3] = (byte) min_delta[0];
        encode_pos += 4;

        encode_pos = SubcolumnTest.SubcolumnEncoder(data_delta, encode_pos,
                encoded_result, beta, block_size);

        return encode_pos;
    }

    public static int BlockDecoder(byte[] encoded_result, int block_index, int block_size, int remainder,
            int encode_pos, int[] data) {
        int[] min_delta = new int[3];

        min_delta[0] = ((encoded_result[encode_pos] & 0xFF) << 24) | ((encoded_result[encode_pos + 1] & 0xFF) << 16) |
                ((encoded_result[encode_pos + 2] & 0xFF) << 8) | (encoded_result[encode_pos + 3] & 0xFF);
        encode_pos += 4;

        int[] block_data = new int[remainder];

        encode_pos = SubcolumnTest.SubcolumnDecoder(encoded_result, encode_pos,
                block_data, block_size);

        for (int i = 0; i < remainder; i++) {
            data[block_index * block_size + i] = block_data[i] + min_delta[0];
        }

        return encode_pos;
    }

    public static int Encoder(int[] data, int block_size, byte[] encoded_result, int beta_value) {
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
        beta[0] = beta_value;

        for (int i = 0; i < num_blocks; i++) {
            encode_pos = BlockEncoder(data, i, block_size, block_size, encode_pos, encoded_result, beta);
        }

        if (remainder <= 3) {
            for (int i = 0; i < remainder; i++) {
                int value = data[num_blocks * block_size + i];
                encoded_result[encode_pos] = (byte) (value >> 24);
                encoded_result[encode_pos + 1] = (byte) (value >> 16);
                encoded_result[encode_pos + 2] = (byte) (value >> 8);
                encoded_result[encode_pos + 3] = (byte) value;
                encode_pos += 4;
            }
        } else {
            encode_pos = BlockEncoder(data, num_blocks, block_size, remainder, encode_pos,
                    encoded_result, beta);
        }

        return encode_pos;
    }

    public static int[] Decoder(byte[] encoded_result) {
        int encode_pos = 0;

        int data_length = ((encoded_result[encode_pos] & 0xFF) << 24) | ((encoded_result[encode_pos + 1] & 0xFF) << 16)
                |
                ((encoded_result[encode_pos + 2] & 0xFF) << 8) | (encoded_result[encode_pos + 3] & 0xFF);
        encode_pos += 4;

        int block_size = ((encoded_result[encode_pos] & 0xFF) << 24) | ((encoded_result[encode_pos + 1] & 0xFF) << 16) |
                ((encoded_result[encode_pos + 2] & 0xFF) << 8) | (encoded_result[encode_pos + 3] & 0xFF);
        encode_pos += 4;

        int num_blocks = data_length / block_size;

        int[] data = new int[data_length];

        for (int i = 0; i < num_blocks; i++) {
            encode_pos = BlockDecoder(encoded_result, i, block_size, block_size, encode_pos, data);
        }

        int remainder = data_length % block_size;

        if (remainder <= 3) {
            for (int i = 0; i < remainder; i++) {
                data[num_blocks * block_size + i] = ((encoded_result[encode_pos] & 0xFF) << 24) |
                        ((encoded_result[encode_pos + 1] & 0xFF) << 16) |
                        ((encoded_result[encode_pos + 2] & 0xFF) << 8) | (encoded_result[encode_pos + 3] & 0xFF);
                encode_pos += 4;
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
    public void testSubcolumn() throws IOException {
        String parent_dir = "D:/github/xjz17/subcolumn/";

        String input_parent_dir = parent_dir + "dataset/";
        
        String output_parent_dir = "D:/encoding-subcolumn/result/compression_vs_beta/";
        // String output_parent_dir = parent_dir + "result/compression_vs_beta/";

        int[] beta_list = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23,
                24, 25, 26, 27, 28, 29, 30, 31 };

        // int block_size = 1024;
        int block_size = 512;

        int repeatTime = 100;

        // repeatTime = 1;

        List<String> integerDatasets = new ArrayList<>();
        integerDatasets.add("Wine-Tasting");

        for (int beta : beta_list) {

            String outputPath = output_parent_dir + "subcolumn_beta_" + beta + ".csv";

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
                byte[] encoded_result = new byte[data2_arr.length * 4];

                long encodeTime = 0;
                long decodeTime = 0;
                double ratio = 0;
                double compressed_size = 0;

                int length = 0;

                long s = System.nanoTime();
                for (int repeat = 0; repeat < repeatTime; repeat++) {
                    length = Encoder(data2_arr, block_size, encoded_result, beta);
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

                s = System.nanoTime();

                int[] data2_arr_decoded = new int[data2_arr.length];

                for (int repeat = 0; repeat < repeatTime; repeat++) {
                    data2_arr_decoded = Decoder(encoded_result);
                }

                e = System.nanoTime();
                decodeTime += ((e - s) / repeatTime);

                for (int i = 0; i < data2_arr_decoded.length; i++) {
                    assertEquals(data2_arr[i], data2_arr_decoded[i]);
                }

                String[] record = {
                        datasetName,
                        "Sub-columns",
                        String.valueOf(encodeTime),
                        String.valueOf(decodeTime),
                        String.valueOf(data1.size()),
                        String.valueOf(compressed_size),
                        String.valueOf(ratio)
                };
                writer.writeRecord(record);
                System.out.println("beta: " + beta);
                System.out.println(ratio);
            }

            writer.close();
        }
    }

    @Test
    public void testTransData() throws IOException {
        String parent_dir = "D:/github/xjz17/subcolumn/";

        String output_parent_dir = "D:/encoding-subcolumn/trans_data_result/compression_vs_beta/";
        // String output_parent_dir = parent_dir + "trans_data_result/compression_vs_beta/";
        
        String input_parent_dir = parent_dir + "trans_data/";

        int[] beta_list = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23,
                24, 25, 26, 27, 28, 29, 30, 31 };

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

        // for (String name : dataset_name) {
        // output_path_list.add(output_parent_dir + name + "_ratio.csv");
        // }

        for (int beta : beta_list) {

            String outputPath = output_parent_dir + "subcolumn_trans_data_beta_" + beta + ".csv";
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

                // CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);
                // writer.setRecordDelimiter('\n');

                // String[] head = {
                // "Input Direction",
                // "Encoding Algorithm",
                // "Encoding Time",
                // "Decoding Time",
                // "Points",
                // "Compressed Size",
                // "Compression Ratio"
                // };
                // writer.writeRecord(head);

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
                        length = Encoder(data2_arr, dataset_block_size.get(file_i), encoded_result, beta);
                    }

                    long e = System.nanoTime();
                    encodeTime += ((e - s) / repeatTime);
                    compressed_size += length;
                    double ratioTmp = compressed_size / (double) (data1.size() * Integer.BYTES);
                    ratio += ratioTmp;
                    s = System.nanoTime();

                    int[] data2_arr_decoded = new int[data1.size()];

                    for (int repeat = 0; repeat < repeatTime; repeat++) {
                        data2_arr_decoded = Decoder(encoded_result);
                    }

                    e = System.nanoTime();
                    decodeTime += ((e - s) / repeatTime);

                    totalEncodeTime += encodeTime;
                    totalDecodeTime += decodeTime;
                    totalCompressedSize += compressed_size;
                    totalPoints += data1.size();

                    for (int i = 0; i < data2_arr_decoded.length; i++) {
                        assertEquals(data2_arr[i], data2_arr_decoded[i]);
                    }

                }

                double compressionRatio = totalCompressedSize / (totalPoints * Integer.BYTES);

                String[] record = {
                        dataset_name.get(file_i),
                        "Sub-columns",
                        String.valueOf(totalEncodeTime),
                        String.valueOf(totalDecodeTime),
                        String.valueOf(totalPoints),
                        String.valueOf(totalCompressedSize),
                        String.valueOf(compressionRatio)
                };

                writer.writeRecord(record);
                System.out.println(compressionRatio);

                System.out.println("beta: " + beta);
            }

            writer.close();
        }
    }

}
