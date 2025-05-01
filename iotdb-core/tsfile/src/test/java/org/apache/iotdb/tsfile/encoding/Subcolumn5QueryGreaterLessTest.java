package org.apache.iotdb.tsfile.encoding;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;

import org.junit.Test;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;

public class Subcolumn5QueryGreaterLessTest {
    // Subcolumn5Test Query Greater Less

    public static void Query(byte[] encoded_result, int lower_bound, int upper_bound) {

        int encode_pos = 0;

        // int data_length = ((encoded_result[encode_pos] & 0xFF) << 24) | ((encoded_result[encode_pos + 1] & 0xFF) << 16)
        //         |
        //         ((encoded_result[encode_pos + 2] & 0xFF) << 8) | (encoded_result[encode_pos + 3] & 0xFF);
        int data_length = Subcolumn5Test.bytes2Integer(encoded_result, encode_pos, 4);
        encode_pos += 4;

        // int block_size = ((encoded_result[encode_pos] & 0xFF) << 24) | ((encoded_result[encode_pos + 1] & 0xFF) << 16) |
        //         ((encoded_result[encode_pos + 2] & 0xFF) << 8) | (encoded_result[encode_pos + 3] & 0xFF);
        int block_size = Subcolumn5Test.bytes2Integer(encoded_result, encode_pos, 4);
        encode_pos += 4;

        int num_blocks = data_length / block_size;

        // 查询结果
        int[] result = new int[data_length];
        int[] result_length = new int[1];

        for (int i = 0; i < num_blocks; i++) {
            encode_pos = BlockQueryIndex(encoded_result, i, block_size,
                    block_size, encode_pos, lower_bound, upper_bound,
                    result, result_length);
        }

        int remainder = data_length % block_size;

        if (remainder <= 3) {
            for (int i = 0; i < remainder; i++) {
                // int value = ((encoded_result[encode_pos] & 0xFF) << 24) |
                //         ((encoded_result[encode_pos + 1] & 0xFF) << 16) |
                //         ((encoded_result[encode_pos + 2] & 0xFF) << 8) | (encoded_result[encode_pos + 3] & 0xFF);
                int value = Subcolumn5Test.bytes2Integer(encoded_result, encode_pos, 4);
                if (value > lower_bound && value < upper_bound) {
                    result[result_length[0]] = value;
                    result_length[0]++;
                }
                encode_pos += 4;
            }
        } else {
            encode_pos = BlockQueryIndex(encoded_result, num_blocks, block_size,
                    remainder, encode_pos, lower_bound, upper_bound,
                    result, result_length);
        }

    }

    public static int BlockQueryIndex(byte[] encoded_result, int block_index, int block_size, int remainder,
            int encode_pos, int lower_bound, int upper_bound, int[] result, int[] result_length) {
        int[] min_delta = new int[3];

        // min_delta[0] = ((encoded_result[encode_pos] & 0xFF) << 24) | ((encoded_result[encode_pos + 1] & 0xFF) << 16) |
        //         ((encoded_result[encode_pos + 2] & 0xFF) << 8) | (encoded_result[encode_pos + 3] & 0xFF);
        min_delta[0] = Subcolumn5Test.bytes2Integer(encoded_result, encode_pos, 4);
        encode_pos += 4;

        // int[] block_data = new int[remainder];

        // int m = encoded_result[encode_pos];
        int m = Subcolumn5Test.bytes2Integer(encoded_result, encode_pos, 1);
        encode_pos += 1;

        lower_bound -= min_delta[0];
        upper_bound -= min_delta[0];

        // 候选索引
        int[] candidate_indices = new int[remainder];
        int candidate_length = 0;
        for (int i = 0; i < remainder; i++) {
            candidate_indices[i] = i;
            candidate_length++;
        }

        if (m == 0) {
            if (lower_bound < 0 && upper_bound > 0) {
                for (int i = 0; i < remainder; i++) {
                    result[result_length[0]] = block_size * block_index + i;
                    result_length[0]++;
                }
            }
            return encode_pos;
        }

        int bw = Subcolumn5Test.bitWidth(block_size);

        // int beta = encoded_result[encode_pos];
        int beta = Subcolumn5Test.bytes2Integer(encoded_result, encode_pos, 1);
        encode_pos += 1;

        int l = (m + beta - 1) / beta;

        int[] bitWidthList = new int[l];

        encode_pos = Subcolumn5Test.decodeBitPacking(encoded_result, encode_pos, 8, l, bitWidthList);

        int[][] subcolumnList = new int[l][remainder];

        int[] encodingType = new int[l];

        encode_pos = Subcolumn5Test.decodeBitPacking(encoded_result, encode_pos, 1, l, encodingType);

        for (int i = l - 1; i >= 0; i--) {
            int type = encodingType[i];
            if (type == 0) {

                if (upper_bound <= 0) {
                    encode_pos *= 8;
                    encode_pos += bitWidthList[i] * remainder;
                    encode_pos = (encode_pos + 7) / 8;
                    continue;
                }

                encode_pos *= 8;

                int new_length = 0;
                for (int j = 0; j < candidate_length; j++) {
                    int index = candidate_indices[j];

                    subcolumnList[i][index] = Subcolumn5Test.bytesToInt(encoded_result,
                            encode_pos + index * bitWidthList[i], bitWidthList[i]);
                    int lower_value = (lower_bound >> (i * beta)) & ((1 << beta) - 1);
                    int upper_value = (upper_bound >> (i * beta)) & ((1 << beta) - 1);
                    if (subcolumnList[i][index] > lower_value && subcolumnList[i][index] < upper_value) {
                        result[result_length[0]] = block_size * block_index + index;
                        result_length[0]++;
                    } else if (subcolumnList[i][index] == lower_value 
                            || subcolumnList[i][index] == upper_value) {
                        candidate_indices[new_length] = index;
                        new_length++;
                    }
                }

                candidate_length = new_length;

                encode_pos += remainder * bitWidthList[i];
                encode_pos = (encode_pos + 7) / 8;

            } else {

                int index = ((encoded_result[encode_pos] & 0xFF) << 8) | (encoded_result[encode_pos + 1] & 0xFF);
                encode_pos += 2;

                if (upper_bound <= 0) {
                    encode_pos *= 8;
                    encode_pos += bw * index;
                    encode_pos = (encode_pos + 7) / 8;

                    encode_pos *= 8;
                    encode_pos += bitWidthList[i] * index;
                    encode_pos = (encode_pos + 7) / 8;
                    continue;
                }

                int[] run_length = new int[index];
                int[] rle_values = new int[index];

                encode_pos = Subcolumn5Test.decodeBitPacking(encoded_result, encode_pos, bw, index, run_length);
                encode_pos = Subcolumn5Test.decodeBitPacking(encoded_result, encode_pos, bitWidthList[i], index,
                        rle_values);

                int new_length = 0;
                int rleIndex = 0;
                int currentPos = 0;
                int lower_value = (lower_bound >> (i * beta)) & ((1 << beta) - 1);
                int upper_value = (upper_bound >> (i * beta)) & ((1 << beta) - 1);

                for (int j = 0; j < candidate_length; j++) {
                    int index_candidate = candidate_indices[j];

                    while (rleIndex < index && currentPos + run_length[rleIndex] <= index_candidate) {
                        currentPos += run_length[rleIndex];
                        rleIndex++;
                    }

                    if (rleIndex < index) {
                        if (rle_values[rleIndex] > lower_value && rle_values[rleIndex] < upper_value) {
                            result[result_length[0]] = block_size * block_index + index_candidate;
                            result_length[0]++;
                        } else if (rle_values[rleIndex] == lower_value 
                                || rle_values[rleIndex] == upper_value) {
                            candidate_indices[new_length] = index_candidate;
                            new_length++;
                        }
                    }
                }

                candidate_length = new_length;

            }
        }

        return encode_pos;
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
    public void testQuery() throws IOException {
        String parent_dir = "D:/github/xjz17/subcolumn/dataset/";
        // String parent_dir = "D:/compress-subcolumn/dataset/";

        String output_parent_dir = "D:/encoding-subcolumn/";

        int[] block_size_list = { 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192 };

        HashMap<String, Integer> queryGreaterRange = new HashMap<>();
        queryGreaterRange.put("Air-pressure", 8720000);
        queryGreaterRange.put("Bird-migration", 2500000);
        queryGreaterRange.put("Bitcoin-price", 160000000);
        queryGreaterRange.put("Blockchain-tr", 100000);
        queryGreaterRange.put("City-temp", 480);
        queryGreaterRange.put("Dewpoint-temp", 9500);
        queryGreaterRange.put("IR-bio-temp", -300);
        queryGreaterRange.put("PM10-dust", 1000);
        queryGreaterRange.put("Stocks-DE", 40000);
        queryGreaterRange.put("Stocks-UK", 20000);
        queryGreaterRange.put("Stocks-USA", 5000);
        queryGreaterRange.put("Wind-Speed", 50);

        HashMap<String, Integer> queryLessRange = new HashMap<>();
        queryLessRange.put("Air-pressure", 8820000);
        queryLessRange.put("Bird-migration", 2600000);
        queryLessRange.put("Bitcoin-price", 170000000);
        queryLessRange.put("Blockchain-tr", 300000);
        queryLessRange.put("City-temp", 700);
        queryLessRange.put("Dewpoint-temp", 9600);
        queryLessRange.put("IR-bio-temp", -200);
        queryLessRange.put("PM10-dust", 2000);
        queryLessRange.put("Stocks-DE", 90000);
        queryLessRange.put("Stocks-UK", 30000);
        queryLessRange.put("Stocks-USA", 6000);
        queryLessRange.put("Wind-Speed", 60);

        int repeatTime = 200;
        
        // repeatTime = 1;

        for (int block_size : block_size_list) {
            String outputPath = output_parent_dir + "subcolumn_query_greater_less_block_" + block_size + ".csv";

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
                    length = Subcolumn5Test.Encoder(data2_arr, block_size, encoded_result);
                }

                long e = System.nanoTime();
                encodeTime += ((e - s) / repeatTime);
                // compressed_size += length / 8;
                compressed_size += length;
                double ratioTmp = compressed_size / (double) (data1.size() * Long.BYTES);
                ratio += ratioTmp;

                System.out.println("Query");

                s = System.nanoTime();

                for (int repeat = 0; repeat < repeatTime; repeat++) {
                    Query(encoded_result, queryGreaterRange.get(datasetName),
                            queryLessRange.get(datasetName));
                }

                e = System.nanoTime();
                decodeTime += ((e - s) / repeatTime);

                String[] record = {
                        datasetName,
                        "Subcolumn",
                        String.valueOf(encodeTime),
                        String.valueOf(decodeTime),
                        String.valueOf(data1.size()),
                        String.valueOf(compressed_size),
                        String.valueOf(ratio)
                };
                writer.writeRecord(record);

                System.out.println("block_size: " + block_size);

                System.out.println(ratio);
            }

            writer.close();
        }
    }

    @Test
    public void testQueryBeta() throws IOException {
        String parent_dir = "D:/github/xjz17/subcolumn/dataset/";
        // String parent_dir = "D:/compress-subcolumn/dataset/";

        String output_parent_dir = "D:/encoding-subcolumn/";

        // int[] block_size_list = { 32, 64, 128, 256, 512, 1024 };
        int[] beta_list = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23,
            24, 25, 26, 27, 28, 29, 30, 31 };
        
        int block_size = 512;

        HashMap<String, Integer> queryGreaterRange = new HashMap<>();
        queryGreaterRange.put("Air-pressure", 8720000);
        queryGreaterRange.put("Bird-migration", 2500000);
        queryGreaterRange.put("Bitcoin-price", 160000000);
        queryGreaterRange.put("Blockchain-tr", 100000);
        queryGreaterRange.put("City-temp", 480);
        queryGreaterRange.put("Dewpoint-temp", 9500);
        queryGreaterRange.put("IR-bio-temp", -300);
        queryGreaterRange.put("PM10-dust", 1000);
        queryGreaterRange.put("Stocks-DE", 40000);
        queryGreaterRange.put("Stocks-UK", 20000);
        queryGreaterRange.put("Stocks-USA", 5000);
        queryGreaterRange.put("Wind-Speed", 50);

        HashMap<String, Integer> queryLessRange = new HashMap<>();
        queryLessRange.put("Air-pressure", 8820000);
        queryLessRange.put("Bird-migration", 2600000);
        queryLessRange.put("Bitcoin-price", 170000000);
        queryLessRange.put("Blockchain-tr", 300000);
        queryLessRange.put("City-temp", 700);
        queryLessRange.put("Dewpoint-temp", 9600);
        queryLessRange.put("IR-bio-temp", -200);
        queryLessRange.put("PM10-dust", 2000);
        queryLessRange.put("Stocks-DE", 90000);
        queryLessRange.put("Stocks-UK", 30000);
        queryLessRange.put("Stocks-USA", 6000);
        queryLessRange.put("Wind-Speed", 60);

        int repeatTime = 200;

        // repeatTime = 1;

        for (int beta : beta_list) {
            String outputPath = output_parent_dir + "subcolumn_query_greater_less_beta_" + beta + ".csv";

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
                    length = Subcolumn5BetaTest.Encoder(data2_arr, block_size, encoded_result, beta);
                }

                long e = System.nanoTime();
                encodeTime += ((e - s) / repeatTime);
                // compressed_size += length / 8;
                compressed_size += length;
                double ratioTmp = compressed_size / (double) (data1.size() * Long.BYTES);
                ratio += ratioTmp;

                System.out.println("Query");

                s = System.nanoTime();

                for (int repeat = 0; repeat < repeatTime; repeat++) {
                    Subcolumn5QueryGreaterLessTest.Query(encoded_result,
                            queryGreaterRange.get(datasetName),
                            queryLessRange.get(datasetName));
                }

                e = System.nanoTime();
                decodeTime += ((e - s) / repeatTime);

                String[] record = {
                        datasetName,
                        "Subcolumn",
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

}
