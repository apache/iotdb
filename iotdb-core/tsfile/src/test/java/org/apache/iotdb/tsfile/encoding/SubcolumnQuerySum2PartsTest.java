package org.apache.iotdb.tsfile.encoding;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;

import static org.junit.Assert.assertEquals;

public class SubcolumnQuerySum2PartsTest {

    public static void Decoder(byte[] encoded_result1, byte[] encoded_result2) {

        int[] encode_pos = new int[2];

        int data_length1 = SubcolumnTest.bytes2Integer(encoded_result1, encode_pos[0], 4);
        int data_length2 = SubcolumnTest.bytes2Integer(encoded_result2, encode_pos[1], 4);

        encode_pos[0] += 4;
        encode_pos[1] += 4;

        int block_size1 = SubcolumnTest.bytes2Integer(encoded_result1, encode_pos[0], 4);
        int block_size2 = SubcolumnTest.bytes2Integer(encoded_result2, encode_pos[1], 4);
        encode_pos[0] += 4;
        encode_pos[1] += 4;

        int num_blocks = data_length1 / block_size1;

        for (int i = 0; i < num_blocks; i++) {
            encode_pos = BlockDecoder(encoded_result1, encoded_result2, i, block_size1, block_size1, encode_pos);
        }

        int remainder = data_length1 % block_size1;

        if (remainder <= 3) {
            for (int i = 0; i < remainder; i++) {
                int value1 = SubcolumnTest.bytes2Integer(encoded_result1, encode_pos[0], 4);
                encode_pos[0] += 4;
                int value2 = SubcolumnTest.bytes2Integer(encoded_result2, encode_pos[1], 4);
                encode_pos[1] += 4;
            }
        } else {
            encode_pos = BlockDecoder(encoded_result1, encoded_result2, num_blocks, block_size1, remainder,
                    encode_pos);
        }
    }

    public static int[] BlockDecoder(byte[] encoded_result1, byte[] encoded_result2, int block_index, int block_size,
            int remainder, int[] encode_pos) {

        // 存储求和结果
        long result = 0;

        int[] min_delta1 = new int[3];
        int[] min_delta2 = new int[3];

        min_delta1[0] = SubcolumnTest.bytes2Integer(encoded_result1, encode_pos[0], 4);
        encode_pos[0] += 4;

        min_delta2[0] = SubcolumnTest.bytes2Integer(encoded_result2, encode_pos[1], 4);
        encode_pos[1] += 4;

        int m1 = SubcolumnTest.bytes2Integer(encoded_result1, encode_pos[0], 1);
        encode_pos[0] += 1;

        int m2 = SubcolumnTest.bytes2Integer(encoded_result2, encode_pos[1], 1);
        encode_pos[1] += 1;

        if (m1 == 0 || m2 == 0) {
            if (m1 != 0) {
                int bw = SubcolumnTest.bitWidth(block_size);

                int beta = SubcolumnTest.bytes2Integer(encoded_result1, encode_pos[0], 1);
                encode_pos[0] += 1;

                int l = (m1 + beta - 1) / beta;

                int[] bitWidthList = new int[l];

                encode_pos[0] = SubcolumnTest.decodeBitPacking(encoded_result1, encode_pos[0], 8, l, bitWidthList);

                int[][] subcolumnList = new int[l][remainder];

                int[] encodingType = new int[l];
                encode_pos[0] = SubcolumnTest.decodeBitPacking(encoded_result1, encode_pos[0], 1, l, encodingType);

                for (int i = l - 1; i >= 0; i--) {
                    int type = encodingType[i];
                    int bitWidth = bitWidthList[i];
                    if (type == 0) {
                        encode_pos[0] = SubcolumnTest.decodeBitPacking(encoded_result1, encode_pos[0], bitWidth,
                                remainder, subcolumnList[i]);
                    } else {
                        int index = ((encoded_result1[encode_pos[0]] & 0xFF) << 8) |
                                (encoded_result1[encode_pos[0] + 1] & 0xFF);
                        encode_pos[0] += 2;

                        int[] run_length = new int[index];
                        int[] rle_values = new int[index];

                        encode_pos[0] = SubcolumnTest.decodeBitPacking(encoded_result1, encode_pos[0], bw, index,
                                run_length);
                        encode_pos[0] = SubcolumnTest.decodeBitPacking(encoded_result1, encode_pos[0], bitWidth, index,
                                rle_values);

                        int currentIndex = 0;
                        for (int j = 0; j < index; j++) {
                            int endPos = run_length[j];
                            int value = rle_values[j];
                            while (currentIndex < endPos) {
                                subcolumnList[i][currentIndex] = value;
                                currentIndex++;
                            }
                        }
                    }
                }
            }

            if (m2 != 0) {
                int bw = SubcolumnTest.bitWidth(block_size);

                int beta = SubcolumnTest.bytes2Integer(encoded_result2, encode_pos[1], 1);
                encode_pos[1] += 1;

                int l = (m2 + beta - 1) / beta;

                int[] bitWidthList = new int[l];

                encode_pos[1] = SubcolumnTest.decodeBitPacking(encoded_result2, encode_pos[1], 8, l, bitWidthList);

                int[][] subcolumnList = new int[l][remainder];

                int[] encodingType = new int[l];
                encode_pos[1] = SubcolumnTest.decodeBitPacking(encoded_result2, encode_pos[1], 1, l, encodingType);

                for (int i = l - 1; i >= 0; i--) {
                    int type = encodingType[i];
                    int bitWidth = bitWidthList[i];
                    if (type == 0) {
                        encode_pos[1] = SubcolumnTest.decodeBitPacking(encoded_result2, encode_pos[1], bitWidth,
                                remainder, subcolumnList[i]);
                    } else {
                        int index = ((encoded_result2[encode_pos[1]] & 0xFF) << 8) |
                                (encoded_result2[encode_pos[1] + 1] & 0xFF);
                        encode_pos[1] += 2;

                        int[] run_length = new int[index];
                        int[] rle_values = new int[index];

                        encode_pos[1] = SubcolumnTest.decodeBitPacking(encoded_result2, encode_pos[1], bw, index,
                                run_length);
                        encode_pos[1] = SubcolumnTest.decodeBitPacking(encoded_result2, encode_pos[1], bitWidth, index,
                                rle_values);

                        int currentIndex = 0;
                        for (int j = 0; j < index; j++) {
                            int endPos = run_length[j];
                            int value = rle_values[j];
                            while (currentIndex < endPos) {
                                subcolumnList[i][currentIndex] = value;
                                currentIndex++;
                            }
                        }
                    }
                }
            }

            return encode_pos;
        }

        int bw = SubcolumnTest.bitWidth(block_size);

        int beta1 = SubcolumnTest.bytes2Integer(encoded_result1, encode_pos[0], 1);
        encode_pos[0] += 1;

        int beta2 = SubcolumnTest.bytes2Integer(encoded_result2, encode_pos[1], 1);
        encode_pos[1] += 1;

        int l1 = (m1 + beta1 - 1) / beta1;
        int l2 = (m2 + beta2 - 1) / beta2;

        int[] bitWidthList1 = new int[l1];
        int[] bitWidthList2 = new int[l2];

        encode_pos[0] = SubcolumnTest.decodeBitPacking(encoded_result1, encode_pos[0], 8, l1, bitWidthList1);
        encode_pos[1] = SubcolumnTest.decodeBitPacking(encoded_result2, encode_pos[1], 8, l2, bitWidthList2);

        int[][] subcolumnList1 = new int[l1][remainder];
        int[][] subcolumnList2 = new int[l2][remainder];

        int[] encodingType1 = new int[l1];
        int[] encodingType2 = new int[l2];

        encode_pos[0] = SubcolumnTest.decodeBitPacking(encoded_result1, encode_pos[0], 1, l1, encodingType1);
        encode_pos[1] = SubcolumnTest.decodeBitPacking(encoded_result2, encode_pos[1], 1, l2, encodingType2);

        for (int i = l1 - 1; i >= 0; i--) {
            int type = encodingType1[i];
            int bitWidth = bitWidthList1[i];
            if (type == 0) {
                encode_pos[0] = SubcolumnTest.decodeBitPacking(encoded_result1, encode_pos[0], bitWidth,
                        remainder, subcolumnList1[i]);
            } else {
                int index = ((encoded_result1[encode_pos[0]] & 0xFF) << 8) |
                        (encoded_result1[encode_pos[0] + 1] & 0xFF);
                encode_pos[0] += 2;

                int[] run_length = new int[index];
                int[] rle_values = new int[index];

                encode_pos[0] = SubcolumnTest.decodeBitPacking(encoded_result1, encode_pos[0], bw, index, run_length);
                encode_pos[0] = SubcolumnTest.decodeBitPacking(encoded_result1, encode_pos[0], bitWidth, index,
                        rle_values);

                int currentIndex = 0;
                for (int j = 0; j < index; j++) {
                    int endPos = run_length[j];
                    int value = rle_values[j];
                    while (currentIndex < endPos) {
                        subcolumnList1[i][currentIndex] = value;
                        currentIndex++;
                    }
                }
            }
        }

        for (int i = l2 - 1; i >= 0; i--) {
            int type = encodingType2[i];
            int bitWidth = bitWidthList2[i];
            if (type == 0) {
                encode_pos[1] = SubcolumnTest.decodeBitPacking(encoded_result2, encode_pos[1], bitWidth,
                        remainder, subcolumnList2[i]);
            } else {
                int index = ((encoded_result2[encode_pos[1]] & 0xFF) << 8) |
                        (encoded_result2[encode_pos[1] + 1] & 0xFF);
                encode_pos[1] += 2;

                int[] run_length = new int[index];
                int[] rle_values = new int[index];

                encode_pos[1] = SubcolumnTest.decodeBitPacking(encoded_result2, encode_pos[1], bw, index, run_length);
                encode_pos[1] = SubcolumnTest.decodeBitPacking(encoded_result2, encode_pos[1], bitWidth, index,
                        rle_values);

                int currentIndex = 0;
                for (int j = 0; j < index; j++) {
                    int endPos = run_length[j];
                    int value = rle_values[j];
                    while (currentIndex < endPos) {
                        subcolumnList2[i][currentIndex] = value;
                        currentIndex++;
                    }
                }
            }
        }

        //目前使用的方法

        int[] decoded_data1 = new int[remainder];
        int[] decoded_data2 = new int[remainder];

        // 首先从subcolumn重建原始数据
        for (int i = 0; i < l1; i++) {
            int shiftAmount = i * beta1;
            for (int j = 0; j < remainder; j++) {
                decoded_data1[j] |= subcolumnList1[i][j] << shiftAmount;
            }
        }

        for (int i = 0; i < l2; i++) {
            int shiftAmount = i * beta2;
            for (int j = 0; j < remainder; j++) {
                decoded_data2[j] |= subcolumnList2[i][j] << shiftAmount;
            }
        }

        for (int j = 0; j < remainder; j++) {
            decoded_data1[j] += min_delta1[0];
            decoded_data2[j] += min_delta2[0];
        }

        // 相乘并累加到结果中
        result = 0;
        for (int j = 0; j < remainder; j++) {
            // 使用long类型避免整数溢出
            result += (long) decoded_data1[j] * decoded_data2[j];
        }

        long baseProduct = (long) min_delta1[0] * min_delta2[0];
        result += baseProduct * remainder;

        // 另一种方法

        // // 计算 subcolumn 的贡献
        // for (int j = 0; j < remainder; j++) {
        // // 处理 subcolumn1 的每个位置与 min_delta2 的乘积
        // for (int i = 0; i < l1; i++) {
        // int shiftAmount = i * beta1;
        // long value = subcolumnList1[i][j] << shiftAmount;
        // if (value != 0) {
        // result += value * min_delta2[0];
        // }
        // }

        // // 处理 subcolumn2 的每个位置与 min_delta1 的乘积
        // for (int i = 0; i < l2; i++) {
        // int shiftAmount = i * beta2;
        // long value = subcolumnList2[i][j] << shiftAmount;
        // if (value != 0) {
        // result += value * min_delta1[0];
        // }
        // }

        // // 处理两个 subcolumn 的交叉乘积
        // for (int i1 = 0; i1 < l1; i1++) {
        // int shiftAmount1 = i1 * beta1;
        // for (int i2 = 0; i2 < l2; i2++) {
        // int shiftAmount2 = i2 * beta2;
        // long value1 = subcolumnList1[i1][j] << shiftAmount1;
        // long value2 = subcolumnList2[i2][j] << shiftAmount2;
        // if (value1 != 0 && value2 != 0) {
        // result += value1 * value2;
        // }
        // }
        // }
        // }

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
        String parent_dir = "D:/github/xjz17/subcolumn/";

        String input_parent_dir = parent_dir + "dataset/";
        
        String output_parent_dir = "D:/encoding-subcolumn/result/query_vs_block/";
        // String output_parent_dir = parent_dir + "result/query_vs_block/";

        int[] block_size_list = { 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192 };

        List<String> integerDatasets = new ArrayList<>();
        integerDatasets.add("Wine-Tasting");

        int repeatTime = 200;

        // repeatTime = 1;

        for (int block_size : block_size_list) {
            String outputPath = output_parent_dir + "subcolumn_query_sum_parts_block_" + block_size + ".csv";

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

                int totalSize = data1.size();
                int halfSize = totalSize / 2;

                // 创建两个数据列
                int[] col1_data = new int[halfSize];
                int[] col2_data = new int[halfSize];

                int max_mul = (int) Math.pow(10, max_decimal);

                // 填充第一列
                for (int i = 0; i < halfSize; i++) {
                    col1_data[i] = (int) (data1.get(i) * max_mul);
                }

                // 填充第二列
                for (int i = 0; i < halfSize; i++) {
                    col2_data[i] = (int) (data1.get(i + halfSize) * max_mul);
                }

                System.out.println(max_decimal);

                byte[] encoded_result1 = new byte[col1_data.length * 4];
                byte[] encoded_result2 = new byte[col2_data.length * 4];

                long encodeTime = 0;
                long decodeTime = 0;
                double ratio = 0;
                double compressed_size = 0;

                int length1 = 0;
                int length2 = 0;

                // 编码第一列
                long s = System.nanoTime();
                for (int repeat = 0; repeat < repeatTime; repeat++) {
                    length1 = SubcolumnTest.Encoder(col1_data, block_size, encoded_result1);
                }
                long e = System.nanoTime();
                encodeTime += ((e - s) / repeatTime);

                // 编码第二列
                s = System.nanoTime();
                for (int repeat = 0; repeat < repeatTime; repeat++) {
                    length2 = SubcolumnTest.Encoder(col2_data, block_size, encoded_result2);
                }
                e = System.nanoTime();
                encodeTime += ((e - s) / repeatTime);

                compressed_size = length1 + length2;

                double ratioTmp;

                if (integerDatasets.contains(datasetName)) {
                    ratioTmp = compressed_size / (double) (data1.size() * Integer.BYTES);
                } else {
                    ratioTmp = compressed_size / (double) (data1.size() * Long.BYTES);
                }

                ratio += ratioTmp;

                System.out.println("Query");

                s = System.nanoTime();

                for (int repeat = 0; repeat < repeatTime; repeat++) {

                    Decoder(encoded_result1, encoded_result2);
                }

                e = System.nanoTime();
                decodeTime += ((e - s) / repeatTime);

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

                System.out.println("block_size: " + block_size);

                System.out.println(ratio);
            }

            writer.close();
        }
    }

    @Test
    public void testQueryBeta() throws IOException {
        String parent_dir = "D:/github/xjz17/subcolumn/";

        String input_parent_dir = parent_dir + "dataset/";
        
        String output_parent_dir = "D:/encoding-subcolumn/result/query_vs_beta/";
        // String output_parent_dir = parent_dir + "result/query_vs_beta/";

        int[] beta_list = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23,
                24, 25, 26, 27, 28, 29, 30, 31 };

        int block_size = 512;

        List<String> integerDatasets = new ArrayList<>();
        integerDatasets.add("Wine-Tasting");

        int repeatTime = 200;

        // repeatTime = 1;

        for (int beta : beta_list) {
            String outputPath = output_parent_dir + "subcolumn_query_sum_parts_beta_" + beta + ".csv";

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

                int totalSize = data1.size();
                int halfSize = totalSize / 2;

                // 创建两个数据列
                int[] col1_data = new int[halfSize];
                int[] col2_data = new int[halfSize];

                int max_mul = (int) Math.pow(10, max_decimal);

                // 填充第一列
                for (int i = 0; i < halfSize; i++) {
                    col1_data[i] = (int) (data1.get(i) * max_mul);
                }

                // 填充第二列
                for (int i = 0; i < halfSize; i++) {
                    col2_data[i] = (int) (data1.get(i + halfSize) * max_mul);
                }

                System.out.println(max_decimal);

                byte[] encoded_result1 = new byte[col1_data.length * 4];
                byte[] encoded_result2 = new byte[col2_data.length * 4];
                long encodeTime = 0;
                long decodeTime = 0;
                double ratio = 0;
                double compressed_size = 0;
                int length1 = 0;
                int length2 = 0;
                // 编码第一列
                long s = System.nanoTime();
                for (int repeat = 0; repeat < repeatTime; repeat++) {
                    length1 = SubcolumnTest.Encoder(col1_data, block_size, encoded_result1);
                }
                long e = System.nanoTime();
                encodeTime += ((e - s) / repeatTime);
                // 编码第二列
                s = System.nanoTime();
                for (int repeat = 0; repeat < repeatTime; repeat++) {
                    length2 = SubcolumnTest.Encoder(col2_data, block_size, encoded_result2);
                }
                e = System.nanoTime();
                encodeTime += ((e - s) / repeatTime);
                compressed_size = length1 + length2;

                double ratioTmp;
                if (integerDatasets.contains(datasetName)) {
                    ratioTmp = compressed_size / (double) (data1.size() * Integer.BYTES);
                } else {
                    ratioTmp = compressed_size / (double) (data1.size() * Long.BYTES);
                }

                ratio += ratioTmp;
                
                System.out.println("Query");

                s = System.nanoTime();
                for (int repeat = 0; repeat < repeatTime; repeat++) {
                    Decoder(encoded_result1, encoded_result2);
                }
                e = System.nanoTime();
                decodeTime += ((e - s) / repeatTime);
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

}
