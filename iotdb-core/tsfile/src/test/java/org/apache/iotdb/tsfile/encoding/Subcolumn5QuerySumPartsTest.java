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

public class Subcolumn5QuerySumPartsTest {

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
        // String parent_dir = "D:/encoding-subcolumn/dataset/";

        String output_parent_dir = "D:/encoding-subcolumn/";

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
                    length1 = Subcolumn5Test.Encoder(col1_data, block_size, encoded_result1);
                }
                long e = System.nanoTime();
                encodeTime += ((e - s) / repeatTime);

                // 编码第二列
                s = System.nanoTime();
                for (int repeat = 0; repeat < repeatTime; repeat++) {
                    length2 = Subcolumn5Test.Encoder(col2_data, block_size, encoded_result2);
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

                int[] col1_data_decoded = new int[halfSize];
                int[] col2_data_decoded = new int[halfSize];

                long sum_result = 0;

                s = System.nanoTime();

                for (int repeat = 0; repeat < repeatTime; repeat++) {
                    col1_data_decoded = Subcolumn5Test.Decoder(encoded_result1);
                    col2_data_decoded = Subcolumn5Test.Decoder(encoded_result2);

                    for (int i = 0; i < halfSize; i++) {
                        sum_result += col1_data_decoded[i] * col2_data_decoded[i];
                    }
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
        // String parent_dir = "D:/encoding-subcolumn/dataset/";

        String output_parent_dir = "D:/encoding-subcolumn/";

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
                    length1 = Subcolumn5Test.Encoder(col1_data, block_size, encoded_result1);
                }
                long e = System.nanoTime();
                encodeTime += ((e - s) / repeatTime);
                // 编码第二列
                s = System.nanoTime();
                for (int repeat = 0; repeat < repeatTime; repeat++) {
                    length2 = Subcolumn5Test.Encoder(col2_data, block_size, encoded_result2);
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
                int[] col1_data_decoded = new int[halfSize];
                int[] col2_data_decoded = new int[halfSize];
                long sum_result = 0;
                s = System.nanoTime();
                for (int repeat = 0; repeat < repeatTime; repeat++) {
                    col1_data_decoded = Subcolumn5Test.Decoder(encoded_result1);
                    col2_data_decoded = Subcolumn5Test.Decoder(encoded_result2);

                    for (int i = 0; i < halfSize; i++) {
                        sum_result += col1_data_decoded[i] * col2_data_decoded[i];
                    }
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
