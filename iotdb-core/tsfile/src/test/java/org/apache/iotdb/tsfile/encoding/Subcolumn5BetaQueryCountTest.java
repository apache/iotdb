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

public class Subcolumn5BetaQueryCountTest {
    // Subcolumn5Test Query Count

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
        String parent_dir = "D:/github/xjz17/subcolumn/elf_resources/dataset/";
        // String parent_dir = "D:/compress-subcolumn/dataset/";

        String output_parent_dir = "D:/compress-subcolumn/";

        // int[] block_size_list = { 32, 64, 128, 256, 512, 1024 };
        int[] beta_list = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23,
            24, 25, 26, 27, 28, 29, 30, 31 };
        
        int block_size = 512;

        HashMap<String, Integer> queryRange = new HashMap<>();
        queryRange.put("Air-pressure", 8820000);
        queryRange.put("Bird-migration", 2600000);
        queryRange.put("Bitcoin-price", 170000000);
        queryRange.put("Blockchain-tr", 300000);
        queryRange.put("City-temp", 700);
        queryRange.put("Dewpoint-temp", 9600);
        queryRange.put("IR-bio-temp", -200);
        queryRange.put("PM10-dust", 2000);
        queryRange.put("Stocks-DE", 90000);
        queryRange.put("Stocks-UK", 30000);
        queryRange.put("Stocks-USA", 6000);
        queryRange.put("Wind-Speed", 60);

        int repeatTime = 200;
        // TODO 真正计算时，记得注释掉将下面的内容
        // repeatTime = 1;

        for (int beta : beta_list) {
            String outputPath = output_parent_dir + "subcolumn5_query_count_beta_" + beta + ".csv";

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
                    Subcolumn5QueryCountTest.Query(encoded_result, queryRange.get(datasetName));
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
