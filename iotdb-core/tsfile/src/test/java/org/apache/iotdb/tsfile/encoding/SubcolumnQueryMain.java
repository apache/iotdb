package org.apache.iotdb.tsfile.encoding;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.junit.Test;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;

public class SubcolumnQueryMain {

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

        String output_parent_dir = "D:/encoding-subcolumn/result/subcolumn_query/";
        // String output_parent_dir = parent_dir + "result/";

        HashMap<String, Integer> queryRange = new HashMap<>();

        queryRange.put("Bird-migration", 2500000);
        queryRange.put("Bitcoin-price", 160000000);
        queryRange.put("City-temp", 480);
        queryRange.put("Dewpoint-temp", 9500);
        queryRange.put("IR-bio-temp", -300);
        queryRange.put("PM10-dust", 1000);
        queryRange.put("Stocks-DE", 40000);
        queryRange.put("Stocks-UK", 20000);
        queryRange.put("Stocks-USA", 5000);
        queryRange.put("Wind-Speed", 50);
        queryRange.put("Wine-Tasting", 0);
        queryRange.put("Arade4", 10000000);
        queryRange.put("EPM-Education", 200);
        queryRange.put("POI-lat", 0);
        queryRange.put("Gov10", 100000);

        HashMap<String, Integer> queryLessRange = new HashMap();

        queryLessRange.put("Bird-migration", 2600000);
        queryLessRange.put("Bitcoin-price", 170000000);
        queryLessRange.put("City-temp", 700);
        queryLessRange.put("Dewpoint-temp", 9600);
        queryLessRange.put("IR-bio-temp", -200);
        queryLessRange.put("PM10-dust", 2000);
        queryLessRange.put("Stocks-DE", 90000);
        queryLessRange.put("Stocks-UK", 30000);
        queryLessRange.put("Stocks-USA", 6000);
        queryLessRange.put("Wind-Speed", 60);
        queryLessRange.put("Wine-Tasting", 10);
        queryLessRange.put("Arade4", 12000000);
        queryLessRange.put("EPM-Education", 300);
        queryLessRange.put("POI-lat", 1);
        queryLessRange.put("Gov10", 120000);

        int repeatTime = 100;

        repeatTime = 500;

        int block_size = 512;

        // repeatTime = 1;

        // String outputPath = output_parent_dir + "subcolumn_query_count_block_" +
        // block_size + ".csv";
        // String outputPath = output_parent_dir + "subcolumn_query_max.csv";

        // String outputPath = output_parent_dir + "subcolumn_query_greater_new.csv";
        // String outputPath = output_parent_dir + "subcolumn_query_greater.csv";
        String outputPath = output_parent_dir + "subcolumn_query_greater_less.csv";
        // String outputPath = output_parent_dir + "subcolumn_query_less.csv";
        // String outputPath = output_parent_dir + "subcolumn_query_equal.csv";

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
            byte[] encoded_result = new byte[data2_arr.length * 8];

            long encodeTime = 0;
            long decodeTime = 0;
            double ratio = 0;
            double compressed_size = 0;

            int length = 0;

            long s = System.nanoTime();
            for (int repeat = 0; repeat < repeatTime; repeat++) {
                length = SubcolumnTest.Encoder(data2_arr, block_size, encoded_result);
            }

            long e = System.nanoTime();
            encodeTime += ((e - s) / repeatTime);
            compressed_size += length;

            double ratioTmp;

            ratioTmp = compressed_size / (double) (data1.size() * Long.BYTES);

            ratio += ratioTmp;

            System.out.println("Query");

            s = System.nanoTime();

            for (int repeat = 0; repeat < repeatTime; repeat++) {
                // SubcolumnQueryCountTest.Query(encoded_result, queryRange.get(datasetName));
                SubcolumnQueryGreaterLessTest.Query(encoded_result, queryRange.get(datasetName), queryLessRange.get(datasetName));
                // SubcolumnQueryGreaterTest.Query(encoded_result, queryRange.get(datasetName));
                // SubcolumnQueryEqualTest.Query(encoded_result, queryRange.get(datasetName));
                // SubcolumnQueryLessTest.Query(encoded_result, queryRange.get(datasetName));
            }

            e = System.nanoTime();
            decodeTime += ((e - s) / repeatTime);

            String[] record = {
                    datasetName,
                    "Sub-column",
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
    public void testQueryBeta() throws IOException {
        String parent_dir = "D:/github/xjz17/subcolumn/";

        String input_parent_dir = parent_dir + "dataset/";

        String output_parent_dir = "D:/encoding-subcolumn/result/";
        // String output_parent_dir = parent_dir + "result/";

        HashMap<String, Integer> queryRange = new HashMap<>();

        queryRange.put("Bird-migration", 2500000);
        queryRange.put("Bitcoin-price", 160000000);
        queryRange.put("City-temp", 480);
        queryRange.put("Dewpoint-temp", 9500);
        queryRange.put("IR-bio-temp", -300);
        queryRange.put("PM10-dust", 1000);
        queryRange.put("Stocks-DE", 40000);
        queryRange.put("Stocks-UK", 20000);
        queryRange.put("Stocks-USA", 5000);
        queryRange.put("Wind-Speed", 50);
        queryRange.put("Wine-Tasting", 0);
        queryRange.put("Arade4", 10000000);
        queryRange.put("EPM-Education", 200);
        queryRange.put("POI-lat", 0);
        queryRange.put("Gov10", 100000);

        int repeatTime = 100;

        repeatTime = 500;
        // repeatTime = 1;

        int block_size = 512;

        int beta = 3;

        // String outputPath = output_parent_dir + "subcolumn_query_count_block_" +
        // block_size + ".csv";
        String outputPath = output_parent_dir + "subcolumn_query_max_beta_" + beta + ".csv";

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
            byte[] encoded_result = new byte[data2_arr.length * 8];

            long encodeTime = 0;
            long decodeTime = 0;
            double ratio = 0;
            double compressed_size = 0;

            int length = 0;

            long s = System.nanoTime();
            for (int repeat = 0; repeat < repeatTime; repeat++) {
                length = SubcolumnBetaTest.Encoder(data2_arr, block_size, encoded_result, beta);
            }

            long e = System.nanoTime();
            encodeTime += ((e - s) / repeatTime);
            compressed_size += length;

            double ratioTmp;

            ratioTmp = compressed_size / (double) (data1.size() * Long.BYTES);

            ratio += ratioTmp;

            System.out.println("Query");

            s = System.nanoTime();

            for (int repeat = 0; repeat < repeatTime; repeat++) {
                // SubcolumnQueryCountTest.Query(encoded_result, queryRange.get(datasetName));
                SubcolumnQueryMaxTest.Query(encoded_result);
            }

            e = System.nanoTime();
            decodeTime += ((e - s) / repeatTime);

            String[] record = {
                    datasetName,
                    "Sub-column",
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
    public void testQueryLong() throws IOException {
        String parent_dir = "D:/github/xjz17/subcolumn/";

        String input_parent_dir = parent_dir + "dataset/";

        String output_parent_dir = "D:/encoding-subcolumn/result/";
        // String output_parent_dir = parent_dir + "result/";

        HashMap<String, Integer> queryRange = new HashMap<>();

        queryRange.put("Bird-migration", 2500000);
        queryRange.put("Bitcoin-price", 160000000);
        queryRange.put("City-temp", 480);
        queryRange.put("Dewpoint-temp", 9500);
        queryRange.put("IR-bio-temp", -300);
        queryRange.put("PM10-dust", 1000);
        queryRange.put("Stocks-DE", 40000);
        queryRange.put("Stocks-UK", 20000);
        queryRange.put("Stocks-USA", 5000);
        queryRange.put("Wind-Speed", 50);
        queryRange.put("Wine-Tasting", 0);
        queryRange.put("Arade4", 10000000);
        queryRange.put("EPM-Education", 200);
        queryRange.put("POI-lat", 0);
        queryRange.put("Gov10", 100000);

        int repeatTime = 100;

        repeatTime = 500;

        int block_size = 512;

        // String outputPath = output_parent_dir + "subcolumn_long_query_count.csv";
        String outputPath = output_parent_dir + "subcolumn_long_query_sum2.csv";

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
                length = SubcolumnLongTest.Encoder(data2_arr, block_size, encoded_result);
            }

            long e = System.nanoTime();
            encodeTime += ((e - s) / repeatTime);
            compressed_size += length;

            double ratioTmp;

            ratioTmp = compressed_size / (double) (data1.size() * Long.BYTES);

            ratio += ratioTmp;

            System.out.println("Query");

            s = System.nanoTime();

            for (int repeat = 0; repeat < repeatTime; repeat++) {
                // SubcolumnLongQueryCountTest.Query(encoded_result, queryRange.get(datasetName));
                SubcolumnLongQuerySum2Test.Query(encoded_result, queryRange.get(datasetName));
            }

            e = System.nanoTime();
            decodeTime += ((e - s) / repeatTime);

            String[] record = {
                    datasetName,
                    "Sub-column",
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

    // block size
    @Test
    public void test1() throws IOException {
        String parent_dir = "D:/github/xjz17/subcolumn/";

        String input_parent_dir = parent_dir + "dataset/";

        String output_parent_dir = "D:/encoding-subcolumn/result/query_vs_block/";
        // String output_parent_dir = parent_dir + "result/query_vs_block/";

        // int[] block_size_list = { 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192 };

        int[] block_size_list = { 512 };

        HashMap<String, Integer> queryRange = new HashMap<>();

        queryRange.put("Bird-migration", 2500000);
        queryRange.put("Bitcoin-price", 160000000);
        queryRange.put("City-temp", 480);
        queryRange.put("Dewpoint-temp", 9500);
        queryRange.put("IR-bio-temp", -300);
        queryRange.put("PM10-dust", 1000);
        queryRange.put("Stocks-DE", 40000);
        queryRange.put("Stocks-UK", 20000);
        queryRange.put("Stocks-USA", 5000);
        queryRange.put("Wind-Speed", 50);
        queryRange.put("Wine-Tasting", 0);
        queryRange.put("Arade4", 10000000);
        queryRange.put("EPM-Education", 200);
        queryRange.put("POI-lat", 0);
        queryRange.put("Gov10", 100000);

        int repeatTime = 100;

        repeatTime = 500;

        // repeatTime = 1;

        for (int block_size : block_size_list) {
            // String outputPath = output_parent_dir + "subcolumn_query_count_block_" +
            // block_size + ".csv";
            String outputPath = output_parent_dir + "subcolumn_query_greater_block_" + block_size + ".csv";

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
                byte[] encoded_result = new byte[data2_arr.length * 8];

                long encodeTime = 0;
                long decodeTime = 0;
                double ratio = 0;
                double compressed_size = 0;

                int length = 0;

                long s = System.nanoTime();
                for (int repeat = 0; repeat < repeatTime; repeat++) {
                    length = SubcolumnTest.Encoder(data2_arr, block_size, encoded_result);
                }

                long e = System.nanoTime();
                encodeTime += ((e - s) / repeatTime);
                compressed_size += length;

                double ratioTmp;

                ratioTmp = compressed_size / (double) (data1.size() * Long.BYTES);

                ratio += ratioTmp;

                System.out.println("Query");

                s = System.nanoTime();

                for (int repeat = 0; repeat < repeatTime; repeat++) {
                    // SubcolumnQueryCountTest.Query(encoded_result, queryRange.get(datasetName));
                    // SubcolumnQueryMaxTest.Query(encoded_result);
                    SubcolumnQueryGreaterTest.Query(encoded_result, queryRange.get(datasetName));
                }

                e = System.nanoTime();
                decodeTime += ((e - s) / repeatTime);

                String[] record = {
                        datasetName,
                        "Sub-column",
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

    // beta
    @Test
    public void test2() throws IOException {
        String parent_dir = "D:/github/xjz17/subcolumn/";

        String input_parent_dir = parent_dir + "dataset/";

        String output_parent_dir = "D:/encoding-subcolumn/result/query_vs_beta/";
        // String output_parent_dir = parent_dir + "result/query_vs_beta/";

        // int[] beta_list = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
        // 17, 18, 19, 20, 21, 22, 23,
        // 24, 25, 26, 27, 28, 29, 30, 31 };

        int[] beta_list = { 3, 4, 5, 6, 7 };

        int block_size = 512;

        HashMap<String, Integer> queryRange = new HashMap<>();

        queryRange.put("Bird-migration", 2500000);
        queryRange.put("Bitcoin-price", 160000000);
        queryRange.put("City-temp", 480);
        queryRange.put("Dewpoint-temp", 9500);
        queryRange.put("IR-bio-temp", -300);
        queryRange.put("PM10-dust", 1000);
        queryRange.put("Stocks-DE", 40000);
        queryRange.put("Stocks-UK", 20000);
        queryRange.put("Stocks-USA", 5000);
        queryRange.put("Wind-Speed", 50);
        queryRange.put("Wine-Tasting", 0);
        queryRange.put("Arade4", 10000000);
        queryRange.put("EPM-Education", 200);
        queryRange.put("POI-lat", 0);
        queryRange.put("Gov10", 100000);

        HashMap<String, Integer> queryLessRange = new HashMap();

        queryLessRange.put("Bird-migration", 2600000);
        queryLessRange.put("Bitcoin-price", 170000000);
        queryLessRange.put("City-temp", 700);
        queryLessRange.put("Dewpoint-temp", 9600);
        queryLessRange.put("IR-bio-temp", -200);
        queryLessRange.put("PM10-dust", 2000);
        queryLessRange.put("Stocks-DE", 90000);
        queryLessRange.put("Stocks-UK", 30000);
        queryLessRange.put("Stocks-USA", 6000);
        queryLessRange.put("Wind-Speed", 60);
        queryLessRange.put("Wine-Tasting", 10);
        queryLessRange.put("Arade4", 12000000);
        queryLessRange.put("EPM-Education", 300);
        queryLessRange.put("POI-lat", 1);
        queryLessRange.put("Gov10", 120000);

        int repeatTime = 500;

        // repeatTime = 200;
        // repeatTime = 1;

        for (int beta : beta_list) {
            // String outputPath = output_parent_dir + "subcolumn_query_count_beta_" + beta
            // + ".csv";
            // String outputPath = output_parent_dir + "subcolumn_query_max_beta_" + beta + ".csv";
            // String outputPath = output_parent_dir + "subcolumn_query_equal_beta_" + beta + ".csv";
            // String outputPath = output_parent_dir + "subcolumn_query_greater_beta_" + beta + ".csv";
            // String outputPath = output_parent_dir + "subcolumn_query_greater_new_beta_" + beta + ".csv";
            // String outputPath = output_parent_dir + "subcolumn_query_less_beta_" + beta + ".csv";
            // String outputPath = output_parent_dir + "subcolumn_query_less_new_beta_" + beta + ".csv";
            // String outputPath = output_parent_dir + "subcolumn_query_equal_new_beta_" + beta + ".csv";
            // String outputPath = output_parent_dir + "subcolumn_query_count_new_beta_" + beta + ".csv";
            // String outputPath = output_parent_dir + "subcolumn_query_max_new_beta_" + beta + ".csv";
            // String outputPath = output_parent_dir + "subcolumn_query_sum2_beta_" + beta + ".csv";
            // String outputPath = output_parent_dir + "subcolumn_query_count2_beta_" + beta + ".csv";
            String outputPath = output_parent_dir + "subcolumn_query_count3_beta_" + beta + ".csv";

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
                byte[] encoded_result = new byte[data2_arr.length * 8];

                long encodeTime = 0;
                long decodeTime = 0;
                double ratio = 0;
                double compressed_size = 0;

                int length = 0;

                long s = System.nanoTime();
                for (int repeat = 0; repeat < repeatTime; repeat++) {
                    length = SubcolumnBetaTest.Encoder(data2_arr, block_size, encoded_result, beta);
                }

                long e = System.nanoTime();
                encodeTime += ((e - s) / repeatTime);
                compressed_size += length;

                double ratioTmp;

                ratioTmp = compressed_size / (double) (data1.size() * Long.BYTES);

                ratio += ratioTmp;

                System.out.println("Query");

                s = System.nanoTime();

                for (int repeat = 0; repeat < repeatTime; repeat++) {
                    // SubcolumnQueryCountTest.Query(encoded_result, queryRange.get(datasetName));
                    // SubcolumnQueryCountNewTest.Query(encoded_result, queryRange.get(datasetName));
                    // SubcolumnQueryMaxTest.Query(encoded_result);
                    // SubcolumnQueryGreaterTest.Query(encoded_result, queryRange.get(datasetName));
                    // SubcolumnQueryGreaterNewTest.Query(encoded_result, queryRange.get(datasetName));
                    // SubcolumnQueryEqualTest.Query(encoded_result, queryRange.get(datasetName));
                    // SubcolumnQueryLessTest.Query(encoded_result, queryRange.get(datasetName));
                    // SubcolumnQueryLessNewTest.Query(encoded_result, queryRange.get(datasetName));
                    // SubcolumnQueryEqualNewTest.Query(encoded_result, queryRange.get(datasetName));
                    // SubcolumnQueryMaxNewTest.Query(encoded_result);
                    // SubcolumnQueryGreaterLessTest.Query(encoded_result, queryRange.get(datasetName),
                    //         queryLessRange.get(datasetName));
                    // SubcolumnQueryCount2Test.Query(encoded_result);
                    SubcolumnQueryCount3Test.Query(encoded_result);
                    // SubcolumnQuerySum2Test.Query(encoded_result, queryRange.get(datasetName));

                }

                e = System.nanoTime();
                decodeTime += ((e - s) / repeatTime);

                String[] record = {
                        datasetName,
                        "Sub-column",
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

    // long block size
    @Test
    public void test3() throws IOException {
        String parent_dir = "D:/github/xjz17/subcolumn/";

        String input_parent_dir = parent_dir + "dataset/";

        String output_parent_dir = "D:/encoding-subcolumn/result/query_vs_block/";
        // String output_parent_dir = parent_dir + "result/query_vs_block/";

        // int[] block_size_list = { 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192 };

        int[] block_size_list = { 512 };

        HashMap<String, Integer> queryRange = new HashMap<>();

        queryRange.put("Bird-migration", 2500000);
        queryRange.put("Bitcoin-price", 160000000);
        queryRange.put("City-temp", 480);
        queryRange.put("Dewpoint-temp", 9500);
        queryRange.put("IR-bio-temp", -300);
        queryRange.put("PM10-dust", 1000);
        queryRange.put("Stocks-DE", 40000);
        queryRange.put("Stocks-UK", 20000);
        queryRange.put("Stocks-USA", 5000);
        queryRange.put("Wind-Speed", 50);
        queryRange.put("Wine-Tasting", 0);
        queryRange.put("Arade4", 10000000);
        queryRange.put("EPM-Education", 200);
        queryRange.put("POI-lat", 0);
        queryRange.put("Gov10", 100000);

        int repeatTime = 100;

        repeatTime = 500;

        // repeatTime = 1;

        for (int block_size : block_size_list) {
            // String outputPath = output_parent_dir + "subcolumn_query_count_block_" +
            // block_size + ".csv";
            String outputPath = output_parent_dir + "subcolumn_long_query_count_block_" + block_size + ".csv";

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
                    length = SubcolumnLongTest.Encoder(data2_arr, block_size, encoded_result);
                }

                long e = System.nanoTime();
                encodeTime += ((e - s) / repeatTime);
                compressed_size += length;

                double ratioTmp;

                ratioTmp = compressed_size / (double) (data1.size() * Long.BYTES);

                ratio += ratioTmp;

                System.out.println("Query");

                s = System.nanoTime();

                for (int repeat = 0; repeat < repeatTime; repeat++) {
                    SubcolumnLongQueryCountTest.Query(encoded_result, queryRange.get(datasetName));
                }

                e = System.nanoTime();
                decodeTime += ((e - s) / repeatTime);

                String[] record = {
                        datasetName,
                        "Sub-column",
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

    // long beta
    @Test
    public void test4() throws IOException {
        String parent_dir = "D:/github/xjz17/subcolumn/";

        String input_parent_dir = parent_dir + "dataset/";

        String output_parent_dir = "D:/encoding-subcolumn/result/query_vs_beta/";
        // String output_parent_dir = parent_dir + "result/query_vs_beta/";

        // int[] beta_list = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
        // 17, 18, 19, 20, 21, 22, 23,
        // 24, 25, 26, 27, 28, 29, 30, 31 };

        int[] beta_list = { 3, 4, 5, 6, 7 };

        int block_size = 512;

        HashMap<String, Integer> queryRange = new HashMap<>();

        queryRange.put("Bird-migration", 2500000);
        queryRange.put("Bitcoin-price", 160000000);
        queryRange.put("City-temp", 480);
        queryRange.put("Dewpoint-temp", 9500);
        queryRange.put("IR-bio-temp", -300);
        queryRange.put("PM10-dust", 1000);
        queryRange.put("Stocks-DE", 40000);
        queryRange.put("Stocks-UK", 20000);
        queryRange.put("Stocks-USA", 5000);
        queryRange.put("Wind-Speed", 50);
        queryRange.put("Wine-Tasting", 0);
        queryRange.put("Arade4", 10000000);
        queryRange.put("EPM-Education", 200);
        queryRange.put("POI-lat", 0);
        queryRange.put("Gov10", 100000);

        int repeatTime = 200;

        repeatTime = 500;

        // repeatTime = 1;

        for (int beta : beta_list) {
            // String outputPath = output_parent_dir + "subcolumn_long_query_count_beta_" + beta
            // + ".csv";
            // String outputPath = output_parent_dir + "subcolumn_long_query_max_beta_" + beta + ".csv";
            String outputPath = output_parent_dir + "subcolumn_long_query_sum2_beta_" + beta + ".csv";

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
                    length = SubcolumnLongBetaTest.Encoder(data2_arr, block_size, encoded_result, beta);
                }

                long e = System.nanoTime();
                encodeTime += ((e - s) / repeatTime);
                compressed_size += length;

                double ratioTmp;

                ratioTmp = compressed_size / (double) (data1.size() * Long.BYTES);

                ratio += ratioTmp;

                System.out.println("Query");

                s = System.nanoTime();

                for (int repeat = 0; repeat < repeatTime; repeat++) {
                    // SubcolumnLongQueryCountTest.Query(encoded_result, queryRange.get(datasetName));
                    // SubcolumnLongQueryEqualTest.Query(encoded_result, queryRange.get(datasetName));
                    SubcolumnLongQuerySum2Test.Query(encoded_result, queryRange.get(datasetName));
                }

                e = System.nanoTime();
                decodeTime += ((e - s) / repeatTime);

                String[] record = {
                        datasetName,
                        "Sub-column",
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
    public void testParts() throws IOException {
        String parent_dir = "D:/github/xjz17/subcolumn/";

        String input_parent_dir = parent_dir + "dataset/";
        
        String output_parent_dir = "D:/encoding-subcolumn/result/query_vs_beta/";
        // String output_parent_dir = parent_dir + "result/query_vs_beta/";

        // int[] beta_list = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23,
        //     24, 25, 26, 27, 28, 29, 30, 31 };

        int[] beta_list = { 3, 4, 5, 6, 7 };
        
        int block_size = 512;

        List<String> integerDatasets = new ArrayList<>();
        integerDatasets.add("Wine-Tasting");

        HashMap<String, Integer> queryRange = new HashMap<>();

        queryRange.put("Bird-migration", 2500000);
        queryRange.put("Bitcoin-price", 160000000);
        queryRange.put("City-temp", 480);
        queryRange.put("Dewpoint-temp", 9500);
        queryRange.put("IR-bio-temp", -300);
        queryRange.put("PM10-dust", 1000);
        queryRange.put("Stocks-DE", 40000);
        queryRange.put("Stocks-UK", 20000);
        queryRange.put("Stocks-USA", 5000);
        queryRange.put("Wind-Speed", 50);
        queryRange.put("Wine-Tasting", 0);
        queryRange.put("Arade4", 10000000);
        queryRange.put("EPM-Education", 200);
        queryRange.put("POI-lat", 0);
        queryRange.put("Gov10", 100000);

        int repeatTime = 500;

        // repeatTime = 1;

        for (int beta : beta_list) {
            // String outputPath = output_parent_dir + "subcolumn_query_less_parts_beta_" + beta + ".csv";
            String outputPath = output_parent_dir + "subcolumn_query_less_parts_new_beta_" + beta + ".csv";

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

                byte[] encoded_result1 = new byte[col1_data.length * 8];
                byte[] encoded_result2 = new byte[col2_data.length * 8];

                long encodeTime = 0;
                long decodeTime = 0;
                double ratio = 0;
                double compressed_size = 0;

                int length1 = 0;
                int length2 = 0;

                // 编码第一列
                long s = System.nanoTime();
                for (int repeat = 0; repeat < repeatTime; repeat++) {
                    length1 = SubcolumnBetaTest.Encoder(col1_data, block_size, encoded_result1, beta);
                }
                long e = System.nanoTime();
                encodeTime += ((e - s) / repeatTime);

                // 编码第二列
                s = System.nanoTime();
                for (int repeat = 0; repeat < repeatTime; repeat++) {
                    length2 = SubcolumnBetaTest.Encoder(col2_data, block_size, encoded_result2, beta);
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
                    // SubcolumnQueryLessPartsTest.QueryTwoColumns(encoded_result1, encoded_result2,
                    //         queryRange.get(datasetName), queryRange.get(datasetName));
                    SubcolumnQueryLessPartsNewTest.QueryTwoColumns(encoded_result1, encoded_result2,
                            queryRange.get(datasetName), queryRange.get(datasetName));
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
