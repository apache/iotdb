package org.apache.iotdb.tsfile.encoding;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;

public class VBPQueryMain {

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
    public void test0() throws IOException {
        String parent_dir = "D:/github/xjz17/subcolumn/";
        // String parent_dir = "D:/encoding-subcolumn/";

        String input_parent_dir = parent_dir + "dataset/";

        String output_parent_dir = "D:/encoding-subcolumn/result/vbp_query/";

        // String output_parent_dir = parent_dir + "result/";
        // String outputPath = output_parent_dir + "vbp_query.csv";

        // String parent_dir = "/Users/xiaojinzhao/Documents/GitHub/subcolumn/";
        // //"D:/github/xjz17/subcolumn/";
        // String parent_dir = "D:/encoding-subcolumn/";

        // String input_parent_dir = parent_dir + "dataset/";

        // String output_parent_dir = parent_dir + "result/vbp_query/";

        // String outputPath = output_parent_dir + "vbp_query_greater_new.csv";
        // String outputPath = output_parent_dir + "vbp_query_less_new.csv";
        // String outputPath = output_parent_dir + "vbp_query_equal_new.csv";
        // String outputPath = output_parent_dir + "vbp_query_greater_less.csv";
        // String outputPath = output_parent_dir + "vbp_query_greater_less_new.csv";
        // String outputPath = output_parent_dir + "vbp_query_count.csv";
        String outputPath = output_parent_dir + "vbp_query_count2.csv";
        // String outputPath = output_parent_dir + "vbp_query_max.csv";
        // String outputPath = output_parent_dir + "vbp_query_sum.csv";

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

        int block_size = 512;

        int repeatTime = 100;
        // repeatTime = 500;

        // repeatTime = 1;

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
            if (!queryRange.containsKey(datasetName)) {
                continue;
            }

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

            // test
            // for (int i = 0; i < data2_arr.length; i++) {
            // System.out.print(data2_arr[i] + " ");
            // }
            // System.out.println();

            System.out.println(max_decimal);
            byte[] encoded_result = new byte[data2_arr.length * 8];

            long encodeTime = 0;
            long decodeTime = 0;
            double ratio = 0;
            double compressed_size = 0;

            int length = 0;

            ArrayList<VBPIndexLong> indexList = new ArrayList<>();

            long s = System.nanoTime();
            for (int repeat = 0; repeat < repeatTime; repeat++) {
                // clear indexList
                indexList.clear();

                length = VBPIndexLongTest.Encoder(data2_arr, block_size, indexList, encoded_result);
            }

            long e = System.nanoTime();
            encodeTime += ((e - s) / repeatTime);
            compressed_size += length;

            for (VBPIndexLong idx : indexList) {
                compressed_size += idx.k * idx.wordsPerPlane * Long.BYTES;
            }

            double ratioTmp;

            ratioTmp = compressed_size / (double) (data1.size() * Long.BYTES);

            ratio += ratioTmp;

            System.out.println("Decode");

            // long[] data2_arr_decoded = new long[data2_arr.length];

            s = System.nanoTime();

            for (int repeat = 0; repeat < repeatTime; repeat++) {
                // VBPQueryGreaterTest.Decoder(encoded_result, indexList, queryRange.get(datasetName));
                // VBPQuerySmallerTest.Decoder(encoded_result, indexList, queryRange.get(datasetName));
                // VBPQueryEqualTest.Decoder(encoded_result, indexList, queryRange.get(datasetName));
                // VBPQueryGreaterLessTest.Decoder(encoded_result, indexList, queryRange.get(datasetName), queryLessRange.get(datasetName));
                // VBPQueryCountTest.Decoder(encoded_result, indexList, queryRange.get(datasetName));
                VBPQueryCount2Test.Decoder(encoded_result, indexList);
                // VBPQueryMaxTest.Decoder(encoded_result, indexList);
                // VBPQuerySumTest.Decoder(encoded_result, indexList);
            }

            e = System.nanoTime();
            decodeTime += ((e - s) / repeatTime);

            String[] record = {
                    datasetName,
                    "VBP",
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
    public void testParts() throws IOException {
        String parent_dir = "D:/github/xjz17/subcolumn/";
        // // String parent_dir = "D:/encoding-subcolumn/";
        //
        String input_parent_dir = parent_dir + "dataset/";
        //
        String output_parent_dir = "D:/encoding-subcolumn/result/vbp_query/";
        // // String output_parent_dir = parent_dir + "result/vbp_query/";

        // String parent_dir = "/Users/xiaojinzhao/Documents/GitHub/subcolumn/";
        // //"D:/github/xjz17/subcolumn/";
        // String parent_dir = "D:/encoding-subcolumn/";

        // String input_parent_dir = parent_dir + "dataset/";

        // String output_parent_dir = parent_dir + "result/vbp_query/";

        String outputPath = output_parent_dir + "vbp_query_less_parts_new.csv";

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

        int block_size = 512;

        int repeatTime = 100;
        // repeatTime = 500;

        // repeatTime = 1;

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
            if (!queryRange.containsKey(datasetName)) {
                continue;
            }

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

            // long[] data2_arr = new long[data1.size()];
            int totalSize = data1.size();
            int halfSize = totalSize / 2;

            long[] col1_data = new long[halfSize];
            long[] col2_data = new long[halfSize];

            long max_mul = (long) Math.pow(10, max_decimal);
            for (int i = 0; i < halfSize; i++) {
                col1_data[i] = (long) (data1.get(i) * max_mul);
            }

            for (int i = 0; i < halfSize; i++) {
                col2_data[i] = (long) (data1.get(i + halfSize) * max_mul);
            }

            System.out.println(max_decimal);

            byte[] encoded_result1 = new byte[col1_data.length * 8];
            byte[] encoded_result2 = new byte[col2_data.length * 8];

            long encodeTime = 0;
            long decodeTime = 0;
            double ratio = 0;
            double compressed_size = 0;

            int length = 0;

            ArrayList<VBPIndexLong> indexList1 = new ArrayList<>();
            ArrayList<VBPIndexLong> indexList2 = new ArrayList<>();

            long s = System.nanoTime();
            for (int repeat = 0; repeat < repeatTime; repeat++) {
                // clear indexList
                indexList1.clear();
                indexList2.clear();

                length = VBPIndexLongTest.Encoder(col1_data, block_size, indexList1, encoded_result1);

                length = VBPIndexLongTest.Encoder(col2_data, block_size, indexList2, encoded_result2);
            }

            long e = System.nanoTime();
            encodeTime += ((e - s) / repeatTime);
            compressed_size += length;

            for (VBPIndexLong idx : indexList1) {
                compressed_size += idx.k * idx.wordsPerPlane * Long.BYTES;
            }

            for (VBPIndexLong idx : indexList2) {
                compressed_size += idx.k * idx.wordsPerPlane * Long.BYTES;
            }

            double ratioTmp;

            ratioTmp = compressed_size / (double) (data1.size() * Long.BYTES);

            ratio += ratioTmp;

            System.out.println("Decode");

            // long[] data2_arr_decoded = new long[data2_arr.length];

            s = System.nanoTime();

            for (int repeat = 0; repeat < repeatTime; repeat++) {
                VBPQuerySmallerPartsTest.Decoder(encoded_result1, encoded_result2, indexList1, indexList2, queryRange.get(datasetName));
            }

            e = System.nanoTime();
            decodeTime += ((e - s) / repeatTime);

            String[] record = {
                    datasetName,
                    "VBP",
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
    public void testMaterialize() throws IOException {
        String parent_dir = "D:/github/xjz17/subcolumn/";
        // // String parent_dir = "D:/encoding-subcolumn/";
        //
        String input_parent_dir = parent_dir + "dataset/";
        //
        String output_parent_dir = "D:/encoding-subcolumn/result/materialization/";

        String outputPath = output_parent_dir + "bitweaving_materialization.csv";

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

        int block_size = 512;

        int repeatTime = 100;
        // repeatTime = 500;

        // repeatTime = 1;

        CsvWriter writer = new CsvWriter(outputPath, ',', StandardCharsets.UTF_8);
        writer.setRecordDelimiter('\n');

        String[] head = {
                "Dataset",
                "Encoding Algorithm",
                "Decoding Time",
                "Points",
        };
        writer.writeRecord(head);

        File directory = new File(input_parent_dir);
        // File[] csvFiles = directory.listFiles();
        File[] csvFiles = directory.listFiles((dir, name) -> name.endsWith(".csv"));

        for (File file : csvFiles) {
            String datasetName = extractFileName(file.toString());
            System.out.println(datasetName);
            if (!queryRange.containsKey(datasetName)) {
                continue;
            }

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

            // long[] data2_arr = new long[data1.size()];
            int totalSize = data1.size();
            int halfSize = totalSize / 2;

            long[] col1_data = new long[halfSize];
            long[] col2_data = new long[halfSize];

            long max_mul = (long) Math.pow(10, max_decimal);
            for (int i = 0; i < halfSize; i++) {
                col1_data[i] = (long) (data1.get(i) * max_mul);
            }

            for (int i = 0; i < halfSize; i++) {
                col2_data[i] = (long) (data1.get(i + halfSize) * max_mul);
            }

            System.out.println(max_decimal);

            byte[] encoded_result1 = new byte[col1_data.length * 8];
            byte[] encoded_result2 = new byte[col2_data.length * 8];

            long encodeTime = 0;
            long decodeTime = 0;
            double ratio = 0;
            double compressed_size = 0;

            int length = 0;

            ArrayList<VBPIndexLong> indexList1 = new ArrayList<>();
            ArrayList<VBPIndexLong> indexList2 = new ArrayList<>();

            long s = System.nanoTime();
            for (int repeat = 0; repeat < repeatTime; repeat++) {
                // clear indexList
                indexList1.clear();
                indexList2.clear();

                length = VBPIndexLongTest.Encoder(col1_data, block_size, indexList1, encoded_result1);

                length = VBPIndexLongTest.Encoder(col2_data, block_size, indexList2, encoded_result2);
            }

            long e = System.nanoTime();
            encodeTime += ((e - s) / repeatTime);
            compressed_size += length;

            for (VBPIndexLong idx : indexList1) {
                compressed_size += idx.k * idx.wordsPerPlane * Long.BYTES;
            }

            for (VBPIndexLong idx : indexList2) {
                compressed_size += idx.k * idx.wordsPerPlane * Long.BYTES;
            }

            double ratioTmp;

            ratioTmp = compressed_size / (double) (data1.size() * Long.BYTES);

            ratio += ratioTmp;

            System.out.println("Decode");

            // long[] data2_arr_decoded = new long[data2_arr.length];

            s = System.nanoTime();

            int[] res1 = new int[data1.size()];
            int[] res2 = new int[data1.size()];
            int[] len1 = new int[1];
            int[] len2 = new int[1];

            int[] result = new int[data1.size()];
            int[] result_length = new int[1];

            for (int repeat = 0; repeat < repeatTime; repeat++) {
                VBPMaterializeTest.Decoder(encoded_result1, indexList1, queryRange.get(datasetName), res1, len1);

                VBPMaterializeTest.Decoder(encoded_result2, indexList2, queryRange.get(datasetName), res2, len2);

                int i = 0, j = 0;
                int idx = 0;
                while (i < len1[0] && j < len2[0]) {
                    if (res1[i] == res2[j]) {
                        result[idx] = res1[i];
                        idx++;
                        i++;
                        j++;
                    } else if (res1[i] < res2[j]) {
                        i++;
                    } else {
                        j++;
                    }
                }

            }

            e = System.nanoTime();
            decodeTime += ((e - s) / repeatTime);

            String[] record = {
                    datasetName,
                    "BitWeaving",
                    String.valueOf(decodeTime),
                    String.valueOf(data1.size()),
            };
            writer.writeRecord(record);
            System.out.println(ratio);
        }

        writer.close();
    }


}
