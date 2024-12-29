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

public class SubcolumnQuerySumTest {
    // SubcolumnByteTest Query Sum

    public static void Query(byte[] encoded_result, int lower_bound, int upper_bound) {

        int startBitPosition = 0;
        int data_length = SubcolumnByteTest.bytesToInt(encoded_result, startBitPosition, 32);
        startBitPosition += 32;

        int block_size = SubcolumnByteTest.bytesToInt(encoded_result, startBitPosition, 32);
        startBitPosition += 32;

        int num_blocks = data_length / block_size;

        // 查询结果
        int[] result = new int[data_length];
        int[] result_length = new int[1];

        for (int i = 0; i < num_blocks; i++) {
            startBitPosition = BlockQuerySum(encoded_result, i, block_size, block_size, startBitPosition, result,
                    result_length);
        }

        int remainder = data_length % block_size;

        if (remainder <= 3) {
            for (int i = 0; i < remainder; i++) {
                int value = SubcolumnByteTest.bytesToIntSigned(encoded_result, startBitPosition, 32);
                if (value >= lower_bound && value <= upper_bound) {
                    result[result_length[0]] = value;
                    result_length[0]++;
                }
                startBitPosition += 32;
            }
        } else {
            startBitPosition = BlockQuerySum(encoded_result, num_blocks, block_size, remainder, startBitPosition,
                    result, result_length);
        }

        // for (int i = 0; i < result_length[0]; i++) {
        // System.out.print(result[i] + " ");
        // }
        // System.out.println();

    }

    public static int BlockQuerySum(byte[] encoded_result, int block_index, int block_size, int remainder,
            int startBitPosition, int[] result, int[] result_length) {
        int[] min_delta = new int[3];

        min_delta[0] = SubcolumnByteTest.bytesToIntSigned(encoded_result, startBitPosition, 32);
        startBitPosition += 32;

        int m = SubcolumnByteTest.bytesToInt(encoded_result, startBitPosition, 6);
        startBitPosition += 6;

        if (m == 0) {
            result[result_length[0]] = min_delta[0];
            result_length[0]++;
            return startBitPosition;
        }

        int beta = SubcolumnByteTest.bytesToInt(encoded_result, startBitPosition, 6);
        startBitPosition += 6;

        int l = (m + beta - 1) / beta;

        int[] bitWidthList = SubcolumnByteTest.bitUnpacking(encoded_result, startBitPosition, 8, l);
        startBitPosition += 8 * l;

        for (int i = l - 1; i >= 0; i--) {
            boolean type = SubcolumnByteTest.bytesToBool(encoded_result, startBitPosition);
            startBitPosition += 1;
            if (!type) {

                for (int j = 0; j < remainder; j++) {
                    int value = SubcolumnByteTest.bytesToInt(encoded_result, startBitPosition + j * bitWidthList[i], bitWidthList[i]);
                    result[result_length[0]] += value << (i * beta);
                }

                startBitPosition += bitWidthList[i] * remainder;


            } else {
                int index = SubcolumnByteTest.bytesToInt(encoded_result, startBitPosition, 16);
                startBitPosition += 16;

                int[] run_length = SubcolumnByteTest.bitUnpacking(encoded_result, startBitPosition, 8, index);
                startBitPosition += 8 * index;

                int[] rle_values = SubcolumnByteTest.bitUnpacking(encoded_result, startBitPosition, bitWidthList[i], index);
                startBitPosition += bitWidthList[i] * index;

                for (int j = 0; j < index; j++) {
                    result[result_length[0]] += rle_values[j] << (i * beta);
                }
            }
        }

        result_length[0]++;

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
    public void testQuery() throws IOException {
        String parent_dir = "D:/github/xjz17/subcolumn/elf_resources/dataset/";
        // String parent_dir = "D:/compress-subcolumn/dataset/";

        String output_parent_dir = "D:/compress-subcolumn/";

        String outputPath = output_parent_dir + "test_byte_query_sum.csv";

        // int block_size = 1024;
        int block_size = 512;

        HashMap<String, int[]> queryRange = new HashMap<>();
        queryRange.put("Air-pressure", new int[] { 8850000, 8855000 });
        queryRange.put("Bird-migration", new int[] { 2500000, 2600000 });
        queryRange.put("Bitcoin-price", new int[] { 170000000, 172000000 });
        queryRange.put("Blockchain-tr", new int[] { 100000, 300000 });
        queryRange.put("City-temp", new int[] { 600, 700 });
        queryRange.put("Dewpoint-temp", new int[] { 9500, 9600 });
        queryRange.put("IR-bio-temp", new int[] { -300, -200 });
        queryRange.put("PM10-dust", new int[] { 1000, 2000 });
        queryRange.put("Stocks-DE", new int[] { 40000, 50000 });
        queryRange.put("Stocks-UK", new int[] { 20000, 30000 });
        queryRange.put("Stocks-USA", new int[] { 5000, 6000 });
        queryRange.put("Wind-Speed", new int[] { 30, 40 });

        int repeatTime = 100;
        // TODO 真正计算时，记得注释掉将下面的内容
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
                length = SubcolumnByteTest.Encoder(data2_arr, block_size, encoded_result);
            }

            long e = System.nanoTime();
            encodeTime += ((e - s) / repeatTime);
            compressed_size += length / 8;
            double ratioTmp = compressed_size / (double) (data1.size() * Long.BYTES);
            ratio += ratioTmp;

            System.out.println("Query");

            s = System.nanoTime();

            for (int repeat = 0; repeat < repeatTime; repeat++) {
                Query(encoded_result, queryRange.get(datasetName)[0],
                        queryRange.get(datasetName)[1]);
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
            System.out.println(ratio);
        }

        writer.close();
    }
}
