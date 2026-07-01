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

public class SubcolumnLongPSOBlockSizeTest {

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

        // String output_parent_dir =
        // "D:/encoding-subcolumn/result/compression_vs_block_pso/";
        String output_parent_dir = parent_dir + "result/compression_vs_block_pso/";

        File outputDir = new File(output_parent_dir);
        if (!outputDir.exists()) {
            outputDir.mkdirs();
        }

        int[] block_size_list = { 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192 };

        int repeatTime = 100;
        repeatTime = 600;

        // repeatTime = 1;

        // 定义数据集名称列表
        List<String> datasetList = new ArrayList<>();
        datasetList.add("Arade4");
        datasetList.add("Bird-migration");
        datasetList.add("Bitcoin-price");
        datasetList.add("City-temp");
        datasetList.add("Dewpoint-temp");
        datasetList.add("EPM-Education");
        datasetList.add("Gov10");
        // datasetList.add("POI-lat");
        datasetList.add("IR-bio-temp");
        datasetList.add("PM10-dust");
        datasetList.add("Stocks-DE");
        datasetList.add("Stocks-UK");
        datasetList.add("Stocks-USA");
        datasetList.add("Wind-Speed");
        datasetList.add("Wine-Tasting");

        for (int block_size : block_size_list) {

            String outputPath = output_parent_dir + "subcolumn_pso_block_" + block_size + ".csv";

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

            // 使用数据集名称列表循环
            for (String datasetName : datasetList) {
                String filePath = input_parent_dir + datasetName + ".csv";
                File file = new File(filePath);

                // 检查文件是否存在
                if (!file.exists()) {
                    System.out.println("File not found: " + filePath);
                    continue;
                }

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
                byte[] encoded_result = new byte[data2_arr.length * 13];

                long encodeTime = 0;
                long decodeTime = 0;
                double ratio = 0;
                double compressed_size = 0;

                int length = 0;

                long s = System.nanoTime();
                for (int repeat = 0; repeat < repeatTime; repeat++) {
                    length = SubcolumnLongPSOTest.Encoder(data2_arr, block_size, encoded_result);
                }

                long e = System.nanoTime();
                encodeTime += ((e - s) / repeatTime);
                compressed_size += length;

                double ratioTmp;

                ratioTmp = compressed_size / (double) (data1.size() * Long.BYTES);

                ratio += ratioTmp;

                System.out.println("Decode");

                s = System.nanoTime();

                long[] data2_arr_decoded = new long[data2_arr.length];

                for (int repeat = 0; repeat < repeatTime; repeat++) {
                    data2_arr_decoded = SubcolumnLongPSOTest.Decoder(encoded_result);
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
                System.out.println("Block size: " + block_size);
                System.out.println(ratio);
            }

            writer.close();
        }
    }

}
