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

public class Subcolumn5BlockSizeTest {
    // Subcolumn5Test 测试不同 block size

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
        String parent_dir = "D:/github/xjz17/subcolumn/dataset/";
        // String parent_dir = "D:/encoding-subcolumn/dataset/";

        String output_parent_dir = "D:/encoding-subcolumn/result/compression_vs_block/";

        int[] block_size_list = { 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192 };

        int repeatTime = 100;

        // repeatTime = 1;

        List<String> integerDatasets = new ArrayList<>();
        integerDatasets.add("Wine-Tasting");

        for (int block_size : block_size_list) {

            String outputPath = output_parent_dir + "subcolumn_block_" + block_size + ".csv";

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
                    data2_arr_decoded = Subcolumn5Test.Decoder(encoded_result);
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

    @Test
    public void testTransData() throws IOException {
        // String parent_dir = "D:/github/xjz17/subcolumn/";
        String parent_dir = "D:/encoding-subcolumn/";

        String output_parent_dir = "D:/encoding-subcolumn/trans_data_result/compression_vs_block/";

        int[] block_size_list = { 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192 };

        String input_parent_dir = parent_dir + "trans_data/";

        ArrayList<String> input_path_list = new ArrayList<>();
        ArrayList<String> dataset_name = new ArrayList<>();
        ArrayList<Integer> dataset_block_size = new ArrayList<>();

        try (Stream<Path> paths = Files.walk(Paths.get(input_parent_dir))) {
            paths.filter(Files::isDirectory)
                    .filter(path -> !path.equals(Paths.get(input_parent_dir)))
                    .forEach(dir -> {
                        String name = dir.getFileName().toString();
                        dataset_name.add(name);
                        input_path_list.add(dir.toString());
                        dataset_block_size.add(1024); // Default block size, can be changed if needed
                    });
        }

        for (int block_size : block_size_list) {

            String outputPath = output_parent_dir + "subcolumn_trans_data_block_" + block_size + ".csv";
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
                        data1.add(Integer.valueOf(loader.getValues()[0]));
                        data2.add(Integer.valueOf(loader.getValues()[1]));
                    }
                    inputStream.close();
                    int[] data2_arr = new int[data2.size()];
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
                        length = Subcolumn5Test.Encoder(data2_arr, block_size, encoded_result);
                    }

                    long e = System.nanoTime();
                    encodeTime += ((e - s) / repeatTime);
                    compressed_size += length;
                    double ratioTmp = compressed_size / (double) (data1.size() * Integer.BYTES);
                    ratio += ratioTmp;
                    s = System.nanoTime();

                    int[] data2_arr_decoded = new int[data1.size()];

                    for (int repeat = 0; repeat < repeatTime; repeat++) {
                        data2_arr_decoded = Subcolumn5Test.Decoder(encoded_result);
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

                System.out.println("Block size: " + block_size);
            }

            writer.close();
        }
    }
}
