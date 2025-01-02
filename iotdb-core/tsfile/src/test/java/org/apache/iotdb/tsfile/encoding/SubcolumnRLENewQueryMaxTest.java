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

public class SubcolumnRLENewQueryMaxTest {
    // SubcolumnByteRLENewTest Query Max

    public static void Query(byte[] encoded_result) {

        int startBitPosition = 0;
        int data_length = SubcolumnByteRLENewTest.bytesToInt(encoded_result, startBitPosition, 32);
        startBitPosition += 32;

        int block_size = SubcolumnByteRLENewTest.bytesToInt(encoded_result, startBitPosition, 32);
        startBitPosition += 32;

        int num_blocks = data_length / block_size;

        // 查询结果
        int[] result = new int[data_length];
        int[] result_length = new int[1];

        for (int i = 0; i < num_blocks; i++) {
            startBitPosition = BlockQueryMax(encoded_result, i, block_size, block_size, startBitPosition, result,
                    result_length);
        }

        int remainder = data_length % block_size;

        if (remainder <= 3) {
            for (int i = 0; i < remainder; i++) {
                int value = SubcolumnByteRLENewTest.bytesToIntSigned(encoded_result, startBitPosition, 32);
                result[result_length[0]] = value;
                result_length[0]++;
                startBitPosition += 32;
            }
        } else {
            startBitPosition = BlockQueryMax(encoded_result, num_blocks, block_size, remainder, startBitPosition,
                    result, result_length);
        }

        // for (int i = 0; i < result_length[0]; i++) {
        // System.out.print(result[i] + " ");
        // }
        // System.out.println();

    }

    public static int BlockQueryMax(byte[] encoded_result, int block_index, int block_size, int remainder,
            int startBitPosition, int[] result, int[] result_length) {
        int[] min_delta = new int[3];

        min_delta[0] = SubcolumnByteRLENewTest.bytesToIntSigned(encoded_result, startBitPosition, 32);
        startBitPosition += 32;

        int m = SubcolumnByteRLENewTest.bytesToInt(encoded_result, startBitPosition, 6);
        startBitPosition += 6;

        int[] candidate_indices = new int[remainder];
        int candidate_length = 0;
        for (int i = 0; i < remainder; i++) {
            candidate_indices[i] = i;
            candidate_length++;
        }

        if (m == 0) {
            result[result_length[0]] = min_delta[0];
            result_length[0]++;
            return startBitPosition;
        }

        byte bw = SubcolumnByteRLENewTest.bitWidthByte(block_size);

        byte beta = SubcolumnByteRLENewTest.bytesToByte(encoded_result, startBitPosition, 6);
        startBitPosition += 6;

        int l = (m + beta - 1) / beta;

        byte[] bitWidthList = SubcolumnByteRLENewTest.bitUnpackingByte(encoded_result, startBitPosition, 8, l);
        startBitPosition += 8 * l;

        byte[][] subcolumnList = new byte[l][remainder];

        for (int i = l - 1; i >= 0; i--) {
            boolean type = SubcolumnByteRLENewTest.bytesToBool(encoded_result, startBitPosition);
            startBitPosition += 1;
            if (!type) {
                startBitPosition += bitWidthList[i] * remainder;

                if (candidate_length == 1) {
                    continue;
                }

                byte maxPart = 0;

                int new_length = 0;
                for (int j = 0; j < candidate_length; j++) {
                    int index = candidate_indices[j];
                    subcolumnList[i][index] = SubcolumnByteRLENewTest.bytesToByte(encoded_result,
                            startBitPosition + index * bitWidthList[i], bitWidthList[i]);

                    if (subcolumnList[i][index] > maxPart) {
                        maxPart = subcolumnList[i][index];

                        new_length = 0;
                        candidate_indices[new_length] = index;
                        new_length++;
                        } else if (subcolumnList[i][index] == maxPart) {
                        candidate_indices[new_length] = index;
                        new_length++;
                    }
                }

                // for (int j = 0; j < candidate_length; j++) {
                //     int index = candidate_indices[j];
                //     if (subcolumnList[i][index] == maxPart) {
                //         candidate_indices[new_length] = index;
                //         new_length++;
                //     }
                // }

                candidate_length = new_length;

            } else {
                int index = SubcolumnByteRLENewTest.bytesToInt(encoded_result, startBitPosition, 16);
                startBitPosition += 16;

                if (candidate_length == 1) {
                    startBitPosition += bw * index;
                    startBitPosition += bitWidthList[i] * index;
                    continue;
                }

                int[] run_length = SubcolumnByteRLENewTest.bitUnpacking(encoded_result, startBitPosition, bw, index);
                startBitPosition += bw * index;

                byte[] rle_values = SubcolumnByteRLENewTest.bitUnpackingByte(encoded_result, startBitPosition, bitWidthList[i],
                        index);
                startBitPosition += bitWidthList[i] * index;

                int maxPart = 0;

                int new_length = 0;
                int rleIndex = 0;
                int currentPos = 0;

                for (int j = 0; j < candidate_length; j++) {
                    int index_candidate = candidate_indices[j];

                    while (rleIndex < index && currentPos + run_length[rleIndex] <= index_candidate) {
                        currentPos += run_length[rleIndex];
                        rleIndex++;
                    }

                    if (rleIndex < index) {
                        if (rle_values[rleIndex] > maxPart) {
                            maxPart = rle_values[rleIndex];

                            new_length = 0;
                            } else if (rle_values[rleIndex] == maxPart) {
                            candidate_indices[new_length] = index_candidate;
                            new_length++;
                        }
                    }
                }

                // for (int j = 0; j < candidate_length; j++) {
                //     int index_candidate = candidate_indices[j];

                //     while (rleIndex < index && currentPos + run_length[rleIndex] <= index_candidate) {
                //         currentPos += run_length[rleIndex];
                //         rleIndex++;
                //     }

                //     if (rleIndex < index) {
                //         if (rle_values[rleIndex] == maxPart) {
                //             candidate_indices[new_length] = index_candidate;
                //             new_length++;
                //         }
                //     }
                // }

                candidate_length = new_length;
            }
        }

        result[result_length[0]] = candidate_indices[0];
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
        // String parent_dir = "D:/github/xjz17/subcolumn/elf_resources/dataset/";
        String parent_dir = "D:/compress-subcolumn/dataset/";

        String output_parent_dir = "D:/compress-subcolumn/";

        String outputPath = output_parent_dir + "test01.csv";

        // int block_size = 1024;
        int block_size = 512;

        int repeatTime = 100;
        // TODO 真正计算时，记得注释掉将下面的内容
        repeatTime = 1;

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
                length = SubcolumnByteRLENewTest.Encoder(data2_arr, block_size, encoded_result);
            }

            long e = System.nanoTime();
            encodeTime += ((e - s) / repeatTime);
            compressed_size += length / 8;
            double ratioTmp = compressed_size / (double) (data1.size() * Long.BYTES);
            ratio += ratioTmp;

            System.out.println("Query");

            s = System.nanoTime();

            for (int repeat = 0; repeat < repeatTime; repeat++) {
                Query(encoded_result);
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
