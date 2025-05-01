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

public class Subcolumn5QueryLessPartsTest {
    // Subcolumn5Test Query Less

    public static void QueryTwoColumns(byte[] encoded_result1, byte[] encoded_result2, int upper_bound1,
            int upper_bound2) {
        int[] first_column_results = new int[encoded_result1.length];
        int[] first_result_length = new int[1];

        Query(encoded_result1, upper_bound1, first_column_results, first_result_length);

        int[] final_results = new int[first_result_length[0]];
        int[] final_result_length = new int[1];

        QueryWithIndices(encoded_result2, upper_bound2, first_column_results, first_result_length[0],
                final_results, final_result_length);
    }

    public static void QueryWithIndices(byte[] encoded_result, int upper_bound,
            int[] candidate_indices, int candidate_length,
            int[] result, int[] result_length) {
        int encode_pos = 0;

        int data_length = ((encoded_result[encode_pos] & 0xFF) << 24) | ((encoded_result[encode_pos + 1] & 0xFF) << 16)
                | ((encoded_result[encode_pos + 2] & 0xFF) << 8) | (encoded_result[encode_pos + 3] & 0xFF);
        encode_pos += 4;

        int block_size = ((encoded_result[encode_pos] & 0xFF) << 24) | ((encoded_result[encode_pos + 1] & 0xFF) << 16) |
                ((encoded_result[encode_pos + 2] & 0xFF) << 8) | (encoded_result[encode_pos + 3] & 0xFF);
        encode_pos += 4;

        int num_blocks = data_length / block_size;

        // 初始化结果索引
        result_length[0] = 0;

        int[] blockIndicesCount = new int[num_blocks + 1];

        for (int i = 0; i < candidate_length; i++) {
            int index = candidate_indices[i];
            int blockIndex = index / block_size;
            blockIndicesCount[blockIndex]++;
        }

        int[][] blockIndices = new int[num_blocks + 1][];
        for (int i = 0; i <= num_blocks; i++) {
            blockIndices[i] = new int[blockIndicesCount[i]];
        }

        int[] currentIndices = new int[num_blocks + 1];

        for (int i = 0; i < candidate_length; i++) {
            int index = candidate_indices[i];
            int blockIndex = index / block_size;
            int localIndex = index % block_size;

            blockIndices[blockIndex][currentIndices[blockIndex]] = localIndex;
            currentIndices[blockIndex]++;
        }

        // 遍历所有块
        for (int i = 0; i < num_blocks; i++) {

            if (blockIndicesCount[i] == 0) {
                // 计算跳过此块所需的字节数
                encode_pos = SkipBlock(encoded_result, i, block_size,
                        block_size, encode_pos);
                continue;
            }

            // 对该块中的候选索引执行查询
            encode_pos = BlockQueryWithIndices(encoded_result, i, block_size,
                    block_size, encode_pos, upper_bound,
                    blockIndices[i], blockIndicesCount[i], result, result_length);
        }

        int remainder = data_length % block_size;

        if (remainder > 0) {
            if (blockIndicesCount[num_blocks] > 0) {
                if (remainder <= 3) {
                    for (int j = 0; j < blockIndicesCount[num_blocks]; j++) {
                        int idx = blockIndices[num_blocks][j];
                        int offset = num_blocks * block_size + idx;
                        if (offset < data_length) {
                            int value = ((encoded_result[encode_pos + idx * 4] & 0xFF) << 24) |
                                    ((encoded_result[encode_pos + idx * 4 + 1] & 0xFF) << 16) |
                                    ((encoded_result[encode_pos + idx * 4 + 2] & 0xFF) << 8) |
                                    (encoded_result[encode_pos + idx * 4 + 3] & 0xFF);
                            if (value < upper_bound) {
                                result[result_length[0]] = offset;
                                result_length[0]++;
                            }
                        }
                    }
                    encode_pos += remainder * 4;
                } else {

                    encode_pos = BlockQueryWithIndices(encoded_result, num_blocks, block_size,
                            remainder, encode_pos, upper_bound,
                            blockIndices[num_blocks], blockIndicesCount[num_blocks], result, result_length);
                }
            } else {
                // 没有候选索引，跳过剩余部分
                if (remainder <= 3) {
                    encode_pos += remainder * 4;
                } else {
                    encode_pos = SkipBlock(encoded_result, num_blocks, block_size,
                            remainder, encode_pos);
                }
            }
        }
    }

    public static int BlockQueryWithIndices(byte[] encoded_result, int block_index, int block_size, int remainder,
            int encode_pos, int upper_bound, int[] candidate_indices, int candidate_length,
            int[] result, int[] result_length) {
        int[] min_delta = new int[3];

        min_delta[0] = ((encoded_result[encode_pos] & 0xFF) << 24) | ((encoded_result[encode_pos + 1] & 0xFF) << 16) |
                ((encoded_result[encode_pos + 2] & 0xFF) << 8) | (encoded_result[encode_pos + 3] & 0xFF);
        encode_pos += 4;

        int m = encoded_result[encode_pos];
        encode_pos += 1;

        upper_bound -= min_delta[0];

        // 所有索引默认都是候选索引
        int[] filtered_indices = new int[candidate_length];
        int filtered_length = candidate_length;
        System.arraycopy(candidate_indices, 0, filtered_indices, 0, candidate_length);

        if (m == 0) {
            if (upper_bound > 0) {
                for (int i = 0; i < filtered_length; i++) {
                    result[result_length[0]] = block_size * block_index + filtered_indices[i];
                    result_length[0]++;
                }
            }
            return encode_pos;
        }

        int bw = Subcolumn5Test.bitWidth(block_size);

        int beta = encoded_result[encode_pos];
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
                for (int j = 0; j < filtered_length; j++) {
                    int index = filtered_indices[j];

                    subcolumnList[i][index] = Subcolumn5Test.bytesToInt(encoded_result,
                            encode_pos + index * bitWidthList[i], bitWidthList[i]);
                    int value = (upper_bound >> (i * beta)) & ((1 << beta) - 1);
                    if (subcolumnList[i][index] < value) {
                        result[result_length[0]] = block_size * block_index + index;
                        result_length[0]++;
                    } else if (subcolumnList[i][index] == value) {
                        filtered_indices[new_length] = index;
                        new_length++;
                    }
                }

                filtered_length = new_length;

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
                int value = (upper_bound >> (i * beta)) & ((1 << beta) - 1);

                // 为每个候选索引查找对应的RLE值
                for (int j = 0; j < filtered_length; j++) {
                    int index_candidate = filtered_indices[j];

                    // 查找包含此索引的RLE段
                    int rleIndex = 0;
                    int currentPos = 0;

                    while (rleIndex < index && currentPos + run_length[rleIndex] <= index_candidate) {
                        currentPos += run_length[rleIndex];
                        rleIndex++;
                    }

                    if (rleIndex < index) {
                        if (rle_values[rleIndex] < value) {
                            result[result_length[0]] = block_size * block_index + index_candidate;
                            result_length[0]++;
                        } else if (rle_values[rleIndex] == value) {
                            filtered_indices[new_length] = index_candidate;
                            new_length++;
                        }
                    }
                }

                filtered_length = new_length;
            }
        }

        return encode_pos;
    }

    private static int SkipBlock(byte[] encoded_result, int block_index, int block_size, int remainder,
            int encode_pos) {
        // int[] min_delta = new int[3];

        encode_pos += 4;

        int m = encoded_result[encode_pos];
        encode_pos += 1;

        if (m == 0) {
            return encode_pos;
        }

        int bw = Subcolumn5Test.bitWidth(block_size);

        int beta = encoded_result[encode_pos];
        encode_pos += 1;

        int l = (m + beta - 1) / beta;

        int[] bitWidthList = new int[l];

        encode_pos = Subcolumn5Test.decodeBitPacking(encoded_result, encode_pos, 8, l, bitWidthList);

        int[] encodingType = new int[l];

        encode_pos = Subcolumn5Test.decodeBitPacking(encoded_result, encode_pos, 1, l, encodingType);

        for (int i = l - 1; i >= 0; i--) {
            int type = encodingType[i];

            if (type == 0) {

                encode_pos = (encode_pos * 8 + bitWidthList[i] * remainder + 7) / 8;
            } else {
                int index = ((encoded_result[encode_pos] & 0xFF) << 8) | (encoded_result[encode_pos + 1] & 0xFF);
                encode_pos += 2;

                encode_pos = (encode_pos * 8 + bw * index + 7) / 8;
                encode_pos = (encode_pos * 8 + bitWidthList[i] * index + 7) / 8;
            }
        }

        return encode_pos;
    }

    public static void Query(byte[] encoded_result, int upper_bound, int[] result, int[] result_length) {

        int encode_pos = 0;

        int data_length = ((encoded_result[encode_pos] & 0xFF) << 24) | ((encoded_result[encode_pos + 1] & 0xFF) << 16)
                |
                ((encoded_result[encode_pos + 2] & 0xFF) << 8) | (encoded_result[encode_pos + 3] & 0xFF);
        encode_pos += 4;

        int block_size = ((encoded_result[encode_pos] & 0xFF) << 24) | ((encoded_result[encode_pos + 1] & 0xFF) << 16) |
                ((encoded_result[encode_pos + 2] & 0xFF) << 8) | (encoded_result[encode_pos + 3] & 0xFF);
        encode_pos += 4;

        int num_blocks = data_length / block_size;

        // 查询结果
        // int[] result = new int[data_length];
        // int[] result_length = new int[1];

        result_length[0] = 0;

        for (int i = 0; i < num_blocks; i++) {
            encode_pos = BlockQueryIndex(encoded_result, i, block_size,
                    block_size, encode_pos, upper_bound,
                    result, result_length);
        }

        int remainder = data_length % block_size;

        if (remainder <= 3) {
            for (int i = 0; i < remainder; i++) {
                int value = ((encoded_result[encode_pos] & 0xFF) << 24) |
                        ((encoded_result[encode_pos + 1] & 0xFF) << 16) |
                        ((encoded_result[encode_pos + 2] & 0xFF) << 8) | (encoded_result[encode_pos + 3] & 0xFF);
                if (value < upper_bound) {
                    result[result_length[0]] = value;
                    result_length[0]++;
                }
                encode_pos += 4;
            }
        } else {
            encode_pos = BlockQueryIndex(encoded_result, num_blocks, block_size,
                    remainder, encode_pos, upper_bound,
                    result, result_length);
        }

    }

    public static int BlockQueryIndex(byte[] encoded_result, int block_index, int block_size, int remainder,
            int encode_pos, int upper_bound, int[] result, int[] result_length) {
        int[] min_delta = new int[3];

        min_delta[0] = ((encoded_result[encode_pos] & 0xFF) << 24) | ((encoded_result[encode_pos + 1] & 0xFF) << 16) |
                ((encoded_result[encode_pos + 2] & 0xFF) << 8) | (encoded_result[encode_pos + 3] & 0xFF);
        encode_pos += 4;

        // int[] block_data = new int[remainder];

        int m = encoded_result[encode_pos];
        encode_pos += 1;

        upper_bound -= min_delta[0];

        // 候选索引列表，当前分列值和 upper_bound 相应值相等的索引
        int[] candidate_indices = new int[remainder];
        int candidate_length = 0;
        for (int i = 0; i < remainder; i++) {
            candidate_indices[i] = i;
            candidate_length++;
        }

        if (m == 0) {
            if (upper_bound > 0) {
                for (int i = 0; i < remainder; i++) {
                    result[result_length[0]] = block_size * block_index + i;
                    result_length[0]++;
                }
            }
            return encode_pos;
        }

        int bw = Subcolumn5Test.bitWidth(block_size);

        int beta = encoded_result[encode_pos];
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
                    int value = (upper_bound >> (i * beta)) & ((1 << beta) - 1);
                    if (subcolumnList[i][index] < value) {
                        result[result_length[0]] = block_size * block_index + index;
                        result_length[0]++;
                    } else if (subcolumnList[i][index] == value) {
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
                int value = (upper_bound >> (i * beta)) & ((1 << beta) - 1);

                for (int j = 0; j < candidate_length; j++) {
                    int index_candidate = candidate_indices[j];

                    while (rleIndex < index && currentPos + run_length[rleIndex] <= index_candidate) {
                        currentPos += run_length[rleIndex];
                        rleIndex++;
                    }

                    if (rleIndex < index) {
                        if (rle_values[rleIndex] < value) {
                            result[result_length[0]] = block_size * block_index + index_candidate;
                            result_length[0]++;
                        } else if (rle_values[rleIndex] == value) {
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
        // String parent_dir = "D:/encoding-subcolumn/dataset/";

        String output_parent_dir = "D:/encoding-subcolumn/";

        int[] block_size_list = { 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192 };

        List<String> integerDatasets = new ArrayList<>();
        integerDatasets.add("Wine-Tasting");

        HashMap<String, Integer> queryRange = new HashMap<>();

        queryRange.put("Bird-migration", 2600000);
        queryRange.put("Bitcoin-price", 170000000);
        queryRange.put("City-temp", 700);
        queryRange.put("Dewpoint-temp", 9600);
        queryRange.put("IR-bio-temp", -200);
        queryRange.put("PM10-dust", 2000);
        queryRange.put("Stocks-DE", 90000);
        queryRange.put("Stocks-UK", 30000);
        queryRange.put("Stocks-USA", 6000);
        queryRange.put("Wind-Speed", 60);
        queryRange.put("Wine-Tasting", 10);

        int repeatTime = 200;

        // repeatTime = 1;

        for (int block_size : block_size_list) {
            String outputPath = output_parent_dir + "subcolumn_query_less_parts_block_" + block_size + ".csv";

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

                s = System.nanoTime();

                for (int repeat = 0; repeat < repeatTime; repeat++) {
                    QueryTwoColumns(encoded_result1, encoded_result2,
                            queryRange.get(datasetName), queryRange.get(datasetName));
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

        HashMap<String, Integer> queryRange = new HashMap<>();

        queryRange.put("Bird-migration", 2600000);
        queryRange.put("Bitcoin-price", 170000000);
        queryRange.put("City-temp", 700);
        queryRange.put("Dewpoint-temp", 9600);
        queryRange.put("IR-bio-temp", -200);
        queryRange.put("PM10-dust", 2000);
        queryRange.put("Stocks-DE", 90000);
        queryRange.put("Stocks-UK", 30000);
        queryRange.put("Stocks-USA", 6000);
        queryRange.put("Wind-Speed", 60);
        queryRange.put("Wine-Tasting", 10);

        int repeatTime = 200;

        // repeatTime = 1;

        for (int beta : beta_list) {
            String outputPath = output_parent_dir + "subcolumn_query_less_parts_beta_" + beta + ".csv";

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
                    length1 = Subcolumn5BetaTest.Encoder(col1_data, block_size, encoded_result1, beta);
                }
                long e = System.nanoTime();
                encodeTime += ((e - s) / repeatTime);

                // 编码第二列
                s = System.nanoTime();
                for (int repeat = 0; repeat < repeatTime; repeat++) {
                    length2 = Subcolumn5BetaTest.Encoder(col2_data, block_size, encoded_result2, beta);
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
                    QueryTwoColumns(encoded_result1, encoded_result2,
                            queryRange.get(datasetName), queryRange.get(datasetName));
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
