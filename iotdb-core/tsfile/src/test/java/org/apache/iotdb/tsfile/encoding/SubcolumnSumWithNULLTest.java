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
import java.util.List;

public class SubcolumnSumWithNULLTest {

    public static void Query(byte[] encoded_result) {

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
        int[] result = new int[data_length];
        int[] result_length = new int[1];

        for (int i = 0; i < num_blocks; i++) {
            encode_pos = BlockQuerySum(encoded_result, i, block_size, block_size, encode_pos, result,
                    result_length);
        }

        int remainder = data_length % block_size;

        if (remainder <= 3) {
            for (int i = 0; i < remainder; i++) {
                int value = ((encoded_result[encode_pos] & 0xFF) << 24) |
                        ((encoded_result[encode_pos + 1] & 0xFF) << 16) |
                        ((encoded_result[encode_pos + 2] & 0xFF) << 8) | (encoded_result[encode_pos + 3] & 0xFF);
                encode_pos += 4;
                result[result_length[0]] = value;
                result_length[0]++;
            }
        } else {
            encode_pos = BlockQuerySum(encoded_result, num_blocks, block_size, remainder, encode_pos,
                    result, result_length);
        }

        // for (int i = 0; i < result_length[0]; i++) {
        // System.out.print(result[i] + " ");
        // }
        // System.out.println();

    }

    public static int BlockQuerySum(byte[] encoded_result, int block_index, int block_size, int remainder,
            int encode_pos, int[] result, int[] result_length) {
        int[] min_delta = new int[3];

        min_delta[0] = ((encoded_result[encode_pos] & 0xFF) << 24) | ((encoded_result[encode_pos + 1] & 0xFF) << 16) |
                ((encoded_result[encode_pos + 2] & 0xFF) << 8) | (encoded_result[encode_pos + 3] & 0xFF);
        encode_pos += 4;

        int m = encoded_result[encode_pos];
        encode_pos += 1;

        if (m == 0) {
            result[result_length[0]] = min_delta[0];
            result_length[0]++;
            return encode_pos;
        }

        int bw = SubcolumnTest.bitWidth(block_size);

        int beta = encoded_result[encode_pos];
        encode_pos += 1;

        int l = (m + beta - 1) / beta;

        int[] bitWidthList = new int[l];

        encode_pos = SubcolumnTest.decodeBitPacking(encoded_result, encode_pos, 8, l, bitWidthList);

        int[] encodingType = new int[l];

        encode_pos = SubcolumnTest.decodeBitPacking(encoded_result, encode_pos, 1, l, encodingType);

        for (int i = l - 1; i >= 0; i--) {
            int type = encodingType[i];
            if (type == 0) {

                encode_pos *= 8;

                for (int j = 0; j < remainder; j++) {
                    int value = SubcolumnTest.bytesToInt(encoded_result, encode_pos + j * bitWidthList[i],
                            bitWidthList[i]);
                    result[result_length[0]] += value << (i * beta);
                }

                encode_pos += remainder * bitWidthList[i];
                encode_pos = (encode_pos + 7) / 8;

            } else {
                int index = ((encoded_result[encode_pos] & 0xFF) << 8) | (encoded_result[encode_pos + 1] & 0xFF);

                encode_pos += 2;

                int[] run_length = new int[index];
                int[] rle_values = new int[index];

                encode_pos = SubcolumnTest.decodeBitPacking(encoded_result, encode_pos, bw, index, run_length);
                encode_pos = SubcolumnTest.decodeBitPacking(encoded_result, encode_pos, bitWidthList[i], index,
                        rle_values);

                for (int j = 0; j < index; j++) {
                    int runCount = j == 0 ? run_length[j] : run_length[j] - run_length[j - 1];
                    result[result_length[0]] += (rle_values[j] << (i * beta)) * runCount;
                    // result[result_length[0]] += (rle_values[j] << (i * beta)) * run_length[j];
                }
            }
        }

        result_length[0]++;

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
    public void testQueryBeta() throws IOException {
//        String parent_dir = "D:/github/xjz17/subcolumn/";
//
//        String input_parent_dir = parent_dir + "dataset/";
//
//        String output_parent_dir = "D:/encoding-subcolumn/result/query_vs_beta/";
//        // String output_parent_dir = parent_dir + "result/query_vs_beta/";
        String parent_dir = "/Users/xiaojinzhao/Documents/GitHub/subcolumn/"; //"D:/github/xjz17/subcolumn/";
        // String parent_dir = "D:/encoding-subcolumn/";

        String input_parent_dir = parent_dir + "dataset/";

        String output_parent_dir = parent_dir + "result/query_sum_null/"; //""D:/encoding-subcolumn/result/";

//        int[] beta_list = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23,
//            24, 25, 26, 27, 28, 29, 30, 31 };
        double[] null_rate_list = {0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1};

        int block_size = 512;

        int repeatTime = 500;

        // repeatTime = 1;

        List<String> integerDatasets = new ArrayList<>();
        integerDatasets.add("Wine-Tasting");

        for (double null_rate : null_rate_list) {
            String outputPath = output_parent_dir + "subcolumn_query_sum_null_" + null_rate + ".csv";

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
                if(datasetName.equals("POI-lon")) continue;

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
                    if (cur_decimal > max_decimal)
                        max_decimal = cur_decimal;
                    data1.add(Float.valueOf(f_str));
                }
                inputStream.close();
                int[] data2_arr = new int[data1.size()];
                int nullCountPerBlock = (int) (null_rate * (double) block_size); // 每块中要设置为null的数量
                int new_arr_length = (int) ((double)(data1.size()/block_size*block_size)*(1-null_rate)+
                        (double)(data1.size()-data1.size()/block_size*block_size)*(1-null_rate));
                System.out.println("new_arr_length:"+new_arr_length);
                int[] data2_arr_new = new int[new_arr_length];

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

                java.util.BitSet bitmap = new java.util.BitSet(data1.size());
                int new_array_index = 0;

                if(null_rate==1){
                    int[] bitmap_bit = new int[data1.size()];
                    long s = System.nanoTime();
                    for (int repeat = 0; repeat < repeatTime; repeat++) {
                        for(int i=0;i<data1.size();i++){
                            bitmap_bit[i] = 0;
                        }
                    }

                    long e = System.nanoTime();
                    encodeTime += ((e - s) / repeatTime);
                    compressed_size += length;
                    double ratioTmp = 0;
                    s = System.nanoTime();

                    for (int repeat = 0; repeat < repeatTime; repeat++) {
                        String[] decode_values = new String[data1.size()];
                        for(int i=0;i<data1.size();i++){
                            decode_values[i] = "NULL";
                        }
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
                    continue;
                } else if(null_rate != 0 && null_rate != 1){
                    for (int blockStart = 0; blockStart < data1.size(); blockStart += block_size) {
                        int blockEnd = Math.min(blockStart + block_size, data1.size());
                        int actualBlockSize = (int) ((double)(blockEnd - blockStart)*(1-null_rate));
                        int actualNullCount = Math.min(nullCountPerBlock, actualBlockSize);

                        // 创建当前块的索引列表用于随机选择
                        java.util.List<Integer> indices = new java.util.ArrayList<>();
                        for (int i = blockStart; i < blockEnd; i++) {
                            indices.add(i);
                        }

                        // 随机打乱并选择要设置为null的位置
                        java.util.Collections.shuffle(indices);
                        java.util.List<Integer> selectedIndices = new java.util.ArrayList<>();
                        for (int i = 0; i < actualNullCount; i++) {
                            selectedIndices.add(indices.get(i));
                        }
                        java.util.Collections.sort(selectedIndices);
                        int i = 0;
                        int j = blockStart;
//                        int nullIndex;
                        while (j < blockEnd) {
                            if (i < actualNullCount && j == selectedIndices.get(i)) {
                                // 这个位置被移除，设置bitmap并跳过
                                bitmap.set(j);
                                i++;
                            } else {
                                // 这个位置保留，复制到新数组
//                                if(new_array_index==70000){
//                                    System.out.println(j/block_size*block_size);
//                                    System.out.println(j);
//                                    System.out.println(new_array_index);
//                                }
                                data2_arr_new[new_array_index] = data2_arr[j];
                                new_array_index++;
                                if(new_array_index == new_arr_length) break;
                            }
                            j++;
                        }
                        if(new_array_index == new_arr_length) break;
                    }
                }else {
                    data2_arr_new = data2_arr;
                }


                long s = System.nanoTime();
                for (int repeat = 0; repeat < repeatTime; repeat++) {
                    length = SubcolumnTest.Encoder(data2_arr_new, (block_size-nullCountPerBlock), encoded_result);
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

                System.out.println("Query");

                s = System.nanoTime();

                for (int repeat = 0; repeat < repeatTime; repeat++) {
                    SubcolumnSumWithNULLTest.Query(encoded_result);
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

                System.out.println(ratio);
            }

            writer.close();
        }
    }

}
