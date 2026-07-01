package org.apache.iotdb.tsfile.encoding;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.*;

public class SubcolumnPointQueryTest {


    public static int[] getAbsDeltaTsBlock(
            int[] ts_block,
            int i,
            int block_size,
            int remaining,
            int[] min_delta) {
        int[] ts_block_delta = new int[remaining];

        int value_delta_min = Integer.MAX_VALUE;
        int value_delta_max = Integer.MIN_VALUE;
        int base = i * block_size;
        int end = i * block_size + remaining;

        for (int j = base; j < end; j++) {
            int cur = ts_block[j];
            if (cur < value_delta_min) {
                value_delta_min = cur;
            }
            if (cur > value_delta_max) {
                value_delta_max = cur;
            }
        }

        for (int j = base; j < end; j++) {
            ts_block_delta[j - base] = ts_block[j] - value_delta_min;
        }

        min_delta[0] = value_delta_min;

        return ts_block_delta;
    }

    public static int BlockEncoder(int[] data, int block_index, int block_size, int remainder,
                                   int encode_pos, byte[] encoded_result, int[] beta) {
        int[] min_delta = new int[3];

        int[] data_delta = getAbsDeltaTsBlock(data, block_index, block_size,
                remainder, min_delta);

        encoded_result[encode_pos] = (byte) (min_delta[0] >> 24);
        encoded_result[encode_pos + 1] = (byte) (min_delta[0] >> 16);
        encoded_result[encode_pos + 2] = (byte) (min_delta[0] >> 8);
        encoded_result[encode_pos + 3] = (byte) min_delta[0];
        encode_pos += 4;

        encode_pos = SubcolumnTest.SubcolumnEncoder(data_delta, encode_pos,
                encoded_result, beta, block_size);

        return encode_pos;
    }

    public static int Encoder(int[] data, int block_size, byte[] encoded_result, int beta_value) {
        int data_length = data.length;
        int encode_pos = 0;

        encoded_result[0] = (byte) (data_length >> 24);
        encoded_result[1] = (byte) (data_length >> 16);
        encoded_result[2] = (byte) (data_length >> 8);
        encoded_result[3] = (byte) data_length;
        encode_pos += 4;

        encoded_result[4] = (byte) (block_size >> 24);
        encoded_result[5] = (byte) (block_size >> 16);
        encoded_result[6] = (byte) (block_size >> 8);
        encoded_result[7] = (byte) block_size;
        encode_pos += 4;

        int num_blocks = data_length / block_size;

        int remainder = data_length % block_size;

        int[] beta = new int[1];
        beta[0] = beta_value;
//        System.out.println(Arrays.toString(beta));

        int pre_encode_pos = encode_pos;

//        encode_pos += 2;
        for (int i = 0; i < num_blocks; i++) {
            encode_pos += 2;
            encode_pos = BlockEncoder(data, i, block_size, block_size, encode_pos, encoded_result, beta);
            int encode_block_length = encode_pos-pre_encode_pos;
            encoded_result[pre_encode_pos] = (byte) (encode_block_length >> 8);
            encoded_result[pre_encode_pos+1] = (byte) encode_block_length;
            pre_encode_pos = encode_pos;
        }


        if (remainder <= 3) {
            for (int i = 0; i < remainder; i++) {
                int value = data[num_blocks * block_size + i];
                encoded_result[encode_pos] = (byte) (value >> 24);
                encoded_result[encode_pos + 1] = (byte) (value >> 16);
                encoded_result[encode_pos + 2] = (byte) (value >> 8);
                encoded_result[encode_pos + 3] = (byte) value;
                encode_pos += 4;
            }
            encoded_result[pre_encode_pos+1] = (byte) (remainder*4);
        } else {
            encode_pos = BlockEncoder(data, num_blocks, block_size, remainder, encode_pos,
                    encoded_result, beta);
            int encode_block_length = encode_pos-pre_encode_pos;
            encoded_result[pre_encode_pos] = (byte) (encode_block_length >> 8);
            encoded_result[pre_encode_pos+1] = (byte) encode_block_length;
        }

        return encode_pos;
    }


    public static void Query(byte[] encoded_result,  int point) {

        int encode_pos = 0;

//        int data_length = ((encoded_result[encode_pos] & 0xFF) << 24) | ((encoded_result[encode_pos + 1] & 0xFF) << 16)
//                |
//                ((encoded_result[encode_pos + 2] & 0xFF) << 8) | (encoded_result[encode_pos + 3] & 0xFF);
        encode_pos += 4;

        int block_size = ((encoded_result[encode_pos] & 0xFF) << 24) | ((encoded_result[encode_pos + 1] & 0xFF) << 16) |
                ((encoded_result[encode_pos + 2] & 0xFF) << 8) | (encoded_result[encode_pos + 3] & 0xFF);
        encode_pos += 4;

        int number_of_skipped_blocks = point / block_size;
        int acc_add_encode_pos = 0;
        for (int i = 0; i < number_of_skipped_blocks; i++) {
            int tmp_acc_add_encode_pos = (((encoded_result[encode_pos+acc_add_encode_pos] & 0xFF) << 8)|(encoded_result[encode_pos+acc_add_encode_pos+1] & 0xFF));
            acc_add_encode_pos += tmp_acc_add_encode_pos;
        }
        encode_pos += acc_add_encode_pos;
        number_of_skipped_blocks += 1;
        // 在当前的block中的位置
        int pos_in_cur_block = point % block_size;
        int[] result = new int[1];
        encode_pos = BlockQueryIndex(encoded_result, number_of_skipped_blocks, block_size,
                block_size, encode_pos, pos_in_cur_block, result);
        System.out.println("result:" + result[0]);

    }

    public static int BlockQueryIndex(byte[] encoded_result, int block_index, int block_size, int remainder,
            int encode_pos, int pos_in_cur_block, int[] result) {

        encode_pos += 2;

        int[] min_delta = new int[3];

        min_delta[0] = ((encoded_result[encode_pos] & 0xFF) << 24) | ((encoded_result[encode_pos + 1] & 0xFF) << 16) |
                ((encoded_result[encode_pos + 2] & 0xFF) << 8) | (encoded_result[encode_pos + 3] & 0xFF);
        encode_pos += 4;

        // int[] block_data = new int[remainder];

        int m = encoded_result[encode_pos];
        encode_pos += 1;


//        // 候选索引列表，当前分列值和 lower_bound 相应值相等的索引
//        int[] candidate_indices = new int[remainder];
//        int candidate_length = 0;
//        for (int i = 0; i < remainder; i++) {
//            candidate_indices[i] = i;
//            candidate_length++;
//        }

        if (m == 0) {
            result[0] = 0;
            return encode_pos;
        }

        int bw = SubcolumnTest.bitWidth(block_size);

        int beta = encoded_result[encode_pos];
        encode_pos += 1;
        System.out.println("m: "+m);
        System.out.println("beta: "+beta);

        int l = (m + beta - 1) / beta;

        int[] bitWidthList = new int[l];


        encode_pos = SubcolumnTest.decodeBitPacking(encoded_result, encode_pos, 8, l, bitWidthList);

        int[][] subcolumnList = new int[l][remainder];

        int[] encodingType = new int[l];

        encode_pos = SubcolumnTest.decodeBitPacking(encoded_result, encode_pos, 1, l, encodingType);
        int result_value = 0;

        for (int i = l - 1; i >= 0; i--) {
            int type = encodingType[i];
            if (type == 0) {
                encode_pos *= 8;
                result_value +=  SubcolumnTest.bytesToInt(encoded_result,
                        encode_pos + pos_in_cur_block * bitWidthList[i], bitWidthList[i]);

                encode_pos += remainder * bitWidthList[i];
                encode_pos = (encode_pos + 7) / 8;

            } else {

                int index = ((encoded_result[encode_pos] & 0xFF) << 8) | (encoded_result[encode_pos + 1] & 0xFF);

                encode_pos += 2;


                int[] run_length = new int[index];
                int[] rle_values = new int[index];

                encode_pos = SubcolumnTest.decodeBitPacking(encoded_result, encode_pos, bw, index, run_length);
                encode_pos *= 8;
                int accumulate_run_length = 0;
                if(pos_in_cur_block == accumulate_run_length){
                    result_value += SubcolumnTest.bytesToInt(encoded_result,
                            encode_pos, bitWidthList[i]);
                }else{
                    for(int x=0;x<index;x++){
                        accumulate_run_length += run_length[x];
                        if(pos_in_cur_block < accumulate_run_length ){
                            result_value += SubcolumnTest.bytesToInt(encoded_result,
                                    encode_pos + x * bitWidthList[i], bitWidthList[i]);
                            break;
                        }
                    }
                }
                encode_pos += index * bitWidthList[i];
                encode_pos = (encode_pos + 7) / 8;

//                encode_pos = SubcolumnTest.decodeBitPacking(encoded_result, encode_pos, bitWidthList[i], index,
//                        rle_values);


            }
            result_value <<= beta;
        }
        result[0]= result_value;


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

        String parent_dir = "/Users/xiaojinzhao/Documents/GitHub/subcolumn/";
        String input_parent_dir = parent_dir + "dataset/";
        String output_parent_dir = parent_dir + "result/point_query/";//"D:/encoding-subcolumn/result/query_vs_beta/";

//        String parent_dir = "D:/github/xjz17/subcolumn/";
//
//        String input_parent_dir = parent_dir + "dataset/";
//
//        String output_parent_dir = "D:/encoding-subcolumn/result/query_vs_beta/";
//        // String output_parent_dir = parent_dir + "result/query_vs_beta/";

        int[] beta_list = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23,
            24, 25, 26, 27, 28, 29, 30, 31 };
        
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

        int repeatTime = 500;

//         repeatTime = 1;

        List<String> integerDatasets = new ArrayList<>();
        integerDatasets.add("Wine-Tasting");

        for (int beta : beta_list) {
            String outputPath = output_parent_dir + "subcolumn_point_query_beta_" + beta + ".csv";

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
                if(datasetName.equals("POI-lon")||datasetName.equals("POI-lat")) continue;
//                if(!datasetName.equals("Bitcoin-price")) continue;

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
                    length = Encoder(data2_arr, block_size, encoded_result, beta);
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

                Random random = new Random();

                // 生成 [0, length-1] 范围内的随机整数

                int max_random_value = data1.size()/block_size*block_size;

                s = System.nanoTime();
                for (int repeat = 0; repeat < repeatTime; repeat++) {
                    int randomNumber = random.nextInt(max_random_value);
                    SubcolumnPointQueryTest.Query(encoded_result, randomNumber);
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
