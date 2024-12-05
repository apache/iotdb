package org.apache.iotdb.tsfile.encoding;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;

import org.junit.Test;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;

public class SubcolumnSPRINTZTest {
    public static int BOSEncoder(int[] data, int block_size, byte[] encoded_result) {
        int data_length = data.length;
        int startBitPosition = 0;

        SubcolumnTest.writeBits(encoded_result, startBitPosition, 32, data_length);
        startBitPosition += 32;

        // System.out.println("data_length: " + data_length);

        SubcolumnTest.writeBits(encoded_result, startBitPosition, 32, block_size);
        startBitPosition += 32;

        // System.out.println("block_size: " + block_size);

        int num_blocks = data_length / block_size;

        // System.out.println("num_blocks: " + num_blocks);

        for (int i = 0; i < num_blocks; i++) {
            startBitPosition = BOSBlockEncoder(data, i, block_size, block_size, startBitPosition, encoded_result);
        }

        int remainder = data_length % block_size;

        if (remainder <= 3) {
            for (int i = 0; i < remainder; i++) {
                SubcolumnTest.writeBits(encoded_result, startBitPosition, 32, data[num_blocks * block_size + i]);
                startBitPosition += 32;
            }
        } else {
            startBitPosition = BOSBlockEncoder(data, num_blocks, block_size, remainder, startBitPosition,
                    encoded_result);
        }

        return startBitPosition;
    }

    public static int[] BOSDecoder(byte[] encoded_result) {
        int startBitPosition = 0;

        int data_length = SubcolumnTest.readBits(encoded_result, startBitPosition, 32, 0);
        startBitPosition += 32;

        // System.out.println("data_length: " + data_length);

        int block_size = SubcolumnTest.readBits(encoded_result, startBitPosition, 32, 0);
        startBitPosition += 32;

        // System.out.println("block_size: " + block_size);

        int num_blocks = data_length / block_size;

        int[] data = new int[data_length];

        for (int i = 0; i < num_blocks; i++) {
            startBitPosition = BOSBlockDecoder(encoded_result, i, block_size, block_size, startBitPosition, data);
        }

        int remainder = data_length % block_size;

        if (remainder <= 3) {
            for (int i = 0; i < remainder; i++) {
                data[num_blocks * block_size + i] = SubcolumnTest.readBits(encoded_result, startBitPosition, 32, 0);
                startBitPosition += 32;
            }
        } else {
            startBitPosition = BOSBlockDecoder(encoded_result, num_blocks, block_size, remainder,
                    startBitPosition, data);
        }

        return data;
    }

    public static int zigzag(int num) {
        if (num < 0)
            return ((-num) << 1) - 1;
        else
            return num << 1;
    }

    public static int deZigzag(int num) {
        if (num % 2 == 0)
            return num >> 1;
        else
            return -((num + 1) >> 1);
    }

    public static int[] getAbsDeltaTsBlock(
            int[] ts_block,
            int i,
            int block_size,
            int remaining,
            int[] min_delta) {
        int[] ts_block_delta = new int[remaining - 1];

        int base = i * block_size + 1;
        int end = i * block_size + remaining;
        min_delta[0] = ts_block[base - 1];
        int value_delta_min = Integer.MAX_VALUE;
        int value_delta_max = Integer.MIN_VALUE;
        for (int j = base; j < end; j++) {
            int epsilon_v = ts_block[j] - ts_block[j - 1];
            epsilon_v = zigzag(epsilon_v);
            if (epsilon_v < value_delta_min) {
                value_delta_min = epsilon_v;
            }
            if (epsilon_v > value_delta_max) {
                value_delta_max = epsilon_v;
            }
            ts_block_delta[j - base] = epsilon_v;

        }
        for (int j = 0; j < remaining - 1; j++) {
            ts_block_delta[j] = ts_block_delta[j] - value_delta_min;

        }
        min_delta[1] = (value_delta_min);
        min_delta[2] = (value_delta_max - value_delta_min);

        return ts_block_delta;
    }

    public static int BOSBlockEncoder(int[] data, int block_index, int block_size, int remainder,
            int startBitPosition, byte[] encoded_result) {
        int[] min_delta = new int[3];

        int[] data_delta = getAbsDeltaTsBlock(data, block_index, block_size, remainder, min_delta);

        // System.out.println("data_delta_length: " + data_delta.length);

        // for (int i = 0; i < data_delta.length; i++) {
        // System.out.print(data_delta[i] + " ");
        // }
        // System.out.println();

        SubcolumnTest.writeBits(encoded_result, startBitPosition, 32, min_delta[0]);
        startBitPosition += 32;

        // System.out.println("min_delta[0]: " + min_delta[0]);

        SubcolumnTest.writeBits(encoded_result, startBitPosition, 32, min_delta[1]);
        startBitPosition += 32;

        // System.out.println("min_delta[1]: " + min_delta[1]);

        // int m = bitWidth(min_delta[2]);

        // startBitPosition = SubcolumnTest.SubcolumnBetaBPEncoder(data_delta, startBitPosition, encoded_result);
        // startBitPosition = SubcolumnTest.SubcolumnLBPEncoder(data_delta, startBitPosition, encoded_result);
        // startBitPosition = SubcolumnTest.SubcolumnLBetaBPEncoder(data_delta, startBitPosition, encoded_result);
        startBitPosition = SubcolumnTest.SubcolumnBetaEncoder(data_delta, startBitPosition, encoded_result);

        return startBitPosition;
    }

    public static int BOSBlockDecoder(byte[] encoded_result, int block_index, int block_size, int remainder,
            int startBitPosition, int[] data) {
        int[] min_delta = new int[3];

        min_delta[0] = SubcolumnTest.readBits(encoded_result, startBitPosition, 32, 1);
        startBitPosition += 32;

        // System.out.println("min_delta[0]: " + min_delta[0]);

        min_delta[1] = SubcolumnTest.readBits(encoded_result, startBitPosition, 32, 1);
        startBitPosition += 32;

        // System.out.println("min_delta[1]: " + min_delta[1]);

        int[] data_delta = new int[remainder - 1];

        // startBitPosition = SubcolumnTest.SubcolumnBetaBPDecoder(encoded_result, startBitPosition, data_delta);
        // startBitPosition = SubcolumnTest.SubcolumnLBPDecoder(encoded_result, startBitPosition, data_delta);
        // startBitPosition = SubcolumnTest.SubcolumnLBetaBPDecoder(encoded_result, startBitPosition, data_delta);
        startBitPosition = SubcolumnTest.SubcolumnBetaDecoder(encoded_result, startBitPosition, data_delta);

        // System.out.println("data_delta_length: " + data_delta.length);

        // for (int i = 0; i < data_delta.length; i++) {
        // System.out.print(data_delta[i] + " ");
        // }
        // System.out.println();

        for (int i = 0; i < remainder - 1; i++) {
            data_delta[i] = data_delta[i] + min_delta[1];
        }

        for (int i = 0; i < remainder - 1; i++) {
            data_delta[i] = deZigzag(data_delta[i]);
        }

        data[block_index * block_size] = min_delta[0];

        for (int i = 0; i < remainder - 1; i++) {
            data[block_index * block_size + i + 1] = data[block_index * block_size + i] + data_delta[i];
        }

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
    public void testBOS() throws IOException {
        String parent_dir = "/Users/allen/Documents/github/xjz17/subcolumn/elf_resources/";
        // String parent_dir = "/Users/allen/Documents/compress-subcolumn/";
        String output_parent_dir = "/Users/allen/Documents/compress-subcolumn/";
        // String output_parent_dir =
        // "/Users/allen/Documents/github/xjz17/subcolumn/elf_resources";
        // String input_parent_dir = parent_dir + "ElfData_Short/";
        String input_parent_dir = parent_dir + "dataset/";
        // String input_parent_dir = parent_dir + "testdata/";
        ArrayList<String> input_path_list = new ArrayList<>();
        ArrayList<String> output_path_list = new ArrayList<>();
        ArrayList<String> dataset_name = new ArrayList<>();
        ArrayList<Integer> dataset_block_size = new ArrayList<>();
        // dataset_name.add("CS-Sensors");
        // dataset_name.add("Metro-Traffic");
        // dataset_name.add("USGS-Earthquakes");
        // dataset_name.add("YZ-Electricity");
        // dataset_name.add("GW-Magnetic");
        // dataset_name.add("TY-Fuel");
        // dataset_name.add("Cyber-Vehicle");
        // dataset_name.add("Vehicle-Charge");
        // dataset_name.add("Nifty-Stocks");
        // dataset_name.add("TH-Climate");
        // dataset_name.add("TY-Transport");
        // dataset_name.add("EPM-Education");
        input_path_list.add(input_parent_dir);
        dataset_block_size.add(1024);

        // output_path_list.add(output_parent_dir + "compress_ratio.csv");
        // output_path_list.add(output_parent_dir + "subcolumn.csv");
        output_path_list.add(output_parent_dir + "sprintz_subcolumn_beta.csv");
        // for (String value : dataset_name) {
        // input_path_list.add(input_parent_dir + value);
        // dataset_block_size.add(1024);
        // }

        // output_path_list.add(output_parent_dir + "/CS-Sensors_ratio.csv"); // 0
        //// dataset_block_size.add(1024);
        // output_path_list.add(output_parent_dir + "/Metro-Traffic_ratio.csv");// 1
        //// dataset_block_size.add(2048);
        // output_path_list.add(output_parent_dir + "/USGS-Earthquakes_ratio.csv");// 2
        //// dataset_block_size.add(2048);
        // output_path_list.add(output_parent_dir + "/YZ-Electricity_ratio.csv"); // 3
        //// dataset_block_size.add(2048);
        // output_path_list.add(output_parent_dir + "/GW-Magnetic_ratio.csv"); //4
        //// dataset_block_size.add(1024);
        // output_path_list.add(output_parent_dir + "/TY-Fuel_ratio.csv");//5
        //// dataset_block_size.add(2048);
        // output_path_list.add(output_parent_dir + "/Cyber-Vehicle_ratio.csv"); //6
        //// dataset_block_size.add(2048);
        // output_path_list.add(output_parent_dir + "/Vehicle-Charge_ratio.csv");//7
        //// dataset_block_size.add(2048);
        // output_path_list.add(output_parent_dir + "/Nifty-Stocks_ratio.csv");//8
        //// dataset_block_size.add(1024);
        // output_path_list.add(output_parent_dir + "/TH-Climate_ratio.csv");//9
        //// dataset_block_size.add(2048);
        // output_path_list.add(output_parent_dir + "/TY-Transport_ratio.csv");//10
        //// dataset_block_size.add(2048);
        // output_path_list.add(output_parent_dir + "/EPM-Education_ratio.csv");//11
        //// dataset_block_size.add(1024);

        int repeatTime2 = 100;
        // TODO 真正计算时，记得注释掉将下面的内容
        // repeatTime2 = 1;

        // for (int file_i = 1; file_i < 2; file_i++) {

        for (int file_i = 0; file_i < input_path_list.size(); file_i++) {

            String inputPath = input_path_list.get(file_i);
            System.out.println(inputPath);
            String Output = output_path_list.get(file_i);

            File file = new File(inputPath);
            File[] tempList = file.listFiles();

            CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);

            String[] head = {
                    "Dataset",
                    "Encoding Algorithm",
                    "Encoding Time",
                    "Decoding Time",
                    "Points",
                    "Compressed Size",
                    "Compression Ratio"
            };
            writer.writeRecord(head); // write header to output file

            assert tempList != null;

            for (File f : tempList) {
                // f=tempList[2];

                // System.out.println(f);
                String datasetName = extractFileName(f.toString());
                System.out.println(datasetName);
                InputStream inputStream = Files.newInputStream(f.toPath());

                CsvReader loader = new CsvReader(inputStream, StandardCharsets.UTF_8);
                ArrayList<Float> data1 = new ArrayList<>();
                // ArrayList<Integer> data2 = new ArrayList<>();

                // loader.readHeaders();
                int max_decimal = 0;
                while (loader.readRecord()) {
                    String f_str = loader.getValues()[0];
                    // System.out.println(f_str);
                    // if (f_str.equals("")) {
                    // continue;
                    // }
                    int cur_decimal = getDecimalPrecision(f_str);
                    if (cur_decimal > max_decimal)
                        max_decimal = cur_decimal;
                    // String value = loader.getValues()[index];
                    data1.add(Float.valueOf(f_str));
                    // data2.add(Integer.valueOf(loader.getValues()[1]));
                    // data.add(Integer.valueOf(value));
                }
                inputStream.close();
                int[] data2_arr = new int[data1.size()];
                int max_mul = (int) Math.pow(10, max_decimal);
                for (int i = 0; i < data1.size(); i++) {
                    data2_arr[i] = (int) (data1.get(i) * max_mul);
                }

                // System.out.println("initial data");
                // for (int i = 0; i < data2_arr.length; i++) {
                // System.out.print(data2_arr[i] + " ");
                // }
                // System.out.println();

                System.out.println(max_decimal);
                byte[] encoded_result = new byte[data2_arr.length * 4];

                // for (int div = 2; div < 11; div++) {
                // System.out.println(div);
                long encodeTime = 0;
                long decodeTime = 0;
                double ratio = 0;
                double compressed_size = 0;

                int length = 0;

                long s = System.nanoTime();
                for (int repeat = 0; repeat < repeatTime2; repeat++) {
                    length = BOSEncoder(data2_arr, dataset_block_size.get(file_i),
                            encoded_result);
                }

                long e = System.nanoTime();
                encodeTime += ((e - s) / repeatTime2);
                compressed_size += length / 8;
                double ratioTmp = compressed_size / (double) (data1.size() * Long.BYTES);
                ratio += ratioTmp;
                s = System.nanoTime();
                // System.out.println("Decode");
                for (int repeat = 0; repeat < repeatTime2; repeat++) {
                    // SubcolumnDecoder(encoded_result);
                    int[] data2_arr_decoded = BOSDecoder(encoded_result);

                    for (int i = 0; i < data2_arr_decoded.length; i++) {
                        // System.out.print(data2_arr_decoded[i] + " ");
                        assert ((data2_arr[i] == data2_arr_decoded[i])
                                || (data2_arr[i] + Integer.MAX_VALUE + 1 == data2_arr_decoded[i]));
                        // assert data2_arr[i] == data2_arr_decoded[i];
                        // if (data2_arr_decoded[i] != data2_arr[i]) {
                        // if (!(data2_arr_decoded[i] == data2_arr[i]
                        // || data2_arr_decoded[i] == data2_arr[i] + Integer.MAX_VALUE + 1)) {
                        // System.out.println("Error");
                        // System.out.println("repeat: " + repeat);
                        // System.out.println(i);
                        // System.out.println(data2_arr_decoded[i]);
                        // System.out.println(data2_arr[i]);
                        // break;
                        // }
                    }
                    // System.out.println();

                    // for (int i = 0; i < data2_arr.length; i++) {
                    // System.out.print(data2_arr[i] + " ");
                    // }
                    // System.out.println();
                }
                e = System.nanoTime();
                decodeTime += ((e - s) / repeatTime2);

                String[] record = {
                        datasetName,
                        "SPRINTZ+SubcolumnBeta",
                        String.valueOf(encodeTime),
                        String.valueOf(decodeTime),
                        // String.valueOf(div),
                        String.valueOf(data1.size()),
                        String.valueOf(compressed_size),
                        String.valueOf(ratio)
                };
                writer.writeRecord(record);
                System.out.println(ratio);
                // }

                // break;
            }
            writer.close();
        }
    }
}
