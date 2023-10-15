package org.apache.iotdb.tsfile.encoding;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Stack;

public class Outlier3DTest {

    public static int getBitWith(int num) {
        if (num == 0) return 1;
        else return 32 - Integer.numberOfLeadingZeros(num);
    }

    public static byte[] int2Bytes(int integer) {
        byte[] bytes = new byte[4];
        bytes[0] = (byte) (integer >> 24);
        bytes[1] = (byte) (integer >> 16);
        bytes[2] = (byte) (integer >> 8);
        bytes[3] = (byte) integer;
        return bytes;
    }

    public static byte[] intByte2Bytes(int integer) {
        byte[] bytes = new byte[1];
        bytes[0] = (byte) integer;
        return bytes;
    }

    public static int bytes2Integer(ArrayList<Byte> encoded, int start, int num) {
        int value = 0;
        if (num > 4) {
            System.out.println("bytes2Integer error");
            return 0;
        }
        for (int i = 0; i < num; i++) {
            value <<= 8;
            int b = encoded.get(i + start) & 0xFF;
            value |= b;
        }
        return value;
    }

    public static byte[] bitPacking(ArrayList<Integer> numbers, int start, int bit_width) {
        int block_num = numbers.size() / 8;
        byte[] result = new byte[bit_width * block_num];
        for (int i = 0; i < block_num; i++) {
            for (int j = 0; j < bit_width; j++) {
                int tmp_int = 0;
                for (int k = 0; k < 8; k++) {
                    tmp_int += (((numbers.get(i * 8 + k + start) >> j) % 2) << k);
                }
                result[i * bit_width + j] = (byte) tmp_int;
            }
        }
        return result;
    }

    public static byte[] bitPacking(ArrayList<ArrayList<Integer>> numbers, int start, int index, int bit_width) {
        int block_num = numbers.size() / 8;
        byte[] result = new byte[bit_width * block_num];
        for (int i = 0; i < block_num; i++) {
            for (int j = 0; j < bit_width; j++) {
                int tmp_int = 0;
                for (int k = 0; k < 8; k++) {
                    tmp_int += (((numbers.get(i * 8 + k + start).get(index) >> j) % 2) << k);
                }
                result[i * bit_width + j] = (byte) tmp_int;
            }
        }
        return result;
    }



    public static ArrayList<Integer> getAbsDeltaTsBlock(
            ArrayList<Integer> ts_block,
            ArrayList<Integer> min_delta) {
        ArrayList<Integer> ts_block_delta = new ArrayList<>();

        ts_block_delta.add(ts_block.get(0));
        int value_delta_min = Integer.MAX_VALUE;
        for (int i = 1; i < ts_block.size(); i++) {

            int epsilon_v = ts_block.get(i) - ts_block.get(i - 1);

            if (epsilon_v < value_delta_min) {
                value_delta_min = epsilon_v;
            }

        }
        min_delta.add(value_delta_min);
        for (int i = 1; i < ts_block.size(); i++) {
            int epsilon_v = ts_block.get(i) - value_delta_min - ts_block.get(i - 1);
            ts_block_delta.add(epsilon_v);
        }
        return ts_block_delta;
    }

    private static int bosEncode(ArrayList<Integer> ts_block, double x_c, int beta, int supple_length) {

        int final_right_max = Integer.MIN_VALUE;
        ArrayList<Byte> cur_byte = new ArrayList<>();

        ArrayList<Integer> min_delta = new ArrayList<>();
        ArrayList<Integer> ts_block_delta = getAbsDeltaTsBlock(ts_block, min_delta);
        for (int s = 0; s < supple_length; s++) {
            ts_block_delta.add(0);
        }

        int block_size = ts_block_delta.size();
        ArrayList<Integer> ts_block_order_value = getAbsDeltaTsBlock(ts_block, min_delta);
        Collections.sort(ts_block_order_value);

        double sum = 0;
        for (int i = 1; i < block_size; i++) {
            if (ts_block_delta.get(i) > final_right_max) {
                final_right_max = ts_block_delta.get(i);
            }
            sum += ts_block_delta.get(i);
        }
        double mu = sum / block_size;
        double variance = 0;
        for (int i = 1; i < block_size; i++) {
            variance += (ts_block_delta.get(i) - mu) * (ts_block_delta.get(i) - mu);
        }
        double sigma = Math.sqrt(variance / block_size);

        ArrayList<Integer> PDF = new ArrayList<>();
        int min_delta_value = ts_block_order_value.get(0);
        int tmp = min_delta_value;
        int final_i = 0;


        for (int i = 1; i < block_size; i++) {
            if(ts_block_order_value.get(i) != tmp){
                int start_v = ts_block_order_value.get(i);
                PDF.add(tmp);
                tmp =start_v;
                final_i = i;
            }
        }
        if(final_i!=block_size-1){
            PDF.add(tmp);
        }


        int final_k_start_i = (int) ((double) PDF.size() * x_c);
        if(final_k_start_i > PDF.size()-1){
            final_k_start_i = PDF.size()-1;
        }
        int final_k_start_value = PDF.get(final_k_start_i);
        int max_delta = PDF.get(PDF.size()-1);
        int final_k_end_value = (int) (final_k_start_value+ Math.pow(2,beta) > max_delta? max_delta:(final_k_start_value+ Math.pow(2,beta)));
        int cost =0;

        int k1 = 0;
        int k2 = 0;

        for (int i = 1; i < block_size; i++) {
            if (ts_block_delta.get(i) < final_k_start_value) {
                k1++;
            } else if (ts_block_delta.get(i) > final_k_end_value) {
                k2++;
            }
        }
        int left_bit_width = getBitWith(final_k_start_value);
        int right_bit_width = getBitWith(final_right_max - final_k_end_value);
        cost += left_bit_width*k1;
        cost += right_bit_width*k2;
        cost += beta*(block_size-1-k1-k2);
        cost += Math.min(getBitWith(block_size) * (k1 + k2) , (block_size + k1 + k2));

        return cost;
    }

    public static int ReorderingRegressionEncoder(
            ArrayList<Integer> data, int block_size, double x_c, int beta) throws IOException {
        block_size++;
        ArrayList<Byte> encoded_result = new ArrayList<Byte>();
        int length_all = data.size();
        byte[] length_all_bytes = int2Bytes(length_all);
        for (byte b : length_all_bytes) encoded_result.add(b);
        int block_num = length_all / block_size;
        int bits_number = 0;

        byte[] block_size_byte = int2Bytes(block_size);
        for (byte b : block_size_byte) encoded_result.add(b);


        for (int i = 0; i < 1; i++) {
//        for (int i = 0; i < block_num; i++) {

            ArrayList<Integer> ts_block = new ArrayList<>();
            for (int j = 0; j < block_size; j++) {
                ts_block.add(data.get(j + i * block_size));
            }
            // time-order
            bits_number += bosEncode(ts_block, x_c, beta, 0);

        }


        return bits_number;
    }


    public static void main(@org.jetbrains.annotations.NotNull String[] args) throws IOException {

        String parent_dir = "/Users/xiaojinzhao/Desktop/encoding-outlier/"; ///Users/xiaojinzhao/Desktop
//        String parent_dir = "/Users/zihanguo/Downloads/outliier_code/encoding-outlier/";
        String output_parent_dir = parent_dir + "vldb/compression_ratio/test2d";
        String input_parent_dir = parent_dir + "trans_data/";//手动改过的数据
        ArrayList<String> input_path_list = new ArrayList<>();
        ArrayList<String> output_path_list = new ArrayList<>();
        ArrayList<String> dataset_name = new ArrayList<>();
        ArrayList<Integer> dataset_block_size = new ArrayList<>();
        dataset_name.add("CS-Sensors");
        dataset_name.add("Metro-Traffic");
        dataset_name.add("USGS-Earthquakes");
        dataset_name.add("YZ-Electricity");
        dataset_name.add("GW-Magnetic");
        dataset_name.add("TY-Fuel");
        dataset_name.add("Cyber-Vehicle");
        dataset_name.add("Vehicle-Charge");
        dataset_name.add("Nifty-Stocks");
        dataset_name.add("TH-Climate");
        dataset_name.add("TY-Transport");
        dataset_name.add("EPM-Education");

        for (int i = 0; i < dataset_name.size(); i++) {
            input_path_list.add(input_parent_dir + dataset_name.get(i));
        }

        output_path_list.add(output_parent_dir + "/CS-Sensors_ratio.csv"); // 0
        dataset_block_size.add(1024);
        output_path_list.add(output_parent_dir + "/Metro-Traffic_ratio.csv");// 1
        dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/USGS-Earthquakes_ratio.csv");// 2
        dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/YZ-Electricity_ratio.csv"); // 3
        dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/GW-Magnetic_ratio.csv"); //4
        dataset_block_size.add(1024);
        output_path_list.add(output_parent_dir + "/TY-Fuel_ratio.csv");//5
        dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/Cyber-Vehicle_ratio.csv"); //6
        dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/Vehicle-Charge_ratio.csv");//7
        dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/Nifty-Stocks_ratio.csv");//8
        dataset_block_size.add(1024);
        output_path_list.add(output_parent_dir + "/TH-Climate_ratio.csv");//9
        dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/TY-Transport_ratio.csv");//10
        dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/EPM-Education_ratio.csv");//11
        dataset_block_size.add(1024);

        ArrayList<Integer> columnIndexes = new ArrayList<>(); // set the column indexes of compressed
        for (int i = 0; i < 2; i++) {
            columnIndexes.add(i, i);
        }


        for (int file_i = 0; file_i < input_path_list.size(); file_i++) {

            String inputPath = input_path_list.get(file_i);
            System.out.println(inputPath);
            String Output = output_path_list.get(file_i);

            int repeatTime = 1; // set repeat time

            File file = new File(inputPath);
            File[] tempList = file.listFiles();

            CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);

            String[] head = {
                    "Input Direction",
                    "Encoding Algorithm",
                    "Encoding Time",
                    "Decoding Time",
                    "Points",
                    "Cost",
                    "x_c",
                    "beta"
            };
            writer.writeRecord(head); // write header to output file

            assert tempList != null;

            for (File f : tempList) {
                System.out.println(f);
                InputStream inputStream = Files.newInputStream(f.toPath());

                CsvReader loader = new CsvReader(inputStream, StandardCharsets.UTF_8);
                ArrayList<Integer> data1 = new ArrayList<>();
                ArrayList<Integer> data2 = new ArrayList<>();
                ArrayList<ArrayList<Integer>> data_decoded = new ArrayList<>();


                loader.readHeaders();
                while (loader.readRecord()) {
                    data1.add(Integer.valueOf(loader.getValues()[0]));
                    data2.add(Integer.valueOf(loader.getValues()[1]));
                }
                inputStream.close();
                for (double x_c = 0; x_c < 1.009; x_c+=0.05) {
                    for (int beta = 1; beta < 15; beta++) {
                        long encodeTime = 0;
                        long decodeTime = 0;
                        double compressed_size = 0;
                        int repeatTime2 = 1;
                        for (int i = 0; i < repeatTime; i++) {
                            long s = System.nanoTime();

                            long buffer_bits = 0;
                            for (int repeat = 0; repeat < repeatTime2; repeat++) {
                                buffer_bits = ReorderingRegressionEncoder(data2, dataset_block_size.get(file_i), x_c, beta);
                            }

                            compressed_size += buffer_bits;
                        }

                        compressed_size /= repeatTime;
                        encodeTime /= repeatTime;
                        decodeTime /= repeatTime;

                        String[] record = {
                                f.toString(),
                                "Outlier",
                                String.valueOf(encodeTime),
                                String.valueOf(decodeTime),
                                String.valueOf(data1.size()),
                                String.valueOf(compressed_size),
                                String.valueOf(x_c),
                                String.valueOf(beta),
                        };
                        writer.writeRecord(record);
                    }
                }
                break;
            }

            writer.close();

        }
    }

}
