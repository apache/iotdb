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

import static java.lang.Math.pow;

public class TS_2DIFF {

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

    private static byte[] long2intBytes(long integer) {
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

    private static long bytesLong2Integer(ArrayList<Byte> encoded, int decode_pos, int num) {
        long value = 0;
        if (num > 4) {
            System.out.println("bytes2Integer error");
            return 0;
        }
        for (int i = 0; i < num; i++) {
            value <<= 8;
            int b = encoded.get(i + decode_pos) & 0xFF;
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


    public static ArrayList<Integer> decodeBitPacking(
            ArrayList<Byte> encoded, int decode_pos, int bit_width, int block_size) {
        ArrayList<Integer> result_list = new ArrayList<>();
        for (int i = 0; i < (block_size - 1) / 8; i++) { // bitpacking  纵向8个，bit width是多少列
            int[] val8 = new int[8];
            for (int j = 0; j < 8; j++) {
                val8[j] = 0;
            }
            for (int j = 0; j < bit_width; j++) {
                byte tmp_byte = encoded.get(decode_pos + bit_width - 1 - j);
                byte[] bit8 = new byte[8];
                for (int k = 0; k < 8; k++) {
                    bit8[k] = (byte) (tmp_byte & 1);
                    tmp_byte = (byte) (tmp_byte >> 1);
                }
                for (int k = 0; k < 8; k++) {
                    val8[k] = val8[k] * 2 + bit8[k];
                }
            }
            for (int j = 0; j < 8; j++) {
                result_list.add(val8[j]);
            }
            decode_pos += bit_width;
        }
        return result_list;
    }



    public static ArrayList<Integer> getAbsDeltaTsBlock(
            ArrayList<Integer> ts_block,
            ArrayList<Integer> min_delta) {
        ArrayList<Integer> ts_block_delta = new ArrayList<>();

//        ts_block_delta.add(ts_block.get(0));
        int value_delta_min = Integer.MAX_VALUE;
        int value_delta_max = Integer.MIN_VALUE;
        for (int i = 1; i < ts_block.size(); i++) {

            int epsilon_v = ts_block.get(i) - ts_block.get(i - 1);

            if (epsilon_v < value_delta_min) {
                value_delta_min = epsilon_v;
            }

            if (epsilon_v > value_delta_max) {
                value_delta_max = epsilon_v;
            }
        }
        min_delta.add(ts_block.get(0));
        min_delta.add(value_delta_min);
        min_delta.add(getBitWith(value_delta_max-value_delta_min));
        for (int i = 1; i < ts_block.size(); i++) {
            int epsilon_v = ts_block.get(i) - value_delta_min - ts_block.get(i - 1);
            ts_block_delta.add(epsilon_v);
        }
        return ts_block_delta;
    }

    public static ArrayList<Integer> getAbsDeltaTsBlock(
            ArrayList<Integer> ts_block,
            int i,
            int block_size,
            ArrayList<Integer> min_delta) {
        ArrayList<Integer> ts_block_delta = new ArrayList<>();

        int value_delta_min = Integer.MAX_VALUE;
        int value_delta_max = Integer.MIN_VALUE;
        for (int j = i*block_size+1; j < (i+1)*block_size; j++) {

            int epsilon_v = ts_block.get(j) - ts_block.get(j - 1);

            if (epsilon_v < value_delta_min) {
                value_delta_min = epsilon_v;
            }

            if (epsilon_v > value_delta_max) {
                value_delta_max = epsilon_v;
            }
        }
        min_delta.add(ts_block.get(i*block_size));
        min_delta.add(value_delta_min);
        min_delta.add(getBitWith(value_delta_max-value_delta_min));

        for (int j = i*block_size+1; j < (i+1)*block_size; j++) {
            int epsilon_v = ts_block.get(j) - value_delta_min - ts_block.get(j - 1);
            ts_block_delta.add(epsilon_v);
        }
        return ts_block_delta;
    }
    public static ArrayList<Integer> getBitWith(ArrayList<Integer> ts_block) {
        ArrayList<Integer> ts_block_bit_width = new ArrayList<>();
        for (int integers : ts_block) {
            ts_block_bit_width.add(getBitWith(integers));
        }
        return ts_block_bit_width;
    }

    public static ArrayList<Byte> encode2Bytes(
            ArrayList<Integer> ts_block,
            ArrayList<Integer> min_delta) {
        ArrayList<Byte> encoded_result = new ArrayList<>();

        // encode value0
        byte[] value0_byte = int2Bytes(min_delta.get(0));
        for (byte b : value0_byte) encoded_result.add(b);

        // encode theta
        byte[] value_min_byte = int2Bytes(min_delta.get(1));
        for (byte b : value_min_byte) encoded_result.add(b);

        // encode value
        byte[] max_bit_width_value_byte = intByte2Bytes(min_delta.get(2));
        for (byte b : max_bit_width_value_byte) encoded_result.add(b);
        byte[] value_bytes = bitPacking(ts_block, 0,min_delta.get(2));
        for (byte b : value_bytes) encoded_result.add(b);


        return encoded_result;
    }

    public static ArrayList<Byte> BOSEncoder(
            ArrayList<Integer> data, int block_size, String dataset_name, int q,int k) {
        block_size++;
//        int length_all = data.size();
//        byte[] length_all_bytes1 = int2Bytes(length_all);
//        byte[] encoded_result1 = new byte[length_all*4];
//        System.arraycopy(length_all_bytes1, 0, encoded_result1, 0, length_all_bytes1.length);
//        int encode_pos = length_all_bytes1.length;
//        int block_num = length_all / block_size;
//        byte[] block_size_byte = int2Bytes(block_size);
//        System.arraycopy(block_size_byte, 0, encoded_result1, encode_pos, block_size_byte.length);

        ArrayList<Byte> encoded_result = new ArrayList<Byte>();
        int length_all = data.size();
        byte[] length_all_bytes = int2Bytes(length_all);
        for (byte b : length_all_bytes) encoded_result.add(b);
        int block_num = length_all / block_size;

        byte[] block_size_byte = int2Bytes(block_size);
        for (byte b : block_size_byte) encoded_result.add(b);


//        for (int i = 0; i < 1; i++) {
        for (int i = 0; i < block_num; i++) {

            ArrayList<Integer> min_delta = new ArrayList<>();
            ArrayList<Integer> ts_block_delta = getAbsDeltaTsBlock(data, i, block_size, min_delta);
            ArrayList<Byte> cur_encoded_result =  encode2Bytes(ts_block_delta, min_delta);
            encoded_result.addAll(cur_encoded_result);

        }

        int remaining_length = length_all - block_num * block_size;
        if (remaining_length <= 3) {
            for (int i = remaining_length; i > 0; i--) {
                byte[] timestamp_end_bytes = int2Bytes(data.get(data.size() - i));
                for (byte b : timestamp_end_bytes) encoded_result.add(b);
            }

        } else {
            ArrayList<Integer> ts_block = new ArrayList<>();

            for (int j = block_num * block_size; j < length_all; j++) {
                ts_block.add(data.get(j));
            }

            int supple_length;
            if (remaining_length % 8 == 0) {
                supple_length = 1;
            } else if (remaining_length % 8 == 1) {
                supple_length = 0;
            } else {
                supple_length = 9 - remaining_length % 8;
            }

            ArrayList<Integer> min_delta = new ArrayList<>();
            ArrayList<Integer> ts_block_delta = getAbsDeltaTsBlock(ts_block, min_delta);

            for (int s = 0; s < supple_length; s++) {
                ts_block_delta.add(0);
            }

            ArrayList<Byte> cur_encoded_result = encode2Bytes(ts_block_delta, min_delta);
            encoded_result.addAll(cur_encoded_result);
        }


        return encoded_result;
    }

    public static int BOSBlockDecoder(ArrayList<Byte> encoded, int decode_pos, ArrayList<Integer> value_list, int block_size) {

        int value0 = bytes2Integer(encoded, decode_pos, 4);
        decode_pos += 4;
        value_list.add(value0);

        int min_delta = bytes2Integer(encoded, decode_pos, 4);
        decode_pos += 4;


        int bit_width_final = bytes2Integer(encoded, decode_pos, 1);
        decode_pos += 1;


        ArrayList<Integer> final_normal =  decodeBitPacking(encoded, decode_pos, bit_width_final, block_size);

        int pre_v = value0;

        value_list.add(pre_v);
        for (int i = 0; i < block_size - 1; i++) {
            int current_delta = final_normal.get(i) + min_delta;
            pre_v = current_delta + pre_v;
            value_list.add(pre_v);
        }
        return decode_pos;
    }

    public static ArrayList<Integer> BOSDecoder(ArrayList<Byte> encoded) {

        int decode_pos = 0;
        int length_all = bytes2Integer(encoded, decode_pos, 4);
        decode_pos += 4;
        int block_size = bytes2Integer(encoded, decode_pos, 4);
        decode_pos += 4;

        int block_num = length_all / block_size;
        int remain_length = length_all - block_num * block_size;
        int zero_number;
        if (remain_length % 8 == 0) {
            zero_number = 1;
        } else if (remain_length % 8 == 1) {
            zero_number = 0;
        } else {
            zero_number = 9 - remain_length % 8;
        }
        ArrayList<Integer> value_list = new ArrayList<>();
//        for (int k = 0; k < 1; k++) {
        for (int k = 0; k < block_num; k++) {
            decode_pos = BOSBlockDecoder(encoded, decode_pos, value_list, block_size);
        }

        if (remain_length <= 3) {
            for (int i = 0; i < remain_length; i++) {
                int value_end = bytes2Integer(encoded, decode_pos, 4);
                decode_pos += 4;
                value_list.add(value_end);
            }
        } else {
            decode_pos = BOSBlockDecoder(encoded, decode_pos, value_list, remain_length + zero_number);
        }
        return value_list;
    }

    @Test
    public void ts2diff() throws IOException {
        String parent_dir = "/Users/xiaojinzhao/Desktop/encoding-outlier/"; ///Users/xiaojinzhao/Desktop
//        String parent_dir = "/Users/zihanguo/Downloads/outliier_code/encoding-outlier/";
        String output_parent_dir = parent_dir + "vldb/compression_ratio/ts2diff_block_size";
        String input_parent_dir = parent_dir + "trans_data/";
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

//        String parent_dir = "C:\\Users\\Jinnsjao Shawl\\Documents\\GitHub\\encoding-outlier\\";
//        String output_parent_dir = parent_dir + "vldb\\compression_ratio\\outlier";
//        String input_parent_dir = parent_dir + "iotdb_test_small\\";
//        ArrayList<String> input_path_list = new ArrayList<>();
//        ArrayList<String> output_path_list = new ArrayList<>();
//        ArrayList<String> dataset_name = new ArrayList<>();
//        ArrayList<Integer> dataset_block_size = new ArrayList<>();
//        dataset_name.add("CS-Sensors");
//        dataset_name.add("Metro-Traffic");
//        dataset_name.add("USGS-Earthquakes");
//        dataset_name.add("YZ-Electricity");
//        dataset_name.add("GW-Magnetic");
//        dataset_name.add("TY-Fuel");
//        dataset_name.add("Cyber-Vehicle");
//        dataset_name.add("Vehicle-Charge");
//        dataset_name.add("Nifty-Stocks");
//        dataset_name.add("TH-Climate");
//        dataset_name.add("TY-Transport");
//        dataset_name.add("EPM-Education");
//
//        for (int i = 0; i < dataset_name.size(); i++) {
//            input_path_list.add(input_parent_dir + dataset_name.get(i));
//        }
//
//        output_path_list.add(output_parent_dir + "\\CS-Sensors_ratio.csv"); // 0
//        dataset_block_size.add(1024);
//        output_path_list.add(output_parent_dir + "\\Metro-Traffic_ratio.csv");// 1
//        dataset_block_size.add(512);
//        output_path_list.add(output_parent_dir + "\\USGS-Earthquakes_ratio.csv");// 2
//        dataset_block_size.add(512);
//        output_path_list.add(output_parent_dir + "\\YZ-Electricity_ratio.csv"); // 3
//        dataset_block_size.add(256);
//        output_path_list.add(output_parent_dir + "\\GW-Magnetic_ratio.csv"); //4
//        dataset_block_size.add(128);
//        output_path_list.add(output_parent_dir + "\\TY-Fuel_ratio.csv");//5
//        dataset_block_size.add(64);
//        output_path_list.add(output_parent_dir + "\\Cyber-Vehicle_ratio.csv"); //6
//        dataset_block_size.add(128);
//        output_path_list.add(output_parent_dir + "\\Vehicle-Charge_ratio.csv");//7
//        dataset_block_size.add(512);
//        output_path_list.add(output_parent_dir + "\\Nifty-Stocks_ratio.csv");//8
//        dataset_block_size.add(256);
//        output_path_list.add(output_parent_dir + "\\TH-Climate_ratio.csv");//9
//        dataset_block_size.add(512);
//        output_path_list.add(output_parent_dir + "\\TY-Transport_ratio.csv");//10
//        dataset_block_size.add(512);
//        output_path_list.add(output_parent_dir + "\\EPM-Education_ratio.csv");//11
//        dataset_block_size.add(512);


        ArrayList<Integer> columnIndexes = new ArrayList<>(); // set the column indexes of compressed
        for (int i = 0; i < 2; i++) {
            columnIndexes.add(i, i);
        }

//        for (int file_i = 3; file_i < 4; file_i++) {
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
                    "Compressed Size",
                    "Compression Ratio"
            };
            writer.writeRecord(head); // write header to output file

            assert tempList != null;

            for (File f : tempList) {
//                f = tempList[2];
                System.out.println(f);
                InputStream inputStream = Files.newInputStream(f.toPath());

                CsvReader loader = new CsvReader(inputStream, StandardCharsets.UTF_8);
                ArrayList<Integer> data1 = new ArrayList<>();
                ArrayList<Integer> data2 = new ArrayList<>();
                ArrayList<Integer> data_decoded = new ArrayList<>();


//                for (int index : columnIndexes) {
                // add a column to "data"
//                    System.out.println(index);

                loader.readHeaders();
//                    data.clear();
                while (loader.readRecord()) {
//                        String value = loader.getValues()[index];
                    data1.add(Integer.valueOf(loader.getValues()[0]));
                    data2.add(Integer.valueOf(loader.getValues()[1]));
//                        data.add(Integer.valueOf(value));
                }
//                    System.out.println(data2);
                inputStream.close();

                long encodeTime = 0;
                long decodeTime = 0;
                double ratio = 0;
                double compressed_size = 0;
                int repeatTime2 = 10;
                for (int i = 0; i < repeatTime; i++) {

                    ArrayList<Byte> buffer2 = new ArrayList<>();
                    long s = System.nanoTime();
                    for (int repeat = 0; repeat < repeatTime2; repeat++) {
                        buffer2 = BOSEncoder(data2, dataset_block_size.get(file_i), dataset_name.get(file_i), 1, 1);
                    }
//                        System.out.println(buffer2.size());
//                            buffer_bits = ReorderingRegressionEncoder(data, dataset_block_size.get(file_i), dataset_name.get(file_i));

                    long e = System.nanoTime();
                    encodeTime += ((e - s) / repeatTime2);
                    compressed_size += buffer2.size();
                    double ratioTmp = (double) compressed_size / (double) (data1.size() * Integer.BYTES);
                    ratio += ratioTmp;
                    s = System.nanoTime();
//                    for (int repeat = 0; repeat < repeatTime2; repeat++)
//                        data_decoded = BOSDecoder(buffer2);
                    e = System.nanoTime();
                    decodeTime += ((e - s) / repeatTime2);
                }

                ratio /= repeatTime;
                compressed_size /= repeatTime;
                encodeTime /= repeatTime;
                decodeTime /= repeatTime;

                String[] record = {
                        f.toString(),
                        "TS_2DIFF",
                        String.valueOf(encodeTime),
                        String.valueOf(decodeTime),
                        String.valueOf(data1.size()),
                        String.valueOf(compressed_size),
                        String.valueOf(ratio)
                };
                writer.writeRecord(record);
                System.out.println(ratio);

//                }

//   break;
            }
            writer.close();

        }
    }


    @Test
    public void ts2diffVaryBlockSize() throws IOException {
        String parent_dir = "/Users/xiaojinzhao/Desktop/encoding-outlier/"; ///Users/xiaojinzhao/Desktop
//        String parent_dir = "/Users/zihanguo/Downloads/outliier_code/encoding-outlier/";
        String output_parent_dir = parent_dir + "vldb/compression_ratio/block_size_ts2diff";
        String input_parent_dir = parent_dir + "trans_data/";
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

//        String parent_dir = "C:\\Users\\Jinnsjao Shawl\\Documents\\GitHub\\encoding-outlier\\";
//        String output_parent_dir = parent_dir + "vldb\\compression_ratio\\outlier";
//        String input_parent_dir = parent_dir + "iotdb_test_small\\";
//        ArrayList<String> input_path_list = new ArrayList<>();
//        ArrayList<String> output_path_list = new ArrayList<>();
//        ArrayList<String> dataset_name = new ArrayList<>();
//        ArrayList<Integer> dataset_block_size = new ArrayList<>();
//        dataset_name.add("CS-Sensors");
//        dataset_name.add("Metro-Traffic");
//        dataset_name.add("USGS-Earthquakes");
//        dataset_name.add("YZ-Electricity");
//        dataset_name.add("GW-Magnetic");
//        dataset_name.add("TY-Fuel");
//        dataset_name.add("Cyber-Vehicle");
//        dataset_name.add("Vehicle-Charge");
//        dataset_name.add("Nifty-Stocks");
//        dataset_name.add("TH-Climate");
//        dataset_name.add("TY-Transport");
//        dataset_name.add("EPM-Education");
//
//        for (int i = 0; i < dataset_name.size(); i++) {
//            input_path_list.add(input_parent_dir + dataset_name.get(i));
//        }
//
//        output_path_list.add(output_parent_dir + "\\CS-Sensors_ratio.csv"); // 0
//        dataset_block_size.add(1024);
//        output_path_list.add(output_parent_dir + "\\Metro-Traffic_ratio.csv");// 1
//        dataset_block_size.add(512);
//        output_path_list.add(output_parent_dir + "\\USGS-Earthquakes_ratio.csv");// 2
//        dataset_block_size.add(512);
//        output_path_list.add(output_parent_dir + "\\YZ-Electricity_ratio.csv"); // 3
//        dataset_block_size.add(256);
//        output_path_list.add(output_parent_dir + "\\GW-Magnetic_ratio.csv"); //4
//        dataset_block_size.add(128);
//        output_path_list.add(output_parent_dir + "\\TY-Fuel_ratio.csv");//5
//        dataset_block_size.add(64);
//        output_path_list.add(output_parent_dir + "\\Cyber-Vehicle_ratio.csv"); //6
//        dataset_block_size.add(128);
//        output_path_list.add(output_parent_dir + "\\Vehicle-Charge_ratio.csv");//7
//        dataset_block_size.add(512);
//        output_path_list.add(output_parent_dir + "\\Nifty-Stocks_ratio.csv");//8
//        dataset_block_size.add(256);
//        output_path_list.add(output_parent_dir + "\\TH-Climate_ratio.csv");//9
//        dataset_block_size.add(512);
//        output_path_list.add(output_parent_dir + "\\TY-Transport_ratio.csv");//10
//        dataset_block_size.add(512);
//        output_path_list.add(output_parent_dir + "\\EPM-Education_ratio.csv");//11
//        dataset_block_size.add(512);


        ArrayList<Integer> columnIndexes = new ArrayList<>(); // set the column indexes of compressed
        for (int i = 0; i < 2; i++) {
            columnIndexes.add(i, i);
        }

//        for (int file_i = 3; file_i < 4; file_i++) {
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
                    "Compressed Size",
                    "Block Size",
                    "Compression Ratio"
            };
            writer.writeRecord(head); // write header to output file

            assert tempList != null;

            for (File f : tempList) {
//                f = tempList[2];
                System.out.println(f);
                InputStream inputStream = Files.newInputStream(f.toPath());

                CsvReader loader = new CsvReader(inputStream, StandardCharsets.UTF_8);
                ArrayList<Integer> data1 = new ArrayList<>();
                ArrayList<Integer> data2 = new ArrayList<>();
                ArrayList<Integer> data_decoded = new ArrayList<>();


//                for (int index : columnIndexes) {
                // add a column to "data"
//                    System.out.println(index);

                loader.readHeaders();
//                    data.clear();
                while (loader.readRecord()) {
//                        String value = loader.getValues()[index];
                    data1.add(Integer.valueOf(loader.getValues()[0]));
                    data2.add(Integer.valueOf(loader.getValues()[1]));
//                        data.add(Integer.valueOf(value));
                }
//                    System.out.println(data2);
                inputStream.close();
//                for (int block_size_i = 8; block_size_i < 9; block_size_i++) {
                for (int block_size_i = 13; block_size_i > 3; block_size_i--) {
                    int block_size = (int) Math.pow(2, block_size_i);
                    System.out.println(block_size);
                    long encodeTime = 0;
                    long decodeTime = 0;
                    double ratio = 0;
                    double compressed_size = 0;
                    int repeatTime2 = 10;
                    for (int i = 0; i < repeatTime; i++) {

                        ArrayList<Byte> buffer2 = new ArrayList<>();
                        long s = System.nanoTime();
                        for (int repeat = 0; repeat < repeatTime2; repeat++) {
                            buffer2 = BOSEncoder(data2, block_size, dataset_name.get(file_i), 1, 1);
                        }
//                        System.out.println(buffer2.size());
//                            buffer_bits = ReorderingRegressionEncoder(data, dataset_block_size.get(file_i), dataset_name.get(file_i));

                        long e = System.nanoTime();
                        encodeTime += ((e - s) / repeatTime2);
                        compressed_size += buffer2.size();
                        double ratioTmp = (double) compressed_size / (double) (data1.size() * Integer.BYTES);
                        ratio += ratioTmp;
                        s = System.nanoTime();
//                    for (int repeat = 0; repeat < repeatTime2; repeat++)
//                        data_decoded = BOSDecoder(buffer2);
                        e = System.nanoTime();
                        decodeTime += ((e - s) / repeatTime2);
                    }

                    ratio /= repeatTime;
                    compressed_size /= repeatTime;
                    encodeTime /= repeatTime;
                    decodeTime /= repeatTime;

                    String[] record = {
                            f.toString(),
                            "TS_2DIFF",
                            String.valueOf(encodeTime),
                            String.valueOf(decodeTime),
                            String.valueOf(data1.size()),
                            String.valueOf(compressed_size),
                            String.valueOf(block_size_i),
                            String.valueOf(ratio)
                    };
                    writer.writeRecord(record);
                    System.out.println(ratio);
                }
//                }

//   break;
            }
            writer.close();

        }
    }

    @Test
    public void ts2diffVaryK() throws IOException {
        String parent_dir = "/Users/xiaojinzhao/Desktop/encoding-outlier/"; ///Users/xiaojinzhao/Desktop
//        String parent_dir = "/Users/zihanguo/Downloads/outliier_code/encoding-outlier/";
        String output_parent_dir = parent_dir + "vldb/compression_ratio/vary_k_ts2diff";
        String input_parent_dir = parent_dir + "trans_data/";
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
                    "Compressed Size",
                    "k",
                    "Compression Ratio"
            };
            writer.writeRecord(head); // write header to output file

            assert tempList != null;

            for (File f : tempList) {

                System.out.println(f);
                InputStream inputStream = Files.newInputStream(f.toPath());

                CsvReader loader = new CsvReader(inputStream, StandardCharsets.UTF_8);
                ArrayList<Integer> data1 = new ArrayList<>();
                ArrayList<Integer> data2 = new ArrayList<>();
                ArrayList<Integer> data_decoded = new ArrayList<>();


                loader.readHeaders();
                while (loader.readRecord()) {

                    data1.add(Integer.valueOf(loader.getValues()[0]));
                    data2.add(Integer.valueOf(loader.getValues()[1]));

                }

                inputStream.close();
                for (int k = 1; k < 8; k++) {
                    System.out.println(k);
                    long encodeTime = 0;
                    long decodeTime = 0;
                    double ratio = 0;
                    double compressed_size = 0;
                    int repeatTime2 = 10;
                    for (int i = 0; i < repeatTime; i++) {
                        long s = System.nanoTime();
                        ArrayList<Byte> buffer2 = new ArrayList<>();
                        for (int repeat = 0; repeat < repeatTime2; repeat++) {
                            buffer2 = BOSEncoder(data2, dataset_block_size.get(file_i), dataset_name.get(file_i),1,1);
                        }

                        long e = System.nanoTime();
                        encodeTime += ((e - s) / repeatTime2);
                        compressed_size += buffer2.size();
                        double ratioTmp = (double) compressed_size / (double) (data1.size() * Integer.BYTES);
                        ratio += ratioTmp;
                        s = System.nanoTime();
                        for (int repeat = 0; repeat < repeatTime2; repeat++)
                            data_decoded = BOSDecoder(buffer2);
                        e = System.nanoTime();
                        decodeTime += ((e - s) / repeatTime2);
                    }

                    ratio /= repeatTime;
                    compressed_size /= repeatTime;
                    encodeTime /= repeatTime;
                    decodeTime /= repeatTime;

                    String[] record = {
                            f.toString(),
                            "TS_2DIFF",
                            String.valueOf(encodeTime),
                            String.valueOf(decodeTime),
                            String.valueOf(data1.size()),
                            String.valueOf(compressed_size),
                            String.valueOf(k),
                            String.valueOf(ratio)
                    };
                    writer.writeRecord(record);
                    System.out.println(ratio);
                }

            }
            writer.close();

        }
    }

    @Test
    public void ts2diffVaryQ() throws IOException {
        String parent_dir = "/Users/xiaojinzhao/Desktop/encoding-outlier/"; ///Users/xiaojinzhao/Desktop
//        String parent_dir = "/Users/zihanguo/Downloads/outliier_code/encoding-outlier/";
        String output_parent_dir = parent_dir + "vldb/compression_ratio/vary_q_ts2diff";
        String input_parent_dir = parent_dir + "trans_data/";
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
                    "Compressed Size",
                    "Q",
                    "Compression Ratio"
            };
            writer.writeRecord(head); // write header to output file

            assert tempList != null;

            for (File f : tempList) {

                System.out.println(f);
                InputStream inputStream = Files.newInputStream(f.toPath());

                CsvReader loader = new CsvReader(inputStream, StandardCharsets.UTF_8);
                ArrayList<Integer> data1 = new ArrayList<>();
                ArrayList<Integer> data2 = new ArrayList<>();
                ArrayList<Integer> data_decoded = new ArrayList<>();


                loader.readHeaders();
                while (loader.readRecord()) {
                    data1.add(Integer.valueOf(loader.getValues()[0]));
                    data2.add(Integer.valueOf(loader.getValues()[1]));
                }
                inputStream.close();
                for (int q_exp = 0; q_exp < 8; q_exp++) {
                    int q = (int) Math.pow(2, q_exp);
                    System.out.println(q);
                    long encodeTime = 0;
                    long decodeTime = 0;
                    double ratio = 0;
                    double compressed_size = 0;
                    int repeatTime2 = 10;
                    for (int i = 0; i < repeatTime; i++) {
                        long s = System.nanoTime();
                        ArrayList<Byte> buffer2 = new ArrayList<>();
                        for (int repeat = 0; repeat < repeatTime2; repeat++) {
                            buffer2 = BOSEncoder(data2, dataset_block_size.get(file_i), dataset_name.get(file_i),1,1);
                        }

                        long e = System.nanoTime();
                        encodeTime += ((e - s) / repeatTime2);
                        compressed_size += buffer2.size();
                        double ratioTmp = (double) compressed_size / (double) (data1.size() * Integer.BYTES);
                        ratio += ratioTmp;
                        s = System.nanoTime();
                        for (int repeat = 0; repeat < repeatTime2; repeat++)
                            data_decoded = BOSDecoder(buffer2);
                        e = System.nanoTime();
                        decodeTime += ((e - s) / repeatTime2);
                    }

                    ratio /= repeatTime;
                    compressed_size /= repeatTime;
                    encodeTime /= repeatTime;
                    decodeTime /= repeatTime;

                    String[] record = {
                            f.toString(),
                            "TS_2DIFF",
                            String.valueOf(encodeTime),
                            String.valueOf(decodeTime),
                            String.valueOf(data1.size()),
                            String.valueOf(compressed_size),
                            String.valueOf(q_exp),
                            String.valueOf(ratio)
                    };
                    writer.writeRecord(record);
                    System.out.println(ratio);
                }
            }
            writer.close();

        }
    }
}
