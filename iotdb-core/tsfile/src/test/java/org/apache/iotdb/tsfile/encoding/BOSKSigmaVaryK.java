package org.apache.iotdb.tsfile.encoding;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;

import static java.lang.Math.pow;
import static org.apache.iotdb.tsfile.encoding.BOSKSigmaVaryBlocksize.BOSDecoder;
import static org.apache.iotdb.tsfile.encoding.BOSKSigmaVaryBlocksize.BOSEncoder;

public class BOSKSigmaVaryK {

    public static void main(@org.jetbrains.annotations.NotNull String[] args) throws IOException {
        String parent_dir = "/Users/xiaojinzhao/Desktop/encoding-outlier/"; ///Users/xiaojinzhao/Desktop
//        String parent_dir = "/Users/zihanguo/Downloads/outliier_code/encoding-outlier/";
        String output_parent_dir = parent_dir + "vldb/compression_ratio/k_sigma";
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

//        for (int file_i = 6; file_i < 7; file_i++) {
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
                for (int k = 1; k < 8; k++) {
                    long encodeTime = 0;
                    long decodeTime = 0;
                    double ratio = 0;
                    double compressed_size = 0;
                    int repeatTime2 = 10;
                    for (int i = 0; i < repeatTime; i++) {
                        long s = System.nanoTime();
                        ArrayList<Byte> buffer1 = new ArrayList<>();
                        ArrayList<Byte> buffer2 = new ArrayList<>();
                        long buffer_bits = 0;
                        for (int repeat = 0; repeat < repeatTime2; repeat++) {
//                            buffer1 = ReorderingRegressionEncoder(data1, dataset_block_size.get(file_i), dataset_name.get(file_i));
                            buffer2 = BOSEncoder(data2, dataset_block_size.get(file_i), dataset_name.get(file_i), k);
                        }
//                        System.out.println(buffer2.size());
//                            buffer_bits = ReorderingRegressionEncoder(data, dataset_block_size.get(file_i), dataset_name.get(file_i));

                        long e = System.nanoTime();
                        encodeTime += ((e - s) / repeatTime2);
//                        compressed_size += buffer1.size();
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
                            "TS_2DIFF+KSigma",
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
//                }

//   break;
            }
            writer.close();

        }
    }

}
