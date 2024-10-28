package org.apache.iotdb.tsfile.encoding;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;
import org.apache.commons.compress.archivers.sevenz.SevenZOutputFile;
import org.apache.commons.math3.transform.DctNormalization;
import org.apache.commons.math3.transform.FastCosineTransformer;
import org.apache.commons.math3.transform.TransformType;
import org.apache.iotdb.tsfile.compress.ICompressor;
import org.apache.iotdb.tsfile.encoding.decoder.Decoder;
import org.apache.iotdb.tsfile.encoding.encoder.Encoder;
import org.apache.iotdb.tsfile.encoding.encoder.TSEncodingBuilder;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.filter.operator.In;
import org.junit.Test;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

class ValueWithIndex {
    double value;
    int index;

    public ValueWithIndex(double value, int index) {
        this.value = value;
        this.index = index;
    }
}

public class DCT_IDCT {

    static int blockSize = 257;// 使用的blocksize，只能使用2的幂次+1
    static int n = 3;// 选取前几个分量，需要保证n < blockSize
    public static void getTopNMaxAbsoluteValues(double[] array, int n, double[] values, int[] indices) {
        if (array == null || array.length < n) {
            throw new IllegalArgumentException("Array must contain at least " + n + " elements.");
        }

        List<ValueWithIndex> list = new ArrayList<>();
        for (int i = 0; i < array.length; i++) {
            list.add(new ValueWithIndex(array[i], i));
        }

        list.sort((o1, o2) -> Double.compare(Math.abs(o2.value), Math.abs(o1.value)));

        for (int i = 0; i < n; i++) {
            values[i] = list.get(i).value;
            indices[i] = list.get(i).index;
        }
    }
    private static final FastCosineTransformer dctTransformer = new FastCosineTransformer(DctNormalization.ORTHOGONAL_DCT_I);

    public static ArrayList<Byte> encode(int[] timeSeries) throws IOException {
        int numberOfBlocks = (timeSeries.length + blockSize - 1) / blockSize;
        ArrayList<Byte> resList = new ArrayList<>();
        ByteBuffer byteBuffer = ByteBuffer.allocate(4);
        byteBuffer.putInt(numberOfBlocks);
        byte[] byteArray = byteBuffer.array();
        for (byte temp:byteArray){
            resList.add(temp);
        }

        for (int i = 0; i < numberOfBlocks; i++) {
            if (i != numberOfBlocks - 1) {
                int start = i * blockSize;
                int end = Math.min(start + blockSize, timeSeries.length);

                int[] block = new int[end - start];
                System.arraycopy(timeSeries, start, block, 0, end - start);

                double[] d_ts = new double[block.length];
                int index = 0;
                for (int value : block) {
                    d_ts[index] = value;
                    index++;
                }
                double[] res = dctTransformer.transform(d_ts, TransformType.FORWARD);
                double[] values = new double[n];
                int[] indices = new int[n];

                try {
                    getTopNMaxAbsoluteValues(res, n, values, indices);
                    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
                    float[] f_new_res = new float[block.length];
                    double[] d_f_new_res = new double[block.length];
                    for (int k = 0; k < n; k++){
                        f_new_res[indices[k]] = (float)values[k];
                        d_f_new_res[indices[k]] = (double)f_new_res[indices[k]];
                    }
                    double[] new_dts = dctTransformer.transform(d_f_new_res, TransformType.INVERSE);
                    Encoder encoder =
                            TSEncodingBuilder.getEncodingBuilder(TSEncoding.TS_2DIFF).getEncoder(TSDataType.INT32);
                    int[] new_its = new int[new_dts.length];
                    for (int k = 0; k < new_dts.length; k++) {
                         new_its[k] = (int) Math.round(new_dts[k]);
                    }
                    int[] err = new int[new_dts.length];
                    for (int k = 0; k < new_dts.length; k++) {
                        err[k] = new_its[k] - block[k];
                    }
                    for (int k = 0; k < new_dts.length; k++) {
                        encoder.encode(err[k],buffer);
                    }
                    encoder.flush(buffer);
                    byte[] elems = buffer.toByteArray();
                    for (int k = 0; k < n; k++){
                        byteBuffer = ByteBuffer.allocate(4);
                        byteBuffer.putInt(indices[k]);
                        byteArray = byteBuffer.array();
                        for (byte temp:byteArray){
                            resList.add(temp);
                        }
                        byteBuffer = ByteBuffer.allocate(4);
                        byteBuffer.putFloat((float)values[k]);
                        byteArray = byteBuffer.array();
                        for (byte temp:byteArray){
                            resList.add(temp);
                        }
                    }
                    byteBuffer = ByteBuffer.allocate(4);
                    byteBuffer.putInt(elems.length);
                    byteArray = byteBuffer.array();
                    for (byte temp:byteArray){
                        resList.add(temp);
                    }
                    for (byte temp:elems){
                        resList.add(temp);
                    }
                } catch (IllegalArgumentException e) {
                    System.out.println(e.getMessage());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            } else {
                int start = i * blockSize;
                int end = Math.min(start + blockSize, timeSeries.length);

                int[] block = new int[end - start];
                System.arraycopy(timeSeries, start, block, 0, end - start);
                Encoder encoder =
                        TSEncodingBuilder.getEncodingBuilder(TSEncoding.TS_2DIFF).getEncoder(TSDataType.INT32);
                ByteArrayOutputStream buffer = new ByteArrayOutputStream();
                for (int j : block) {
                    encoder.encode(j, buffer);
                }
                encoder.flush(buffer);
                byte[] elems = buffer.toByteArray();
                byteBuffer = ByteBuffer.allocate(4);
                byteBuffer.putInt(elems.length);
                byteArray = byteBuffer.array();
                for (byte temp:byteArray){
                    resList.add(temp);
                }
                for (byte temp:elems){
                    resList.add(temp);
                }
            }
        }
        return resList;
    }

    public static ArrayList<Integer> decode(ArrayList<Byte> encoded) throws IOException {
        ArrayList<Integer> res = new ArrayList<>();
        int cursor = 0;
        byte[] numByte = new byte[4];
        for (int i = 0; i < 4; i++){
            numByte[i] = encoded.get(cursor++);
        }
        ByteBuffer byteBuffer = ByteBuffer.wrap(numByte);
        int numberOfBlocks = byteBuffer.getInt(0);
        for (int i = 0; i < numberOfBlocks; i++) {
            if (i != numberOfBlocks - 1) {
                double[] new_res = new double[blockSize];
                for (int k = 0; k < n; k++){
                    numByte = new byte[4];
                    for (int j = 0; j < 4; j++){
                        numByte[j] = encoded.get(cursor++);
                    }
                    byteBuffer = ByteBuffer.wrap(numByte);
                    int index = byteBuffer.getInt(0);
                    numByte = new byte[4];
                    for (int j = 0; j < 4; j++){
                        numByte[j] = encoded.get(cursor++);
                    }
                    byteBuffer = ByteBuffer.wrap(numByte);
                    float value = byteBuffer.getFloat(0);
                    new_res[index] = (double)value;
                }
                double[] new_dts = dctTransformer.transform(new_res, TransformType.INVERSE);
                Decoder decoder = Decoder.getDecoderByType(TSEncoding.TS_2DIFF, TSDataType.INT32);
                int[] new_its = new int[new_dts.length];
                for (int k = 0; k < new_dts.length; k++) {
                    new_its[k] = (int) Math.round(new_dts[k]);
                }
                numByte = new byte[4];
                for (int j = 0; j < 4; j++){
                    numByte[j] = encoded.get(cursor++);
                }
                byteBuffer = ByteBuffer.wrap(numByte);
                int length = byteBuffer.getInt(0);
                numByte = new byte[length];
                for (int j = 0; j < length; j++){
                    numByte[j] = encoded.get(cursor++);
                }
                byteBuffer = ByteBuffer.wrap(numByte);
                int j = 0;
                while (decoder.hasNext(byteBuffer)) {
                    int temp = decoder.readInt(byteBuffer);
                    res.add(new_its[j++] - temp);
                }
            } else {
                Decoder decoder = Decoder.getDecoderByType(TSEncoding.TS_2DIFF, TSDataType.INT32);
                numByte = new byte[4];
                for (int j = 0; j < 4; j++){
                    numByte[j] = encoded.get(cursor++);
                }
                byteBuffer = ByteBuffer.wrap(numByte);
                int length = byteBuffer.getInt(0);
                numByte = new byte[length];
                for (int j = 0; j < length; j++){
                    numByte[j] = encoded.get(cursor++);
                }
                byteBuffer = ByteBuffer.wrap(numByte);
                while (decoder.hasNext(byteBuffer)) {
                    int temp = decoder.readInt(byteBuffer);
                    res.add(temp);
                }
            }
        }
        return res;
    }

    public static void main1(String[] args) throws IOException {}

    @Test
    public void main1() throws IOException {
//        String parent_dir = "/Users/xiaojinzhao/Documents/GitHub/encoding-outlier/";// your data path
        String parent_dir = "/Users/zihanguo/Downloads/R/outlier/outliier_code/encoding-outlier/";
        String output_parent_dir = parent_dir + "icde0802/supply_experiment/R3O2_compare_compression/compression_ratio/dct_comp";
        String input_parent_dir = parent_dir + "icde0802/supply_experiment/R3O2_compare_compression/data/";
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

        for (String value : dataset_name) {
            input_path_list.add(input_parent_dir + value);
            dataset_block_size.add(1024);
        }

        output_path_list.add(output_parent_dir + "/CS-Sensors_ratio.csv"); // 0
//        dataset_block_size.add(1024);
        output_path_list.add(output_parent_dir + "/Metro-Traffic_ratio.csv");// 1
//        dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/USGS-Earthquakes_ratio.csv");// 2
//        dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/YZ-Electricity_ratio.csv"); // 3
//        dataset_block_size.add(256);
        output_path_list.add(output_parent_dir + "/GW-Magnetic_ratio.csv"); //4
//        dataset_block_size.add(1024);
        output_path_list.add(output_parent_dir + "/TY-Fuel_ratio.csv");//5
//        dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/Cyber-Vehicle_ratio.csv"); //6
//        dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/Vehicle-Charge_ratio.csv");//7
//        dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/Nifty-Stocks_ratio.csv");//8
//        dataset_block_size.add(1024);
        output_path_list.add(output_parent_dir + "/TH-Climate_ratio.csv");//9
//        dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/TY-Transport_ratio.csv");//10
//        dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/EPM-Education_ratio.csv");//11
//        dataset_block_size.add(1024);

        int repeatTime2 = 50;
//        for (int file_i = 8; file_i < 9; file_i++) {
        long compressTime = 0;
        long uncompressTime = 0;
        for (int file_i = 0; file_i < input_path_list.size(); file_i++) {
//        for (int file_i = input_path_list.size()-1; file_i >=0 ; file_i--) {

            String inputPath = input_path_list.get(file_i);
            System.out.println(inputPath);
            String Output = output_path_list.get(file_i);

            File file = new File(inputPath);
            File[] tempList = file.listFiles();

            CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);

            String[] head = {
                    "Input Direction",
                    "Encoding Algorithm",
//                    "Compress Algorithm",
                    "Encoding Time",
                    "Decoding Time",
                    "Points",
                    "Compressed Size",
                    "Compression Ratio"
            };
            writer.writeRecord(head); // write header to output file

            assert tempList != null;

            for (File f : tempList) {
                System.out.println(f);
                InputStream inputStream = Files.newInputStream(f.toPath());

                CsvReader loader = new CsvReader(inputStream, StandardCharsets.UTF_8);
                ArrayList<Integer> data2 = new ArrayList<>();

                loader.readHeaders();
                while (loader.readRecord()) {
//                    data1.add(Integer.valueOf(loader.getValues()[0]));
                    data2.add(Integer.valueOf(loader.getValues()[0]));
                }
                inputStream.close();

                int[] data2_arr = new int[data2.size()];
                for(int i = 0;i<data2.size();i++){
                    data2_arr[i] = data2.get(i);
                }
                double ratio = 0;
                double compressed_size = 0;
                long s = System.nanoTime();
                ArrayList<Byte> res = new ArrayList<>();
                for (int repeat = 0; repeat < repeatTime2; repeat++) {
                    res = DCT_IDCT.encode(data2_arr);
                }
                long e = System.nanoTime();
                compressTime += ((e - s) / repeatTime2);

                // test compression ratio and compressed size
                compressed_size += res.size();
                double ratioTmp = compressed_size / (double) (data2.size() * Integer.BYTES);
                ratio += ratioTmp;
                s = System.nanoTime();
                for (int repeat = 0; repeat < repeatTime2; repeat++){
                    ArrayList<Integer> b = DCT_IDCT.decode(res);
                }
                e = System.nanoTime();
                uncompressTime += ((e - s) / repeatTime2);


                String[] record = {
                        f.toString(),
                        "BOS+DCT",
                        String.valueOf(compressTime),
                        String.valueOf(uncompressTime),
                        String.valueOf(data2.size()),
                        String.valueOf(compressed_size),
                        String.valueOf(ratio)
                };
                writer.writeRecord(record);
                System.out.println(ratio);
            }
            writer.close();
        }
    }

    @Test
    public void main2() throws IOException {
//        String parent_dir = "/Users/xiaojinzhao/Documents/GitHub/encoding-outlier/";// your data path
        String parent_dir = "/Users/zihanguo/Downloads/R/outlier/outliier_code/encoding-outlier/";
        String output_parent_dir = parent_dir + "icde0802/supply_experiment/R3O2_compare_compression/compression_ratio/dct_comp2";
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

        for (String value : dataset_name) {
            input_path_list.add(input_parent_dir + value);
            dataset_block_size.add(1024);
        }

        output_path_list.add(output_parent_dir + "/CS-Sensors_ratio.csv"); // 0
//        dataset_block_size.add(1024);
        output_path_list.add(output_parent_dir + "/Metro-Traffic_ratio.csv");// 1
//        dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/USGS-Earthquakes_ratio.csv");// 2
//        dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/YZ-Electricity_ratio.csv"); // 3
//        dataset_block_size.add(256);
        output_path_list.add(output_parent_dir + "/GW-Magnetic_ratio.csv"); //4
//        dataset_block_size.add(1024);
        output_path_list.add(output_parent_dir + "/TY-Fuel_ratio.csv");//5
//        dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/Cyber-Vehicle_ratio.csv"); //6
//        dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/Vehicle-Charge_ratio.csv");//7
//        dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/Nifty-Stocks_ratio.csv");//8
//        dataset_block_size.add(1024);
        output_path_list.add(output_parent_dir + "/TH-Climate_ratio.csv");//9
//        dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/TY-Transport_ratio.csv");//10
//        dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/EPM-Education_ratio.csv");//11
//        dataset_block_size.add(1024);

        int repeatTime2 = 50;
//        for (int file_i = 8; file_i < 9; file_i++) {
        long compressTime = 0;
        long uncompressTime = 0;
        for (int file_i = 0; file_i < input_path_list.size(); file_i++) {
//        for (int file_i = input_path_list.size()-1; file_i >=0 ; file_i--) {

            String inputPath = input_path_list.get(file_i);
            System.out.println(inputPath);
            String Output = output_path_list.get(file_i);

            File file = new File(inputPath);
            File[] tempList = file.listFiles();

            CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);

            String[] head = {
                    "Input Direction",
                    "Encoding Algorithm",
//                    "Compress Algorithm",
                    "Encoding Time",
                    "Decoding Time",
                    "Points",
                    "Compressed Size",
                    "Compression Ratio"
            };
            writer.writeRecord(head); // write header to output file

            assert tempList != null;

            for (File f : tempList) {
                System.out.println(f);
                InputStream inputStream = Files.newInputStream(f.toPath());

                CsvReader loader = new CsvReader(inputStream, StandardCharsets.UTF_8);
                ArrayList<Integer> data2 = new ArrayList<>();

                loader.readHeaders();
                while (loader.readRecord()) {
//                    data1.add(Integer.valueOf(loader.getValues()[0]));
                    data2.add(Integer.valueOf(loader.getValues()[1]));
                }
                inputStream.close();

                int[] data2_arr = new int[data2.size()];
                for(int i = 0;i<data2.size();i++){
                    data2_arr[i] = data2.get(i);
                }
                double ratio = 0;
                double compressed_size = 0;
                long s = System.nanoTime();
                ArrayList<Byte> res = new ArrayList<>();
                for (int repeat = 0; repeat < repeatTime2; repeat++) {
                    res = DCT_IDCT.encode(data2_arr);
                }
                long e = System.nanoTime();
                compressTime += ((e - s) / repeatTime2);

                // test compression ratio and compressed size
                compressed_size += res.size();
                double ratioTmp = compressed_size / (double) (data2.size() * Integer.BYTES);
                ratio += ratioTmp;
                s = System.nanoTime();
                for (int repeat = 0; repeat < repeatTime2; repeat++){
                    ArrayList<Integer> b = DCT_IDCT.decode(res);
                }
                e = System.nanoTime();
                uncompressTime += ((e - s) / repeatTime2);


                String[] record = {
                        f.toString(),
                        "DCT",
                        String.valueOf(compressTime),
                        String.valueOf(uncompressTime),
                        String.valueOf(data2.size()),
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