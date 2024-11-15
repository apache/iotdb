package org.apache.iotdb.tsfile.encoding;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;

import org.junit.Test;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;

public class SubColumnCostTest {
    public static int getBitWidth(int num) {
        if (num == 0)
            return 1;
        else
            return 32 - Integer.numberOfLeadingZeros(num);
    }

    public static int getCount(long long1, int mask) {
        return ((int) (long1 & mask));
    }

    public static int getUniqueValue(long long1, int left_shift) {
        return ((int) ((long1) >> left_shift));
    }

    public static void int2Bytes(int integer, int encode_pos, byte[] cur_byte) {
        cur_byte[encode_pos] = (byte) (integer >> 24);
        cur_byte[encode_pos + 1] = (byte) (integer >> 16);
        cur_byte[encode_pos + 2] = (byte) (integer >> 8);
        cur_byte[encode_pos + 3] = (byte) (integer);
    }

    public static void intByte2Bytes(int integer, int encode_pos, byte[] cur_byte) {
        cur_byte[encode_pos] = (byte) (integer);
    }

    private static void long2intBytes(long integer, int encode_pos, byte[] cur_byte) {
        cur_byte[encode_pos] = (byte) (integer >> 24);
        cur_byte[encode_pos + 1] = (byte) (integer >> 16);
        cur_byte[encode_pos + 2] = (byte) (integer >> 8);
        cur_byte[encode_pos + 3] = (byte) (integer);
    }

    public static int bytes2Integer(byte[] encoded, int start, int num) {
        int value = 0;
        if (num > 4) {
            System.out.println("bytes2Integer error");
            return 0;
        }
        for (int i = 0; i < num; i++) {
            value <<= 8;
            int b = encoded[i + start] & 0xFF;
            value |= b;
        }
        return value;
    }

    private static long bytesLong2Integer(byte[] encoded, int decode_pos) {
        long value = 0;
        for (int i = 0; i < 4; i++) {
            value <<= 8;
            int b = encoded[i + decode_pos] & 0xFF;
            value |= b;
        }
        return value;
    }

    public static void pack8Values(ArrayList<Integer> values, int offset, int width, int encode_pos,
            byte[] encoded_result) {
        int bufIdx = 0;
        int valueIdx = offset;
        // remaining bits for the current unfinished Integer
        int leftBit = 0;

        while (valueIdx < 8 + offset) {
            // buffer is used for saving 32 bits as a part of result
            int buffer = 0;
            // remaining size of bits in the 'buffer'
            int leftSize = 32;

            // encode the left bits of current Integer to 'buffer'
            if (leftBit > 0) {
                buffer |= (values.get(valueIdx) << (32 - leftBit));
                leftSize -= leftBit;
                leftBit = 0;
                valueIdx++;
            }

            while (leftSize >= width && valueIdx < 8 + offset) {
                // encode one Integer to the 'buffer'
                buffer |= (values.get(valueIdx) << (leftSize - width));
                leftSize -= width;
                valueIdx++;
            }
            // If the remaining space of the buffer can not save the bits for one Integer,
            if (leftSize > 0 && valueIdx < 8 + offset) {
                // put the first 'leftSize' bits of the Integer into remaining space of the
                // buffer
                buffer |= (values.get(valueIdx) >>> (width - leftSize));
                leftBit = width - leftSize;
            }

            // put the buffer into the final result
            for (int j = 0; j < 4; j++) {
                encoded_result[encode_pos] = (byte) ((buffer >>> ((3 - j) * 8)) & 0xFF);
                encode_pos++;
                bufIdx++;
                if (bufIdx >= width) {
                    return;
                }
            }
        }

    }

    public static void unpack8Values(byte[] encoded, int offset, int width, ArrayList<Integer> result_list) {
        int byteIdx = offset;
        long buffer = 0;
        // total bits which have read from 'buf' to 'buffer'. i.e.,
        // number of available bits to be decoded.
        int totalBits = 0;
        int valueIdx = 0;

        while (valueIdx < 8) {
            // If current available bits are not enough to decode one Integer,
            // then add next byte from buf to 'buffer' until totalBits >= width
            while (totalBits < width) {
                buffer = (buffer << 8) | (encoded[byteIdx] & 0xFF);
                byteIdx++;
                totalBits += 8;
            }

            // If current available bits are enough to decode one Integer,
            // then decode one Integer one by one until left bits in 'buffer' is
            // not enough to decode one Integer.
            while (totalBits >= width && valueIdx < 8) {
                result_list.add((int) (buffer >>> (totalBits - width)));
                valueIdx++;
                totalBits -= width;
                buffer = buffer & ((1L << totalBits) - 1);
            }
        }
    }

    public static int bitPacking(ArrayList<Integer> numbers, int start, int bit_width, int encode_pos,
            byte[] encoded_result) {
        int block_num = (numbers.size() - start) / 8;
        for (int i = 0; i < block_num; i++) {
            pack8Values(numbers, start + i * 8, bit_width, encode_pos, encoded_result);
            encode_pos += bit_width;
        }

        return encode_pos;

    }

    public static ArrayList<Integer> decodeBitPacking(
            byte[] encoded, int decode_pos, int bit_width, int block_size) {
        ArrayList<Integer> result_list = new ArrayList<>();
        int block_num = (block_size - 1) / 8;

        for (int i = 0; i < block_num; i++) { // bitpacking
            unpack8Values(encoded, decode_pos, bit_width, result_list);
            decode_pos += bit_width;
        }
        return result_list;
    }

    public static int encodeOutlier2Bytes(
            ArrayList<Integer> ts_block_delta,
            int bit_width,
            int encode_pos, byte[] encoded_result) {

        encode_pos = bitPacking(ts_block_delta, 0, bit_width, encode_pos, encoded_result);

        int n_k = ts_block_delta.size();
        int n_k_b = n_k / 8;
        long cur_remaining = 0; // encoded int
        int cur_number_bits = 0; // the bit width used of encoded int
        for (int i = n_k_b * 8; i < n_k; i++) {
            long cur_value = ts_block_delta.get(i);
            int cur_bit_width = bit_width; // remaining bit width of current value

            if (cur_number_bits + bit_width >= 32) {
                cur_remaining <<= (32 - cur_number_bits);
                cur_bit_width = bit_width - 32 + cur_number_bits;
                cur_remaining += ((cur_value >> cur_bit_width));
                long2intBytes(cur_remaining, encode_pos, encoded_result);
                encode_pos += 4;

                cur_remaining = 0;
                cur_number_bits = 0;
            }

            cur_remaining <<= cur_bit_width;
            cur_number_bits += cur_bit_width;
            cur_remaining += (((cur_value << (32 - cur_bit_width)) & 0xFFFFFFFFL) >> (32 - cur_bit_width));
        }
        cur_remaining <<= (32 - cur_number_bits);
        long2intBytes(cur_remaining, encode_pos, encoded_result);
        encode_pos += 4;
        return encode_pos;

    }

    public static ArrayList<Integer> decodeOutlier2Bytes(
            byte[] encoded,
            int decode_pos,
            int bit_width,
            int length,
            ArrayList<Integer> encoded_pos_result) {

        int n_k_b = length / 8;
        int remaining = length - n_k_b * 8;
        ArrayList<Integer> result_list = new ArrayList<>(
                decodeBitPacking(encoded, decode_pos, bit_width, n_k_b * 8 + 1));
        decode_pos += n_k_b * bit_width;

        ArrayList<Long> int_remaining = new ArrayList<>();
        int int_remaining_size = remaining * bit_width / 32 + 1;
        for (int j = 0; j < int_remaining_size; j++) {

            int_remaining.add(bytesLong2Integer(encoded, decode_pos));
            decode_pos += 4;
        }

        int cur_remaining_bits = 32; // remaining bit width of current value
        long cur_number = int_remaining.get(0);
        int cur_number_i = 1;
        for (int i = n_k_b * 8; i < length; i++) {
            if (bit_width < cur_remaining_bits) {
                int tmp = (int) (cur_number >> (32 - bit_width));
                result_list.add(tmp);
                cur_number <<= bit_width;
                cur_number &= 0xFFFFFFFFL;
                cur_remaining_bits -= bit_width;
            } else {
                int tmp = (int) (cur_number >> (32 - cur_remaining_bits));
                int remain_bits = bit_width - cur_remaining_bits;
                tmp <<= remain_bits;

                cur_number = int_remaining.get(cur_number_i);
                cur_number_i++;
                tmp += (cur_number >> (32 - remain_bits));
                result_list.add(tmp);
                cur_number <<= remain_bits;
                cur_number &= 0xFFFFFFFFL;
                cur_remaining_bits = 32 - remain_bits;
            }
        }
        encoded_pos_result.add(decode_pos);
        return result_list;
    }

    public static int[] getAbsDeltaTsBlock(
            int[] ts_block,
            int i,
            int block_size,
            int remaining,
            int[] min_delta) {
        int[] ts_block_delta = new int[remaining - 1];

        int value_delta_min = Integer.MAX_VALUE;
        int value_delta_max = Integer.MIN_VALUE;
        int base = i * block_size + 1;
        int end = i * block_size + remaining;

        int tmp_j_1 = ts_block[base - 1];
        min_delta[0] = tmp_j_1;
        int j = base;
        int tmp_j;

        while (j < end) {
            tmp_j = ts_block[j];
            int epsilon_v = tmp_j - tmp_j_1;
            ts_block_delta[j - base] = epsilon_v;
            if (epsilon_v < value_delta_min) {
                value_delta_min = epsilon_v;
            }
            if (epsilon_v > value_delta_max) {
                value_delta_max = epsilon_v;
            }
            tmp_j_1 = tmp_j;
            j++;
        }
        j = 0;
        end = remaining - 1;
        while (j < end) {
            ts_block_delta[j] = ts_block_delta[j] - value_delta_min;
            j++;
        }

        min_delta[1] = value_delta_min;
        min_delta[2] = (value_delta_max - value_delta_min);

        return ts_block_delta;
    }

    public static class SubColumnResult {
        public int lBest;
        public int betaBest;

        public SubColumnResult(int lBest, int betaBest) {
            this.lBest = lBest;
            this.betaBest = betaBest;
        }
    }

    public static int RLECost(ArrayList<Integer> numbers) {
        ArrayList<Integer> rleList = new ArrayList<>();

        if (numbers.isEmpty()) {
            return 0;
        }

        int count = 1;
        int currentNumber = numbers.get(0);

        for (int i = 1; i < numbers.size(); i++) {
            if (numbers.get(i) == currentNumber) {
                count++;
            } else {
                rleList.add(count);
                rleList.add(currentNumber);
                currentNumber = numbers.get(i);
                count = 1;
            }
        }

        rleList.add(count);
        rleList.add(currentNumber);

        int maxValue = Collections.max(rleList);
        int maxBits = getBitWidth(maxValue);

        // System.out.println(rleList);
        // System.out.println("rle maxBits: " + maxBits);
        return maxBits * rleList.size();

    }

    public static SubColumnResult subColumn(ArrayList<Integer> list) {
        if (list.isEmpty()) {
            return new SubColumnResult(0, 0);
        }

        int maxValue = Collections.max(list);
        int minValue = Collections.min(list);

        // int m = (int) Math.ceil(Math.log(maxValue - minValue + 1) / Math.log(2));
        int m = getBitWidth(maxValue);
        // System.out.println("m: " + m);

        // int cMin = list.size() * m;
        int cMin = list.size() * m;

        int lBest = 0;
        int betaBest = 0;

        for (int l = 1; l <= m; l++) {
            // System.out.println("l: " + l);
            ArrayList<Integer> highBitsList = new ArrayList<>();
            for (int num : list) {
                int highBits = (num >> l) & ((1 << (m - l)) - 1);
                highBitsList.add(highBits);
            }

            // System.out.println("高 " + (m - l) + " 位: " + highBitsList);

            int highCost = 0;
            highCost += RLECost(highBitsList);
            // System.out.println("highCost: " + highCost);

            ArrayList<Integer> lowBitsList = new ArrayList<>();
            for (int num : list) {
                int lowBits = num & ((1 << l) - 1);
                lowBitsList.add(lowBits);
            }

            // System.out.println("低 " + l + " 位: " + lowBitsList);

            for (int beta = 1; beta <= 4; beta++) {
                // System.out.println("beta: " + beta);

                int lowCost = 0;

                // int parts = (int) Math.ceil((double) l / beta);
                int parts = (l + beta - 1) / beta;

                for (int p = 0; p < parts; p++) {
                    ArrayList<Integer> bpList = new ArrayList<>();
                    for (int num : lowBitsList) {
                        int bitsPart = (num >> (p * beta)) & ((1 << beta) - 1);
                        bpList.add(bitsPart);
                    }

                    // System.out.println(bpList);

                    int maxValuePart = Collections.max(bpList);
                    int maxBitsPart = getBitWidth(maxValuePart);
                    // System.out.println("maxBitsPart: " + maxBitsPart);

                    lowCost += bpList.size() * maxBitsPart;
                }

                // System.out.println("lowCost: " + lowCost);

                if (highCost + lowCost < cMin) {
                    cMin = highCost + lowCost;
                    lBest = l;
                    betaBest = beta;
                }
            }
        }

        return new SubColumnResult(lBest, betaBest);

    }

    public static void SubColumnEncodeBits(int[] ts_block_delta, int l, int beta) {

    }

    public static int SubColumnBlockEncoder(int[] ts_block, int block_i, int block_size, int remaining, int encode_pos,
            byte[] cur_byte) {

        int[] min_delta = new int[3];
        int[] ts_block_delta = getAbsDeltaTsBlock(ts_block, block_i, block_size, remaining, min_delta);
        block_size = min_delta[1];

        ArrayList<Integer> ts_block_delta_list = new ArrayList<>();
        for (int i = 0; i < ts_block_delta.length; i++) {
            ts_block_delta_list.add(ts_block_delta[i]);
        }

        SubColumnResult result = subColumn(ts_block_delta_list);
        int l = result.lBest;
        int beta = result.betaBest;

        SubColumnEncodeBits(ts_block_delta, l, beta);

        return encode_pos;

    }

    public static int SubColumnEncoder(
            int[] data, int block_size, byte[] encoded_result) {

        int length_all = data.length;

        int encode_pos = 0;
        int2Bytes(length_all, encode_pos, encoded_result);
        encode_pos += 4;

        int block_num = length_all / block_size;
        int2Bytes(block_size, encode_pos, encoded_result);
        encode_pos += 4;

        for (int i = 0; i < block_num; i++) {
            encode_pos = SubColumnBlockEncoder(data, i, block_size, block_size, encode_pos, encoded_result);
            // System.out.println(encode_pos);
        }

        int remaining_length = length_all - block_num * block_size;
        if (remaining_length <= 3) {
            for (int i = remaining_length; i > 0; i--) {
                int2Bytes(data[data.length - i], encode_pos, encoded_result);
                encode_pos += 4;
            }

        } else {

            int start = block_num * block_size;
            int remaining = length_all - start;

            encode_pos = SubColumnBlockEncoder(data, block_num, block_size, remaining, encode_pos, encoded_result);

        }

        return encode_pos;

    }

    @Test
    public void test0() throws IOException {
        ArrayList<Integer> list = new ArrayList<>();

        // list.add(26);
        // list.add(48);
        // list.add(51);
        // list.add(33);
        // list.add(24);
        // list.add(56);
        // list.add(65);
        // list.add(75);

        list.add(154);
        list.add(176);
        list.add(179);
        list.add(161);
        list.add(152);
        list.add(184);
        list.add(193);
        list.add(203);

        // System.out.println(list);

        SubColumnResult result = subColumn(list);
        System.out.println(result.lBest);
        System.out.println(result.betaBest);
    }

    public static void main(@org.jetbrains.annotations.NotNull String[] args) throws IOException {
        String parent_dir = "/Users/xiaojinzhao/Documents/GitHub/encoding-outlier/";// your data path
        // String parent_dir =
        // "/Users/zihanguo/Downloads/R/outlier/outliier_code/encoding-outlier/";
        // String output_parent_dir = parent_dir + "icde0802/compression_ratio/rle_bos";
        String input_parent_dir = parent_dir + "trans_data/";
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

        for (String value : dataset_name) {
            input_path_list.add(input_parent_dir + value);
            dataset_block_size.add(1024);
        }

        // output_path_list.add(output_parent_dir + "/CS-Sensors_ratio.csv"); // 0
        // // dataset_block_size.add(1024);
        // output_path_list.add(output_parent_dir + "/Metro-Traffic_ratio.csv");// 1
        // // dataset_block_size.add(2048);
        // output_path_list.add(output_parent_dir + "/USGS-Earthquakes_ratio.csv");// 2
        // // dataset_block_size.add(2048);
        // output_path_list.add(output_parent_dir + "/YZ-Electricity_ratio.csv"); // 3
        // // dataset_block_size.add(256);
        // output_path_list.add(output_parent_dir + "/GW-Magnetic_ratio.csv"); // 4
        // // dataset_block_size.add(1024);
        // output_path_list.add(output_parent_dir + "/TY-Fuel_ratio.csv");// 5
        // // dataset_block_size.add(2048);
        // output_path_list.add(output_parent_dir + "/Cyber-Vehicle_ratio.csv"); // 6
        // // dataset_block_size.add(2048);
        // output_path_list.add(output_parent_dir + "/Vehicle-Charge_ratio.csv");// 7
        // // dataset_block_size.add(2048);
        // output_path_list.add(output_parent_dir + "/Nifty-Stocks_ratio.csv");// 8
        // // dataset_block_size.add(1024);
        // output_path_list.add(output_parent_dir + "/TH-Climate_ratio.csv");// 9
        // // dataset_block_size.add(2048);
        // output_path_list.add(output_parent_dir + "/TY-Transport_ratio.csv");// 10
        // // dataset_block_size.add(2048);
        // output_path_list.add(output_parent_dir + "/EPM-Education_ratio.csv");// 11
        // dataset_block_size.add(1024);

        int repeatTime2 = 100;
        // for (int file_i = 8; file_i < 9; file_i++) {

        for (int file_i = 0; file_i < input_path_list.size(); file_i++) {

            String inputPath = input_path_list.get(file_i);
            System.out.println(inputPath);
            String Output = output_path_list.get(file_i);

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
                System.out.println(f);
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
                int[] data2_arr = new int[data1.size()];
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
                for (int repeat = 0; repeat < repeatTime2; repeat++) {
                    length = SubColumnEncoder(data2_arr, dataset_block_size.get(file_i),
                    encoded_result);
                }

                long e = System.nanoTime();
                encodeTime += ((e - s) / repeatTime2);
                compressed_size += length;
                double ratioTmp = compressed_size / (double) (data1.size() * Integer.BYTES);
                ratio += ratioTmp;
                s = System.nanoTime();
                for (int repeat = 0; repeat < repeatTime2; repeat++)
                    // BOSDecoder(encoded_result);
                    e = System.nanoTime();
                decodeTime += ((e - s) / repeatTime2);

                String[] record = {
                        f.toString(),
                        "RLE+BOS-V",
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
