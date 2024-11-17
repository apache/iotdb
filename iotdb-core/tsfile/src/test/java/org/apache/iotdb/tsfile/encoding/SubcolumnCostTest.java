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

    // public static class SubColumnResult {
    //     public int lBest;
    //     public int betaBest;
    //     public int cost;

    //     public SubColumnResult(int lBest, int betaBest, int cost) {
    //         this.lBest = lBest;
    //         this.betaBest = betaBest;
    //         this.cost = cost;
    //     }
    // }

    public static int RLECost(int[] numbers) {
        int[] values = new int[numbers.length];
        int[] run_length = new int[numbers.length];

        int count = 1;
        int currentNumber = numbers[0];
        int index = 0;

        for (int i = 1; i < numbers.length; i++) {
            if (numbers[i] == currentNumber) {
                count++;
            } else {
                values[index] = currentNumber;
                run_length[index] = count;
                index++;
                currentNumber = numbers[i];
                count = 1;
            }
        }

        values[index] = currentNumber;
        run_length[index] = count;
        index++;

        // for (int i = 0; i < index; i++) {
        // System.out.print(values[i] + " ");
        // }
        // System.out.println();

        // for (int i = 0; i < index; i++) {
        // System.out.print(run_length[i] + " ");
        // }
        // System.out.println();

        int maxValue = Integer.MIN_VALUE;
        for (int i = 0; i < index; i++) {
            if (values[i] > maxValue) {
                maxValue = values[i];
            }
        }

        int maxBits = getBitWidth(maxValue);

        return maxBits * index + 8 * index;

    }

    public static int[] subColumn(int[] list, int remaining) {
        if (remaining == 0) {
            return new int[] { 0, 0, 0, 0 };
        }

        int maxValue = Integer.MIN_VALUE;
        int minValue = Integer.MAX_VALUE;

        for (int i = 0; i < remaining; i++) {
            if (list[i] > maxValue) {
                maxValue = list[i];
            }
            if (list[i] < minValue) {
                minValue = list[i];
            }
        }

        // int m = (int) Math.ceil(Math.log(maxValue - minValue + 1) / Math.log(2));
        int m = getBitWidth(maxValue);
        // System.out.println("m: " + m);

        // int cMin = list.size() * m;
        // int cMin = remaining * m;
        int cMin = Integer.MAX_VALUE;

        int lBest = 0;
        int betaBest = 0;

        for (int l = 1; l <= m; l++) {
            // System.out.println("l: " + l);

            int highCost = 0;

            // if (l != m) {
            int[] highBitsList = new int[remaining];
            for (int i = 0; i < remaining; i++) {
                highBitsList[i] = (list[i] >> l) & ((1 << (m - l)) - 1);
            }

            // for (int i = 0; i < highBitsList.length; i++) {
            // System.out.print(highBitsList[i] + " ");
            // }
            // System.out.println();

            highCost += RLECost(highBitsList);
            // System.out.println("highCost: " + highCost);
            // }

            int[] lowBitsList = new int[remaining];
            for (int i = 0; i < remaining; i++) {
                lowBitsList[i] = list[i] & ((1 << l) - 1);
            }

            // for (int i = 0; i < lowBitsList.length; i++) {
            // System.out.print(lowBitsList[i] + " ");
            // }
            // System.out.println();

            for (int beta = 1; beta <= 4; beta++) {
                // System.out.println("beta: " + beta);

                int lowCost = 0;

                // int parts = (int) Math.ceil((double) l / beta);
                int parts = (l + beta - 1) / beta;

                for (int p = 0; p < parts; p++) {
                    int[] bpList = new int[remaining];
                    int maxValuePart = 0;
                    for (int i = 0; i < remaining; i++) {
                        bpList[i] = (lowBitsList[i] >> (p * beta)) & ((1 << beta) - 1);
                        if (bpList[i] > maxValuePart) {
                            maxValuePart = bpList[i];
                        }
                    }

                    // for (int i = 0; i < bpList.length; i++) {
                    // System.out.print(bpList[i] + " ");
                    // }
                    // System.out.println();

                    // int maxValuePart = Collections.max(bpList);
                    int maxBitsPart = getBitWidth(maxValuePart);
                    // System.out.println("maxBitsPart: " + maxBitsPart);

                    lowCost += remaining * maxBitsPart;
                }

                // System.out.println("lowCost: " + lowCost);

                if (highCost + lowCost < cMin) {
                    cMin = highCost + lowCost;
                    lBest = l;
                    betaBest = beta;
                }
            }
        }

        return new int[] { m, lBest, betaBest, cMin };

    }

    public static int SubColumnBlockEncoder(int[] ts_block, int block_i, int block_size, int remaining, int encode_pos,
            byte[] encoded_result) {

        // for (int i = block_i * block_size; i < block_i * block_size + remaining; i++)
        // {
        // System.out.print(ts_block[i] + " ");
        // }
        // System.out.println();
        // System.out.println("ts block length: " + remaining);

        int[] min_delta = new int[3];
        int[] ts_block_delta = getAbsDeltaTsBlock(ts_block, block_i, block_size, remaining, min_delta);

        for (int i = 0; i < 3; i++) {
            int2Bytes(min_delta[i], encode_pos, encoded_result);
            encode_pos += 4;
        }

        // for (int i = 0; i < 3; i++) {
        // System.out.print(min_delta[i] + " ");
        // }
        // System.out.println();
        // System.out.println("ts block delta length: " + (remaining - 1));

        // for (int i = 0; i < remaining - 1; i++) {
        // System.out.print(ts_block_delta[i] + " ");
        // }
        // System.out.println();

        int[] result = subColumn(ts_block_delta, remaining - 1);
        int m = result[0];
        int l = result[1];
        int beta = result[2];
        int cost = result[3];

        intByte2Bytes(m, encode_pos, encoded_result);
        encode_pos += 1;

        intByte2Bytes(l, encode_pos, encoded_result);
        encode_pos += 1;

        intByte2Bytes(beta, encode_pos, encoded_result);
        encode_pos += 1;

        int[] highBitsList = new int[remaining - 1];
        for (int i = 0; i < remaining - 1; i++) {
            highBitsList[i] = (ts_block_delta[i] >> l) & ((1 << (m - l)) - 1);
        }

        // encode_pos += cost / 8;

        return encode_pos;

    }

    public int SubcolumnEncoder(int[] data, int block_size, byte[] encoded_result) {

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
        int[] list = new int[8];

        list[0] = 154;
        list[1] = 176;
        list[2] = 179;
        list[3] = 161;
        list[4] = 152;
        list[5] = 184;
        list[6] = 193;
        list[7] = 203;

        int[] result = subColumn(list, 8);
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
        // 找到最后一个斜杠的位置，从而提取文件名
        int lastSlashIndex = path.lastIndexOf('/');

        // 提取文件名（从最后一个斜杠之后开始）
        String fileNameWithExtension = path.substring(lastSlashIndex + 1);

        // 去掉文件扩展名（.csv）
        int dotIndex = fileNameWithExtension.lastIndexOf('.');
        if (dotIndex != -1) {
            return fileNameWithExtension.substring(0, dotIndex);
        }

        // 如果没有扩展名，直接返回文件名
        return fileNameWithExtension;
    }

    @Test
    public void BOSOptimalTest() throws IOException {
        // String parent_dir = "/Users/xiaojinzhao/Documents/GitHub/"; // your data path
        String parent_dir = "D:/github/temp/work/compress-subcolumn/";
        // String parent_dir =
        // "/Users/zihanguo/Downloads/R/outlier/outliier_code/encoding-outlier/";
        // String output_parent_dir = parent_dir + "subcolumn/compression_ratio";
        String output_parent_dir = parent_dir;
        // String input_parent_dir = parent_dir +
        // "elf/src/test/resources/ElfData_Short";
        // String input_parent_dir = parent_dir + "ElfData_Short/";
        String input_parent_dir = parent_dir + "testdata/";
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
        // output_path_list.add(output_parent_dir + "/compress_ratio.csv"); // 0
        output_path_list.add(output_parent_dir + "/test0.csv"); // 0
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
        // TODO 做完之后将下面这个去掉
        repeatTime2 = 1;
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
                    // "Div",
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

                loader.readHeaders();
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
                    length = SubcolumnEncoder(data2_arr, dataset_block_size.get(file_i),
                            encoded_result);
                }

                long e = System.nanoTime();
                encodeTime += ((e - s) / repeatTime2);
                compressed_size += length;
                double ratioTmp = compressed_size / (double) (data1.size() * Long.BYTES);
                ratio += ratioTmp;
                s = System.nanoTime();
                // for (int repeat = 0; repeat < repeatTime2; repeat++)
                // SubcolumnDecoder(encoded_result);
                e = System.nanoTime();
                decodeTime += ((e - s) / repeatTime2);

                String[] record = {
                        datasetName,
                        "TS_2DIFF+Sucolumn",
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
                    // length = SubColumnEncoder(data2_arr, dataset_block_size.get(file_i),
                    // encoded_result);
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
